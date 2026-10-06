//! Built-in merge operators, chosen by name when a database opens.
//!
//! SlateDB calls a merge operator synchronously from its own threads, on
//! reads, memtable flushes and compactions, where no Elixir process is
//! waiting. So the operators are written in Rust instead of calling back into
//! Elixir. Each one is associative, as SlateDB requires: it may merge any run
//! of operands, with or without the base value, in any grouping.
//!
//! Numeric operators work on 8-byte little-endian integers. The binding
//! checks operand sizes when they are written. A base value written with
//! `put` must also be 8 bytes; if it is not, reads of that key fail and so
//! does any compaction that includes it.

use std::sync::Arc;

use rustler::NifUnitEnum;
use slatedb::bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};

use crate::reply::{NifError, NifOutcome};

#[derive(Clone, Copy, Debug, PartialEq, Eq, NifUnitEnum)]
pub(crate) enum Builtin {
    /// Wrapping sum of unsigned 64-bit integers.
    U64Add,
    /// Wrapping sum of signed 64-bit integers.
    I64Add,
    /// Largest unsigned 64-bit integer.
    U64Max,
    /// Smallest unsigned 64-bit integer.
    U64Min,
    /// Concatenation, oldest first.
    Append,
}

impl Builtin {
    /// Checks an operand before it is written, so that a bad operand fails
    /// the write instead of a later read or compaction.
    pub(crate) fn validate_operand(self, operand: &[u8]) -> NifOutcome<()> {
        match self {
            Self::Append => Ok(()),
            _ if operand.len() == 8 => Ok(()),
            _ => Err(NifError::invalid(format!(
                "operand for {self:?} must be 8 bytes (a little-endian 64-bit integer), got {}",
                operand.len()
            ))),
        }
    }

    pub(crate) fn operator(self) -> Arc<dyn MergeOperator + Send + Sync> {
        Arc::new(BuiltinOperator(self))
    }
}

struct BuiltinOperator(Builtin);

fn word(key: &Bytes, bytes: &[u8]) -> Result<[u8; 8], MergeOperatorError> {
    bytes.try_into().map_err(|_| MergeOperatorError::Callback {
        message: format!(
            "key {key:?}: value is {} bytes, but this merge operator needs 8",
            bytes.len()
        ),
    })
}

impl BuiltinOperator {
    fn fold_u64(
        key: &Bytes,
        existing: Option<Bytes>,
        operands: &[Bytes],
        init: u64,
        f: fn(u64, u64) -> u64,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut acc = match existing {
            Some(value) => u64::from_le_bytes(word(key, &value)?),
            None => init,
        };
        for operand in operands {
            acc = f(acc, u64::from_le_bytes(word(key, operand)?));
        }
        Ok(Bytes::copy_from_slice(&acc.to_le_bytes()))
    }
}

impl MergeOperator for BuiltinOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        if existing_value.is_none() && operands.is_empty() {
            return Err(MergeOperatorError::EmptyBatch);
        }
        match self.0 {
            // With no base value, an operand stands for itself. Starting the
            // fold from the operator's identity gives exactly that.
            Builtin::U64Add => Self::fold_u64(key, existing_value, operands, 0, u64::wrapping_add),
            Builtin::U64Max => Self::fold_u64(key, existing_value, operands, 0, u64::max),
            Builtin::U64Min => Self::fold_u64(key, existing_value, operands, u64::MAX, u64::min),
            Builtin::I64Add => {
                let mut acc = match existing_value {
                    Some(value) => i64::from_le_bytes(word(key, &value)?),
                    None => 0,
                };
                for operand in operands {
                    acc = acc.wrapping_add(i64::from_le_bytes(word(key, operand)?));
                }
                Ok(Bytes::copy_from_slice(&acc.to_le_bytes()))
            }
            Builtin::Append => {
                let base = existing_value.as_deref().unwrap_or(&[]);
                let len = base.len() + operands.iter().map(Bytes::len).sum::<usize>();
                let mut out = Vec::with_capacity(len);
                out.extend_from_slice(base);
                for operand in operands {
                    out.extend_from_slice(operand);
                }
                Ok(Bytes::from(out))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(n: u64) -> Bytes {
        Bytes::copy_from_slice(&n.to_le_bytes())
    }

    fn n(bytes: &Bytes) -> u64 {
        u64::from_le_bytes(bytes.as_ref().try_into().unwrap())
    }

    #[test]
    fn numeric_operators_are_associative() {
        let key = Bytes::from_static(b"k");
        for op in [Builtin::U64Add, Builtin::U64Max, Builtin::U64Min] {
            let op = BuiltinOperator(op);
            let (a, x, y) = (b(5), b(9), b(3));
            // (a + x) + y == a + (x + y), where x + y is merged without a base.
            let left = op.merge(
                &key,
                Some(op.merge(&key, Some(a.clone()), x.clone()).unwrap()),
                y.clone(),
            );
            let partial = op
                .merge(&key, None, x.clone())
                .and_then(|p| op.merge(&key, Some(p), y.clone()));
            let right = op.merge(&key, Some(a), partial.unwrap());
            assert_eq!(n(&left.unwrap()), n(&right.unwrap()));
        }
    }

    #[test]
    fn values_of_the_wrong_size_are_errors() {
        let op = BuiltinOperator(Builtin::U64Add);
        let key = Bytes::from_static(b"k");
        assert!(op
            .merge(&key, Some(Bytes::from_static(b"abc")), b(1))
            .is_err());
        assert!(op.merge(&key, None, Bytes::from_static(b"abc")).is_err());
        // `validate_operand` builds atoms, which needs a running BEAM, so it
        // is tested from Elixir.
    }

    #[test]
    fn append_concatenates_oldest_first() {
        let op = BuiltinOperator(Builtin::Append);
        let key = Bytes::from_static(b"k");
        let out = op
            .merge_batch(
                &key,
                Some(Bytes::from_static(b"a")),
                &[Bytes::from_static(b"b"), Bytes::from_static(b"c")],
            )
            .unwrap();
        assert_eq!(out.as_ref(), b"abc");
    }
}
