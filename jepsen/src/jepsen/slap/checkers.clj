(ns jepsen.slap.checkers
  "Checker helpers shared by the workloads."
  (:require [jepsen.checker :as checker]
            [jepsen.history :as h]))

(defn without
  "`checker`, on the history without the operations whose :f is `f`: a
  workload's final report (an audit, a check, stats), which is not an
  operation that checker understands."
  [f checker]
  (reify checker/Checker
    (check [_ test history opts]
      (checker/check checker test (h/remove #(= f (:f %)) history) opts))))
