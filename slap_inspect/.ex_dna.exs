%{
  paths: ["lib/", "test/"],
  min_mass: 25,
  min_occurrences: 3,
  excluded_macros: [:schema, :pipe_through, :plug],
  normalize_pipes: true,
  literal_mode: :keep,
  min_similarity: 1.0
}
