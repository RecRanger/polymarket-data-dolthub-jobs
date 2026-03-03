ALTER TABLE entry_outcome_dependencies
ADD CONSTRAINT unique_outcome_pair
UNIQUE (outcome_id_a, outcome_id_b);
