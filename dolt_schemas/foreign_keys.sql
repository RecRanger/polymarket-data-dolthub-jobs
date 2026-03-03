ALTER TABLE silver_market_outcomes ADD CONSTRAINT silver_market_outcomes_bronze_gamma_markets_FK
FOREIGN KEY (market_id) REFERENCES bronze_gamma_markets(market_id);


ALTER TABLE bronze_gamma_markets ADD CONSTRAINT bronze_gamma_markets_bronze_gamma_events_FK
FOREIGN KEY (event_id) REFERENCES bronze_gamma_events(event_id);


ALTER TABLE entry_outcome_dependencies ADD CONSTRAINT entry_outcome_dependencies_silver_outcomes_a_FK
FOREIGN KEY (outcome_id_a) REFERENCES silver_market_outcomes(outcome_id);

ALTER TABLE entry_outcome_dependencies ADD CONSTRAINT entry_outcome_dependencies_silver_outcomes_b_FK
FOREIGN KEY (outcome_id_b) REFERENCES silver_market_outcomes(outcome_id);
