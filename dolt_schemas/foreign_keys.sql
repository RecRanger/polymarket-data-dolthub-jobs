-- outcomes to markets
ALTER TABLE silver_market_outcomes ADD CONSTRAINT silver_market_outcomes_bronze_gamma_markets_FK
FOREIGN KEY (market_id) REFERENCES bronze_gamma_markets(market_id);


-- markets to events
ALTER TABLE bronze_gamma_markets ADD CONSTRAINT bronze_gamma_markets_bronze_gamma_events_FK
FOREIGN KEY (event_id) REFERENCES bronze_gamma_events(event_id);


-- bronze_gamma_event_series_link
ALTER TABLE bronze_gamma_event_series_link ADD CONSTRAINT bronze_gamma_event_series_link_bronze_gamma_events_FK
FOREIGN KEY (event_id) REFERENCES bronze_gamma_events(event_id);

ALTER TABLE bronze_gamma_event_series_link ADD CONSTRAINT bronze_gamma_event_series_link_bronze_gamma_series_FK
FOREIGN KEY (series_id) REFERENCES bronze_gamma_series(series_id);


-- bronze_gamma_event_tag_link
ALTER TABLE bronze_gamma_event_tag_link ADD CONSTRAINT bronze_gamma_event_tag_link_bronze_gamma_events_FK
FOREIGN KEY (event_id) REFERENCES bronze_gamma_events(event_id);

ALTER TABLE bronze_gamma_event_tag_link ADD CONSTRAINT bronze_gamma_event_tag_link_bronze_gamma_series_FK
FOREIGN KEY (tag_id) REFERENCES bronze_gamma_series(tag_id);


-- entry_outcome_dependencies
ALTER TABLE entry_outcome_dependencies ADD CONSTRAINT entry_outcome_dependencies_silver_outcomes_a_FK
FOREIGN KEY (outcome_id_a) REFERENCES silver_market_outcomes(outcome_id);

ALTER TABLE entry_outcome_dependencies ADD CONSTRAINT entry_outcome_dependencies_silver_outcomes_b_FK
FOREIGN KEY (outcome_id_b) REFERENCES silver_market_outcomes(outcome_id);
