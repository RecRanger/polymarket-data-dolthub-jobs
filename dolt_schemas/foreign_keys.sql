ALTER TABLE silver_market_outcomes ADD CONSTRAINT silver_market_outcomes_bronze_gamma_markets_FK
FOREIGN KEY (market_id) REFERENCES bronze_gamma_markets(market_id);

ALTER TABLE bronze_gamma_markets ADD CONSTRAINT bronze_gamma_markets_bronze_gamma_events_FK
FOREIGN KEY (event_id) REFERENCES bronze_gamma_events(event_id);
