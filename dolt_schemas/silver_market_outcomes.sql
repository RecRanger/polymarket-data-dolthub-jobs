
CREATE TABLE silver_market_outcomes (
    outcome_slug VARCHAR NOT NULL, 
    market_id VARCHAR NOT NULL, 
    market_slug VARCHAR NOT NULL, 
    question VARCHAR NOT NULL, 
    outcome_name VARCHAR NOT NULL, 
    outcome_price FLOAT, 
    clob_token_id VARCHAR NOT NULL, 
    db_created_at DATETIME DEFAULT now() NOT NULL, 
    db_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (outcome_slug)
)

