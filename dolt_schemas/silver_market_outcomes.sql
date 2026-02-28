
CREATE TABLE silver_market_outcomes (
    outcome_id VARCHAR(255) NOT NULL, 
    outcome_slug VARCHAR(355) NOT NULL, 
    outcome_index SMALLINT NOT NULL, 
    market_id VARCHAR(255) NOT NULL, 
    market_slug VARCHAR(255) NOT NULL, 
    question VARCHAR(255) NOT NULL, 
    outcome_name VARCHAR(100) NOT NULL, 
    outcome_price FLOAT, 
    clob_token_id VARCHAR(255) NOT NULL, 
    db_created_at DATETIME DEFAULT now() NOT NULL, 
    db_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, 
    PRIMARY KEY (outcome_id)
)

