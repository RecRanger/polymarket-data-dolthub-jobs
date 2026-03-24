
CREATE TABLE bronze_gamma_event_series_link (
    series_id VARCHAR(255) NOT NULL, 
    event_id VARCHAR(255) NOT NULL, 
    series_slug VARCHAR(255) NOT NULL, 
    event_slug VARCHAR(255) NOT NULL, 
    db_created_at DATETIME NOT NULL DEFAULT now(), 
    db_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
    PRIMARY KEY (series_id, event_id)
)

