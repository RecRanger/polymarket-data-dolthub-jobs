
CREATE TABLE bronze_gamma_event_tag_link (
    tag_id VARCHAR(255) NOT NULL, 
    event_id VARCHAR(255) NOT NULL, 
    tag_slug VARCHAR(255) NOT NULL, 
    event_slug VARCHAR(255) NOT NULL, 
    db_created_at DATETIME NOT NULL DEFAULT now(), 
    db_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
    PRIMARY KEY (tag_id, event_id)
)

