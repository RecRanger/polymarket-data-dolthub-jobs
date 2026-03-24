
CREATE TABLE bronze_gamma_tags (
    tag_id VARCHAR(255) NOT NULL, 
    tag_slug VARCHAR(255) NOT NULL, 
    label VARCHAR(255), 
    force_show BOOL, 
    force_hide BOOL, 
    is_carousel BOOL, 
    requires_translation BOOL NOT NULL, 
    created_by VARCHAR(255), 
    updated_by VARCHAR(255), 
    created_at VARCHAR(255), 
    updated_at VARCHAR(255), 
    published_at VARCHAR(255), 
    db_created_at DATETIME NOT NULL DEFAULT now(), 
    db_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
    PRIMARY KEY (tag_id)
)

