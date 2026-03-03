
CREATE TABLE entry_outcome_dependencies (
    dependency_uuid VARCHAR(255) NOT NULL, 
    outcome_id_a VARCHAR(255) NOT NULL, 
    outcome_id_b VARCHAR(255) NOT NULL, 
    relationship_type ENUM('complement','equivalent','implies','mutex','composite','disjunction','upper_bound','lower_bound','conditional') NOT NULL, 
    comments VARCHAR(500), 
    db_created_at DATETIME NOT NULL DEFAULT now(), 
    db_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
    PRIMARY KEY (dependency_uuid)
)

