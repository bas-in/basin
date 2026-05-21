-- V3: Create team_members join table
CREATE TABLE team_members (
    team_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    joined_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, user_id)
);
