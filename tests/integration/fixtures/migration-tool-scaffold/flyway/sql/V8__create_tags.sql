-- V8: Create tags and post_tags tables
CREATE TABLE tags (
    id   SERIAL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

CREATE TABLE post_tags (
    post_id INTEGER NOT NULL,
    tag_id  INTEGER NOT NULL,
    PRIMARY KEY (post_id, tag_id)
);
