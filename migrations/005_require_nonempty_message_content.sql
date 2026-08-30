BEGIN;

ALTER TABLE messages
    DROP CONSTRAINT messages_content_length_valid;

ALTER TABLE messages
    ADD CONSTRAINT messages_content_length_valid CHECK (
        char_length(content) BETWEEN 1 AND 4000
    );

COMMIT;
