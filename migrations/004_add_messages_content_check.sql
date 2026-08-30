BEGIN;

ALTER TABLE messages
    ADD CONSTRAINT messages_content_length_valid CHECK (
        char_length(content) <= 4000
    );

COMMIT;
