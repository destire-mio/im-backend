BEGIN;

ALTER TABLE users
    ADD CONSTRAINT users_name_valid CHECK (
        name = btrim(name)
        AND char_length(name) BETWEEN 1 AND 100
    );

COMMIT;
