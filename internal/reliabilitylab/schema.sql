-- Isolated lab schema only. Never part of the IM application's migrations.
CREATE TABLE packets (
    id TEXT PRIMARY KEY,
    total BIGINT NOT NULL CHECK (total > 0),
    shares INTEGER NOT NULL CHECK (shares BETWEEN 1 AND 10000)
);
CREATE TABLE packet_slots (
    packet_id TEXT NOT NULL REFERENCES packets(id),
    slot INTEGER NOT NULL CHECK (slot >= 0),
    amount BIGINT NOT NULL CHECK (amount > 0),
    claimed_by TEXT CHECK (length(claimed_by) BETWEEN 1 AND 128),
    PRIMARY KEY (packet_id, slot),
    UNIQUE (packet_id, claimed_by),
    UNIQUE (packet_id, slot, amount, claimed_by)
);
CREATE FUNCTION check_allocation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target TEXT; expected_total BIGINT; expected_shares INTEGER;
BEGIN
    IF TG_TABLE_NAME = 'packets' THEN target := NEW.id;
    ELSE target := COALESCE(NEW.packet_id, OLD.packet_id); END IF;
    SELECT total, shares INTO expected_total, expected_shares FROM packets WHERE id = target;
    IF (SELECT count(*) <> expected_shares OR COALESCE(sum(amount), 0) <> expected_total
        OR min(slot) <> 0 OR max(slot) <> expected_shares - 1
        FROM packet_slots WHERE packet_id = target) THEN
        RAISE EXCEPTION 'invalid packet allocation' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER packet_allocation AFTER INSERT OR UPDATE ON packets
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_allocation();
CREATE CONSTRAINT TRIGGER slot_allocation AFTER INSERT OR UPDATE OR DELETE ON packet_slots
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_allocation();
CREATE FUNCTION immutable_allocation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'packets' THEN
        IF ROW(NEW.id,NEW.total,NEW.shares) IS DISTINCT FROM ROW(OLD.id,OLD.total,OLD.shares) THEN
            RAISE EXCEPTION 'packet allocation is immutable' USING ERRCODE = '23514';
        END IF;
    ELSIF ROW(NEW.packet_id,NEW.slot,NEW.amount) IS DISTINCT FROM ROW(OLD.packet_id,OLD.slot,OLD.amount) THEN
        RAISE EXCEPTION 'slot allocation is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER immutable_packet BEFORE UPDATE ON packets
    FOR EACH ROW EXECUTE FUNCTION immutable_allocation();
CREATE TRIGGER immutable_slot BEFORE UPDATE ON packet_slots
    FOR EACH ROW EXECUTE FUNCTION immutable_allocation();
CREATE TABLE packet_credits (
    packet_id TEXT NOT NULL,
    slot INTEGER NOT NULL,
    amount BIGINT NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (packet_id, user_id),
    UNIQUE (packet_id, slot),
    FOREIGN KEY (packet_id, slot, amount, user_id)
        REFERENCES packet_slots(packet_id, slot, amount, claimed_by)
);
CREATE TABLE credit_outbox (
    packet_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (packet_id, user_id),
    FOREIGN KEY (packet_id, user_id) REFERENCES packet_credits(packet_id, user_id)
);
-- A stand-in downstream durable consumer, committed separately from ACK.
CREATE TABLE received_credits (
    packet_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    PRIMARY KEY (packet_id, user_id)
);
CREATE TABLE cache_values (key TEXT PRIMARY KEY, value TEXT NOT NULL, version BIGINT NOT NULL);
CREATE TABLE cache_invalidations (key TEXT PRIMARY KEY, version BIGINT NOT NULL);
CREATE TABLE resource_leases (name TEXT PRIMARY KEY, token BIGINT NOT NULL, until TIMESTAMPTZ NOT NULL);
CREATE TABLE resources (name TEXT PRIMARY KEY, value TEXT NOT NULL, last_token BIGINT NOT NULL DEFAULT 0);
