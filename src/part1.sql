DROP TABLE IF EXISTS
    peers,  tasks, checks, p2p,
    verter, transferred_points,
    friends, recommendations,
    xp, time_tracking, xpamount;

DROP PROCEDURE IF EXISTS pr_add_p2p_check(text, text, text, check_state, time) cascade;
DROP PROCEDURE IF EXISTS pr_add_verter_check(text, text, check_state, time) cascade;
DROP PROCEDURE IF EXISTS import_from_csv(varchar, varchar, varchar) cascade;
DROP PROCEDURE IF EXISTS export_to_csv(varchar, varchar, varchar) cascade;
DROP FUNCTION IF EXISTS fnc_transfer_p2p_point() cascade;
DROP FUNCTION IF EXISTS fnc_trg_xp_max() cascade;

-- PEERS table
CREATE TABLE IF NOT EXISTS peers (
    nickname VARCHAR PRIMARY KEY,
    birthday DATE NOT NULL
);

-- TASKS table
CREATE TABLE IF NOT EXISTS tasks (
    title       VARCHAR PRIMARY KEY,
    parent_task VARCHAR CHECK (parent_task NOT LIKE title),
    max_xp      BIGINT NOT NULL CHECK ( max_xp > 0 ),
    CONSTRAINT fk_tasks_tasks FOREIGN KEY (parent_task) REFERENCES tasks(title)
);

-- CHECKS table
CREATE TABLE IF NOT EXISTS checks (
    id          BIGSERIAL PRIMARY KEY,
    peer        VARCHAR NOT NULL,
    task        VARCHAR NOT NULL,
    check_date  DATE NOT NULL,
    CONSTRAINT fk_checks_peers FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_checks_tasks FOREIGN KEY (task) REFERENCES tasks(title)
);

-- ENUMS
DO $$BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'check_state')
        THEN CREATE TYPE check_state AS ENUM ('start', 'success', 'failure');
    END IF;
END$$;

-- P2P table
CREATE TABLE IF NOT EXISTS p2p (
    id              BIGSERIAL PRIMARY KEY,
    check_id        BIGINT NOT NULL,
    checking_peer   VARCHAR NOT NULL,
    state           check_state NOT NULL,
    check_time      TIME NOT NULL,
    CONSTRAINT fk_p2p_checks FOREIGN KEY (check_id) REFERENCES checks(id),
    CONSTRAINT fk_p2p_peers FOREIGN KEY (checking_peer) REFERENCES peers(nickname)
);

-- VERTER table
CREATE TABLE IF NOT EXISTS verter (
    id          BIGSERIAL PRIMARY KEY,
    check_id    BIGINT NOT NULL,
    state       check_state NOT NULL,
    check_time  TIME NOT NULL,
    CONSTRAINT fk_verter_checks FOREIGN KEY (check_id) REFERENCES checks(id)
);

-- TRANSFERRED_POINTS table
CREATE TABLE IF NOT EXISTS transferred_points (
    id              BIGSERIAL PRIMARY KEY,
    checking_peer   VARCHAR NOT NULL CHECK (checking_peer NOT LIKE checked_peer),
    checked_peer    VARCHAR NOT NULL CHECK (checked_peer NOT LIKE checking_peer),
    points_amount   BIGINT NOT NULL CHECK (points_amount > 0),
    CONSTRAINT fk_transferred_points_peers_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers(nickname),
    CONSTRAINT fk_transferred_points_peers_checked_peer FOREIGN KEY (checked_peer) REFERENCES peers(nickname),
    UNIQUE (checking_peer, checked_peer)
);

-- FRIENDS table
CREATE TABLE IF NOT EXISTS friends (
    id      BIGSERIAL PRIMARY KEY,
    peer1   VARCHAR NOT NULL CHECK (peer1 NOT LIKE peer2),
    peer2   VARCHAR NOT NULL CHECK (peer2 NOT LIKE peer1),
    CONSTRAINT fk_friends_peers_peer1 FOREIGN KEY (peer1) REFERENCES peers(nickname),
    CONSTRAINT fk_friends_peers_peer2 FOREIGN KEY (peer2) REFERENCES peers(nickname)
);

-- RECOMMENDATIONS table
CREATE TABLE IF NOT EXISTS recommendations
(
    id               BIGSERIAL PRIMARY KEY,
    peer             VARCHAR NOT NULL CHECK (peer NOT LIKE  recommended_peer),
    recommended_peer VARCHAR NOT NULL CHECK (recommended_peer NOT LIKE peer),
    CONSTRAINT fk_recommendations_peers_peer             FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_recommendations_peers_recommended_peer FOREIGN KEY (recommended_peer) REFERENCES peers(nickname)
);

-- XP table
CREATE TABLE IF NOT EXISTS xp
(
    id          BIGSERIAL PRIMARY KEY,
    check_id    BIGINT NOT NULL,
    xp_amount   BIGINT NOT NULL CHECK (xp_amount > 0),
    CONSTRAINT fk_xp_check FOREIGN KEY (check_id) REFERENCES checks(id)
);

-- TIME_TRACKING table
CREATE TABLE IF NOT EXISTS time_tracking
(
    id      BIGSERIAL PRIMARY KEY,
    peer    VARCHAR NOT NULL,
    "date"  DATE NOT NULL,
    "time"  TIME NOT NULL,
    state   SMALLINT NOT NULL CHECK(state in (1,2)),
    CONSTRAINT fk_time_tracking_peers FOREIGN KEY (peer) REFERENCES peers(nickname)
);

-- EXPORT TO CSV
CREATE OR REPLACE PROCEDURE export_to_csv(
    table_name varchar,
    global_path varchar,
    delimiter varchar
) AS $$
    BEGIN
        EXECUTE format(
            'COPY %s TO %L WITH CSV DELIMITER %L HEADER',
            table_name,
            global_path,
            delimiter
        );
    END
$$ LANGUAGE plpgsql;

-- TEST
TRUNCATE peers CASCADE;
INSERT INTO peers VALUES ('myregree', '1987.10.19');
INSERT INTO peers VALUES ('darrpama', '1988.11.20');
CALL export_to_csv ('peers', '/Users/myregree/Desktop/projects/SQL2_Info21_v1.0-2/src/peers.csv', ',');

-- IMPORT FROM CSV
CREATE OR REPLACE PROCEDURE import_from_csv(
    table_name varchar,
    global_path varchar,
    delimiter varchar
) AS $$
    BEGIN
        EXECUTE format(
            'copy %s FROM %L WITH DELIMITER %L CSV HEADER',
            table_name,
            global_path,
            delimiter
        );
    END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE import_all_from_csv(
    path TEXT DEFAULT '/Users/myregree/Desktop/projects/SQL2_Info21_v1.0-2/src/csv/'
) AS $$
    BEGIN
        ---- Fill tables with data
        TRUNCATE peers, tasks, checks, p2p, verter, transferred_points, friends, recommendations, xp, time_tracking RESTART IDENTITY CASCADE;
        CALL import_from_csv ('peers', CONCAT(path, '01-init_peers.csv'), ',');
        CALL import_from_csv ('tasks', CONCAT(path, '02-init_tasks.csv'), ',');
        CALL import_from_csv ('checks', CONCAT(path, '03-init_checks.csv'), ',');
        PERFORM setval('checks_id_seq', (SELECT MAX(id) FROM checks)+1);
        CALL import_from_csv ('p2p', CONCAT(path, '04-init_p2p.csv'), ',');
        PERFORM setval('p2p_id_seq', (SELECT MAX(id) FROM p2p)+1);
        CALL import_from_csv ('verter', CONCAT(path, '05-init_verter.csv'), ',');
        PERFORM setval('verter_id_seq', (SELECT MAX(id) FROM verter)+1);
        CALL import_from_csv ('transferred_points', CONCAT(path, '06-init_transferred_points.csv'), ',');
        PERFORM setval('transferred_points_id_seq', (SELECT MAX(id) FROM transferred_points)+1);
        CALL import_from_csv ('friends', CONCAT(path, '07-init_friends.csv'), ',');
        PERFORM setval('friends_id_seq', (SELECT MAX(id) FROM friends)+1);
        CALL import_from_csv ('recommendations', CONCAT(path, '08-init_recommendations.csv'), ',');
        PERFORM setval('recommendations_id_seq', (SELECT MAX(id) FROM recommendations)+1);
        CALL import_from_csv ('xp', CONCAT(path, '09-init_xp.csv'), ',');
        PERFORM setval('xp_id_seq', (SELECT MAX(id) FROM xp)+1);
        CALL import_from_csv ('time_tracking', CONCAT(path, '10-init_time_tracking.csv'), ',');
        PERFORM setval('time_tracking_id_seq', (SELECT MAX(id) FROM time_tracking)+1);
    END
$$ LANGUAGE plpgsql;

CALL import_all_from_csv();