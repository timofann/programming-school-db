/* 1) Write a procedure for adding P2P check
   Parameters: nickname of the person being checked, checker's nickname, 
   task name, [P2P check status]( #check-status), time. If the status is 
   "start", add a record in the Checks table (use today's date). Add a 
   record in the P2P table. If the status is "start", specify the record 
   just added as a check, otherwise specify the check with the unfinished 
   P2P step. */

CREATE OR REPLACE PROCEDURE pr_add_p2p_check(
    new_checked_peer  text,
    new_checking_peer text,
    new_task_title    text,
    new_state         check_state,
    new_check_time    time
) AS $$
DECLARE
    new_check_id BIGINT := 0;
BEGIN
    IF new_state = 'start' THEN
        IF (SELECT count(*) FROM p2p JOIN checks c ON p2p.check_id = c.id
            WHERE p2p.checking_peer = new_checking_peer
              AND c.peer = new_checked_peer
              AND c.task = new_task_title) % 2 = 1
        THEN
            RAISE data_exception USING MESSAGE = 'The check cannot be added: peer has unfinished check';
        ELSE
            INSERT INTO checks (peer, task, check_date)
            VALUES (new_checked_peer, new_task_title, now())
            RETURNING id INTO new_check_id;

            INSERT INTO p2p (check_id, checking_peer, state, check_time)
            VALUES (new_check_id, new_checking_peer, new_state, new_check_time);
        END IF;
    ELSE
        IF (SELECT state FROM p2p JOIN checks c ON p2p.check_id = c.id
            WHERE p2p.checking_peer = new_checking_peer
              AND c.peer = new_checked_peer
              AND c.task = new_task_title
            ORDER BY p2p.id DESC LIMIT 1) != 'start'
        THEN
            RAISE data_exception USING MESSAGE = 'The check cannot be added: peer dont have started check';
        ELSE
            IF (SELECT state FROM p2p JOIN checks c ON p2p.check_id = c.id
                WHERE p2p.checking_peer = new_checking_peer
                    AND c.peer = new_checked_peer
                    AND c.task = new_task_title
                ORDER BY p2p.id DESC LIMIT 1) != 'start'
            THEN
                RAISE data_exception USING MESSAGE = 'The check cannot be added: peer dont have started check';
            ELSE
                new_check_id = (
                    SELECT c.id FROM p2p
                        INNER JOIN checks c ON c.id = p2p.check_id
                    WHERE c.peer = new_checked_peer
                        AND p2p.checking_peer = new_checking_peer
                        AND task = new_task_title
                    ORDER BY c.id DESC LIMIT 1
                );
                INSERT INTO p2p (check_id, checking_peer, state, check_time)
                VALUES (new_check_id, new_checking_peer, new_state, new_check_time);
            END IF;
        END IF;
    END IF;
END
$$ LANGUAGE plpgsql;

-- test for duplicate check
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: test for duplicate check';
END; $$;

-- test for unstarted p2p check
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: test for unstarted check';
END; $$;

-- test for success adding p2p check
TRUNCATE checks, p2p RESTART IDENTITY CASCADE;
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'failure'::check_state, '15:30:01');
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
SELECT * FROM p2p;
SELECT * FROM checks;

/* 2) Write a procedure for adding checking by Verter
   Parameters: nickname of the person being checked, task name, [Verter 
   check status](#check-status), time. Add a record to the Verter table 
   (as a check specify the check of the corresponding task with the latest 
   (by time) successful P2P step) */

CREATE OR REPLACE PROCEDURE pr_add_verter_check(
    new_checked_peer text,
    new_task_title   text,
    new_state        check_state,
    new_check_time   time
) AS $$
    DECLARE
        checkId bigint := (
            SELECT check_id
            FROM p2p
                JOIN checks ON p2p.check_id = checks.id
                    AND checks.task = new_task_title
                    AND checks.peer = new_checked_peer
            WHERE state = 'success'
            ORDER BY check_date, check_time DESC LIMIT 1);
        verter_started   check_state := (SELECT state FROM verter WHERE check_id = checkId AND state = 'start');
        verter_failed    check_state := (SELECT state FROM verter WHERE check_id = checkId AND state = 'failure');
        verter_finished  check_state := (SELECT state FROM verter WHERE check_id = checkId AND state != 'start');
    BEGIN
        IF (new_state = 'start' AND checkId IS NULL)
            THEN RAISE data_exception USING MESSAGE = 'P2P must be success'; END IF;

        IF (new_state = 'start' AND verter_started IS NOT NULL AND verter_failed IS NULL)
            THEN RAISE data_exception USING MESSAGE = 'Verter check has been already started'; END IF;

        IF (new_state != 'start' AND verter_started IS NULL)
            THEN RAISE data_exception USING MESSAGE = 'Verter check must be started'; END IF;

        IF (verter_finished IS NOT NULL OR verter_failed IS NOT NULL)
            THEN RAISE data_exception USING MESSAGE = 'Verter check has been already done'; END IF;

        INSERT INTO verter (check_id, state, check_time)
        VALUES (checkId, new_state, new_check_time);
    END
$$ LANGUAGE plpgsql;

-- FAILURE CASE: p2p must be success
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:02');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'failure'::check_state, '15:30:01');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '22:50:00');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: p2p must be success';
END; $$;

-- FAILURE CASE: Verter check has been already started
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:02');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '22:50:00');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '22:50:00');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: Verter check has been already started';
END; $$;

-- FAILURE CASE: Verter check must be started
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:02');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'failure', '22:50:00');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: Verter check must be started';
END; $$;

-- FAILURE CASE: Verter check has been already done
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:02');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '22:50:00');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'failure', '22:50:00');
    CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'failure', '22:50:00');
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: Verter check must be started';
END; $$;

-- SUCCESS CASES
TRUNCATE checks, p2p, verter RESTART IDENTITY CASCADE;
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '15:30:01');
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '15:30:01');
CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '16:50:00');
CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'failure', '16:50:01');

CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'start'::check_state, '16:55:01');
CALL pr_add_p2p_check('darrpama', 'myregree', 'C2_SimpleBashUtils', 'success'::check_state, '16:55:02');
CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'start', '22:50:00');
CALL pr_add_verter_check('darrpama', 'C2_SimpleBashUtils', 'success', '22:50:01');

SELECT * FROM p2p;
SELECT * FROM checks;
SELECT * FROM verter;


/* 3) Write a trigger: after adding a record with the "start" status to the P2P 
   table, change the corresponding record in the TransferredPoints table */

CREATE OR REPLACE FUNCTION fn_transfer_p2p_point() RETURNS TRIGGER AS
$$
    DECLARE
        checkedPeer text := (
            SELECT checks.peer
            FROM checks
                JOIN p2p ON p2p.check_id = checks.id AND p2p.check_id = NEW.check_id
            WHERE checking_peer = NEW.checking_peer LIMIT 1
        );
        transferRecord BOOL := (
            SELECT EXISTS(
                SELECT id FROM transferred_points
                WHERE checking_peer = NEW.checking_peer
                    AND checked_peer = checkedPeer
            )::BOOL
        );
    BEGIN
        IF NEW.state = 'start' THEN
            IF (transferRecord IS FALSE) THEN
                INSERT INTO transferred_points (checking_peer, checked_peer, points_amount)
                VALUES (NEW.checking_peer, checkedPeer, 1);
            ELSE
                UPDATE transferred_points tp SET points_amount = tp.points_amount + 1
                WHERE tp.checked_peer = checkedPeer
                  AND tp.checking_peer = NEW.checking_peer;
            END IF;
            RETURN NEW;
        ELSE
            RETURN NULL;
        END IF;
    END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_transfer_p2p_point ON p2p;
CREATE TRIGGER trg_transfer_p2p_point AFTER INSERT ON p2p FOR EACH ROW
EXECUTE FUNCTION fn_transfer_p2p_point();

-- test
DO $$ DECLARE tp_count bigint; BEGIN
    TRUNCATE checks, p2p, verter, xp, transferred_points RESTART IDENTITY CASCADE;
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C7_SmartCalc_v1.0', 'start'::check_state, '15:30:01');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C7_SmartCalc_v1.0', 'success'::check_state, '15:30:01');
    CALL pr_add_p2p_check('darrpama', 'myregree', 'C7_SmartCalc_v1.0', 'start'::check_state, '15:30:01');
    SELECT points_amount INTO tp_count FROM transferred_points WHERE checked_peer = 'darrpama' AND checking_peer = 'myregree';
    IF tp_count = 2 THEN RAISE NOTICE 'TEST PASSED: points count';
    ELSE RAISE EXCEPTION 'TEST FAILED: points count';
    END IF;
END; $$;

/* 4) Write a trigger: before adding a record to the XP table, check 
   if it is correct 
   The record is considered correct if:
   - The number of XP does not exceed the maximum available for the 
   task being checked
   - The Check field refers to a successful check
   If the record does not pass the check, do not add it to the table. */

CREATE OR REPLACE FUNCTION fn_trg_xp_max() RETURNS TRIGGER AS $xp$
    DECLARE
        maxXp         INT  := (SELECT max_xp FROM tasks INNER JOIN checks c on tasks.title LIKE c.task WHERE c.id = NEW.check_id);
        p2pCheck      BOOL := (SELECT EXISTS(SELECT id FROM p2p WHERE check_id = NEW.check_id AND state = 'success')::BOOL);
        verterIsset   BOOL := (SELECT EXISTS(SELECT id FROM verter WHERE check_id = NEW.check_id AND state = 'start')::BOOL);
        verterSuccess BOOL := (SELECT EXISTS(SELECT id FROM verter WHERE check_id = NEW.check_id AND state = 'success')::BOOL);
    BEGIN
        IF (NEW.xp_amount > maxXp)
            THEN RAISE data_exception USING MESSAGE = 'Cannot add xp - xp amount is greater than it should be'; END IF;

        IF (p2pCheck IS FALSE)
            THEN RAISE data_exception USING MESSAGE = 'Cannot add xp - P2P check is not success'; END IF;

        IF (verterIsset IS TRUE AND verterSuccess IS FALSE)
            THEN RAISE data_exception USING MESSAGE = 'Cannot add xp - Verter check is not success'; END IF;

        RETURN NEW;
    END;
$xp$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_xp_max ON xp;
CREATE TRIGGER trg_xp_max
    BEFORE INSERT OR UPDATE ON xp
    FOR EACH ROW EXECUTE PROCEDURE fn_trg_xp_max();

--  check is not isset  -- should FAIL
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter, xp, transferred_points RESTART IDENTITY CASCADE;
    INSERT INTO xp(check_id, xp_amount) VALUES (1, 200);
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: P2P check is not success';
END; $$;

--  p2p check is not isset  -- should FAIL
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
    INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
    INSERT INTO xp(check_id, xp_amount) VALUES (1, 200);
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: P2P check is not success';
END; $$;

--  p2p check has only started  -- should FAIL
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
    INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
    INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'start', '2023-03-30 22:25');
    INSERT INTO xp(check_id, xp_amount) VALUES (1, 200);
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: P2P check is not success';
END; $$;

--  Verter check has only started  -- should FAIL
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
    INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
    INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'start', '2023-03-30 22:25');
    INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'success', '2023-03-30 22:35');
    INSERT INTO verter(check_id, state, check_time) VALUES (1, 'start', '2023-03-30 22:45');
    INSERT INTO xp(check_id, xp_amount) VALUES (1, 200);
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: Verter check is not success';
END; $$;

--  p2p and Verter checks isset and has success but xp greater  -- should FAIL
DO $$ DECLARE err_msg text; BEGIN
    TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
    INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
    INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'start', '2023-03-30 22:25');
    INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'success', '2023-03-30 22:35');
    INSERT INTO verter(check_id, state, check_time) VALUES (1, 'start', '2023-03-30 22:45');
    INSERT INTO verter(check_id, state, check_time) VALUES (1, 'success', '2023-03-30 22:55');
    INSERT INTO xp(check_id, xp_amount) VALUES (1, 1000);
    EXCEPTION WHEN data_exception THEN RAISE NOTICE 'TEST PASSED: xp amount is greater than it should be';
END; $$;

--  p2p and Verter checks isset and has success. xp is correct  -- should SUCCESS
TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'start', '2023-03-30 22:25');
INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'success', '2023-03-30 22:35');
INSERT INTO verter(check_id, state, check_time) VALUES (1, 'start', '2023-03-30 22:45');
INSERT INTO verter(check_id, state, check_time) VALUES (1, 'success', '2023-03-30 22:55');
INSERT INTO xp(check_id, xp_amount) VALUES (1, 250);
--  p2p checks isset and has success. Verter not isset. Xp is correct  -- should SUCCESS
TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
INSERT INTO checks(peer, task, check_date) VALUES ('myregree', 'C2_SimpleBashUtils', '2023-03-30 22:25');
INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'start', '2023-03-30 22:25');
INSERT INTO p2p(check_id, checking_peer, state, check_time) VALUES (1, 'darrpama', 'success', '2023-03-30 22:35');
INSERT INTO xp(check_id, xp_amount) VALUES (1, 250);
-- --  clean up tables
-- TRUNCATE checks, p2p, verter, xp RESTART IDENTITY CASCADE;
