setup
{
    CREATE EXTENSION injection_points;
    CREATE EXTENSION postgres_fdw;
    DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
	    )$$;
        END;
    $d$;
    CREATE USER MAPPING FOR public SERVER loopback;

    CREATE SCHEMA sch1;
    CREATE TABLE sch1.a (i int);
    CREATE FOREIGN TABLE sch1.b (i int) SERVER loopback OPTIONS (schema_name 'sch2', table_name 'b');
    CREATE FOREIGN TABLE sch1.c (i int) SERVER loopback OPTIONS (schema_name 'sch2', table_name 'c');

    CREATE SCHEMA sch2;
    CREATE TABLE sch2.b (i int);
    CREATE TABLE sch2.c (i int);

    INSERT INTO sch1.a VALUES (1);
    INSERT INTO sch2.b VALUES (1);
    INSERT INTO sch2.c VALUES (1);
}

session "s0"
setup {
    SELECT injection_points_set_local();
    SELECT injection_points_attach('heapam_lock_tuple-before-lock', 'wait');
}
step "s0_lock" {
    SELECT
       (SELECT 1 FROM sch1.b AS b, sch1.c AS c WHERE a.i = b.i AND b.i = c.i)
    FROM sch1.a as a
    FOR UPDATE;
}

session "s1"
step "s1_update" { UPDATE sch1.a SET i = i + 100; }
step "s1_wakeup" { SELECT injection_points_wakeup('heapam_lock_tuple-before-lock'); }

# "s0_lock" execute a FOR UPDATE query but it stops before locking the result
# tuple because of the injection point. "s1_update" updates the same tuple
# concurrently and we wake up the session "s0", resulting that the FOR UPDATE
# query rechecks the tuple via EPQ. Verify that postgresRecheckForeignScan()
# function can check the tuple properly.
permutation "s0_lock" "s1_update" "s1_wakeup"
