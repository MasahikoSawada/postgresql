--
-- Visibility map
--
CREATE TABLE vmtest (i INT primary key);
INSERT INTO vmtest SELECT generate_series(1,10000);

\set VERBOSITY terse

-- All pages are become all-visible
VACUUM vmtest;
SELECT relallvisible  = (pg_relation_size('vmtest') / current_setting('block_size')::int) FROM pg_class WHERE relname = 'vmtest';

-- All pages are become all-frozen
VACUUM FREEZE vmtest;
SELECT relallfrozen  = (pg_relation_size('vmtest') / current_setting('block_size')::int) FROM pg_class WHERE relname = 'vmtest';

-- All pages are skipped according to VM
VACUUM FREEZE VERBOSE vmtest;

DROP TABLE vmtest;
