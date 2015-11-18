--
-- Visibility Map
--
CREATE TABLE vmtest (i INT primary key);
INSERT INTO vmtest SELECT generate_series(1,10000);

-- All pages are become all-visible
VACUUM FREEZE vmtest;
SELECT relallvisible  = (pg_relation_size('vmtest') / current_setting('block_size')::int) FROM pg_class WHERE relname = 'vmtest';

-- Check whether vacuum skips all-frozen pages
\set VERBOSITY terse
VACUUM FREEZE VERBOSE vmtest;
\set VERBOSITY default

DROP TABLE vmtest;
