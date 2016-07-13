--
-- PUBLICATION
--

CREATE PUBLICATION testpub_default;

CREATE PUBLICATION testpib_ins_trunct WITH noreplicate_delete noreplicate_update;

ALTER PUBLICATION testpub_default WITH noreplicate_insert noreplicate_delete;

\dRp

ALTER PUBLICATION testpub_default WITH replicate_insert replicate_delete;

\dRp

--- adding tables
CREATE SCHEMA pub_test;
CREATE TABLE testpub_tbl1 (id serial primary key, data text);
CREATE TABLE pub_test.testpub_nopk (foo int, bar int);
CREATE VIEW testpub_view AS SELECT 1;

CREATE PUBLICATION testpub_foralltables FOR ALL TABLES noreplicate_delete noreplicate_update;
-- fail - there is table with nopk
ALTER PUBLICATION testpub_foralltables WITH replicate_update;

CREATE TABLE testpub_tbl2 (id serial primary key, data text);
-- fail - already added
ALTER PUBLICATION testpub_foralltables ADD TABLE testpub_tbl2;
-- fail - can't drop from all tables publication
ALTER PUBLICATION testpub_foralltables DROP TABLE testpub_tbl2;

\d+ testpub_tbl2

ALTER PUBLICATION testpub_foralltables SET TABLE pub_test.testpub_nopk;
SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'testpub_foralltables';

\d+ testpub_tbl2

DROP TABLE testpub_tbl2;
DROP PUBLICATION testpub_foralltables;

-- fail - view
CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_view;
-- fail - no pk
CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_tbl1, pub_test.testpub_nopk;

CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_tbl1;

\dRp+ testpub_fortbl

-- fail - view
ALTER PUBLICATION testpub_default ADD TABLE testpub_view;
-- fail - no pk
ALTER PUBLICATION testpub_default ADD TABLE pub_test.testpub_nopk;
-- fail - no pk
ALTER PUBLICATION testpub_default SET TABLE pub_test.testpub_nopk;
-- fail - no pk
ALTER PUBLICATION testpub_default ADD TABLE ALL IN SCHEMA pub_test;
-- fail - no pk
ALTER PUBLICATION testpub_default SET TABLE ALL IN SCHEMA pub_test;

ALTER PUBLICATION testpub_default ADD TABLE testpub_tbl1;
ALTER PUBLICATION testpub_default SET TABLE testpub_tbl1;
ALTER PUBLICATION testpub_default ADD TABLE pub_test.testpub_nopk;

ALTER PUBLICATION testpib_ins_trunct ADD TABLE pub_test.testpub_nopk, testpub_tbl1;

\d+ pub_test.testpub_nopk
\d+ testpub_tbl1

-- fail
ALTER TABLE testpub_tbl1 DROP CONSTRAINT testpub_tbl1_pkey RESTRICT;
ALTER TABLE testpub_tbl1 DROP CONSTRAINT testpub_tbl1_pkey CASCADE;
-- fail nonexistent
ALTER PUBLICATION testpub_default DROP TABLE testpub_tbl1, pub_test.testpub_nopk;
ALTER PUBLICATION testpub_default DROP TABLE pub_test.testpub_nopk;

\d+ testpub_tbl1

DROP VIEW testpub_view;

-- fail deps
DROP TABLE testpub_tbl1;

DROP PUBLICATION testpub_default;
DROP PUBLICATION testpib_ins_trunct;

DROP SCHEMA pub_test CASCADE;
