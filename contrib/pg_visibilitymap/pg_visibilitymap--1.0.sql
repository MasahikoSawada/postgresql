/* contrib/pg_visibilitymap/pg_visibilitymap--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_visibilitymap" to load this file. \quit

-- Show visibility map information.
CREATE FUNCTION pg_visibilitymap(regclass, bigint)
RETURNS int4
AS 'MODULE_PATHNAME', 'pg_visibilitymap'
LANGUAGE C STRICT;

-- Show page status information.
CREATE FUNCTION pg_page_flags(regclass, bigint)
RETURNS int4
AS 'MODULE_PATHNAME', 'pg_page_flags'
LANGUAGE C STRICT;

-- pg_visibilitymap shows the visibility map bits for each block in a relation
CREATE FUNCTION
  pg_visibilitymap(rel regclass, blkno OUT bigint, mapbits OUT int4)
RETURNS SETOF RECORD
AS $$
  SELECT blkno, pg_visibilitymap($1, blkno) AS mapbits
  FROM generate_series(0, pg_relation_size($1) / current_setting('block_size')::bigint - 1) AS blkno;
$$
LANGUAGE SQL;

-- pg_visibility shows the visibility map bits and page-level bits for each
-- block in a relation.  this is more expensive than pg_visibilitymap since
-- we must read all of the pages.
CREATE FUNCTION
  pg_visibility(rel regclass, blkno OUT bigint, mapbits OUT int4,
                pagebits OUT int4)
RETURNS SETOF RECORD
AS $$
  SELECT blkno, pg_visibilitymap($1, blkno) AS mapbits,
      pg_page_flags($1, blkno) AS pagebits
  FROM generate_series(0, pg_relation_size($1) / current_setting('block_size')::bigint - 1) AS blkno;
$$
LANGUAGE SQL;

-- Don't want these to be available to public.
REVOKE ALL ON FUNCTION pg_visibilitymap(regclass, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_page_flags(regclass, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_visibilitymap(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_visibility(regclass) FROM PUBLIC;
