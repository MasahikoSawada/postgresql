/* contrib/amcheck/amcheck--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION amcheck" to load this file. \quit

--
-- bt_page_verify()
--
CREATE FUNCTION bt_page_verify(relname text, blkno int4)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_page_verify'
LANGUAGE C STRICT;

--
-- bt_parent_page_verify()
--
CREATE FUNCTION bt_parent_page_verify(relname text, blkno int4)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_parent_page_verify'
LANGUAGE C STRICT;

--
-- bt_index_verify()
--
CREATE FUNCTION bt_index_verify(relname text)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_verify'
LANGUAGE C STRICT;

--
-- bt_parent_index_verify()
--
CREATE FUNCTION bt_parent_index_verify(relname text)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_parent_index_verify'
LANGUAGE C STRICT;

--
-- bt_leftright_verify()
--
CREATE FUNCTION bt_leftright_verify(relname text)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_leftright_verify'
LANGUAGE C STRICT;
