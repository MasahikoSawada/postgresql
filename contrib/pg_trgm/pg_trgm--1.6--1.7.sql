/* contrib/pg_trgm/pg_trgm--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_trgm UPDATE TO '1.7'" to load this file. \quit

-- 'unsigned char' version support function for gist
CREATE FUNCTION gin_extract_value_trgm_unsigned(text, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gin_extract_query_trgm_unsigned(text, internal, int2, internal, internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS gin_trgm_ops_unsigned
FOR TYPE text USING gin
AS
        OPERATOR        1       % (text, text),
        FUNCTION        1       btint4cmp (int4, int4),
        FUNCTION        2       gin_extract_value_trgm_unsigned (text, internal),
        FUNCTION        3       gin_extract_query_trgm_unsigned (text, internal, int2, internal, internal, internal, internal),
        FUNCTION        4       gin_trgm_consistent (internal, int2, text, int4, internal, internal, internal, internal),
        STORAGE         int4;

ALTER OPERATOR FAMILY gin_trgm_ops_unsigned USING gin ADD
        OPERATOR        3       pg_catalog.~~ (text, text),
        OPERATOR        4       pg_catalog.~~* (text, text),
        OPERATOR        5       pg_catalog.~ (text, text),
        OPERATOR        6       pg_catalog.~* (text, text),
        OPERATOR        7       %> (text, text),
        OPERATOR        9       %>> (text, text),
        OPERATOR        11      pg_catalog.= (text, text),
        FUNCTION        6      (text,text) gin_trgm_triconsistent (internal, int2, text, int4, internal, internal, internal);

-- 'unsigned char' version support functions for gist
CREATE FUNCTION gtrgm_compress_unsigned(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtrgm_consistent_unsigned(internal,text,smallint,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION gtrgm_distance_unsigned(internal,text,smallint,oid,internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS gist_trgm_ops_unsigned
FOR TYPE text USING gist
AS
        OPERATOR        1       % (text, text),
        FUNCTION        1       gtrgm_consistent_unsigned (internal, text, smallint, oid, internal),
        FUNCTION        2       gtrgm_union (internal, internal),
        FUNCTION        3       gtrgm_compress_unsigned (internal),
        FUNCTION        4       gtrgm_decompress (internal),
        FUNCTION        5       gtrgm_penalty (internal, internal, internal),
        FUNCTION        6       gtrgm_picksplit (internal, internal),
        FUNCTION        7       gtrgm_same (gtrgm, gtrgm, internal),
        STORAGE         gtrgm;

ALTER OPERATOR FAMILY gist_trgm_ops_unsigned USING gist ADD
        OPERATOR        2       <-> (text, text) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        3       pg_catalog.~~ (text, text),
        OPERATOR        4       pg_catalog.~~* (text, text),
        OPERATOR        5       pg_catalog.~ (text, text),
        OPERATOR        6       pg_catalog.~* (text, text),
        OPERATOR        7       %> (text, text),
        OPERATOR        8       <->> (text, text) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        9       %>> (text, text),
        OPERATOR        10       <->>> (text, text) FOR ORDER BY pg_catalog.float_ops,
        OPERATOR        11       pg_catalog.= (text, text),
        FUNCTION        8 (text, text)  gtrgm_distance_unsigned (internal, text, smallint, oid, internal),
	FUNCTION	10 (text) gtrgm_options (internal);
