/* src/test/modules/test_copy_format/test_copy_format--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_copy_format" to load this file. \quit


CREATE FUNCTION testfmt(internal)
	RETURNS copy_handler
	AS 'MODULE_PATHNAME', 'copy_testfmt_handler' LANGUAGE C;
