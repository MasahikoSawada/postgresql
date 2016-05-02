CREATE OR REPLACE FUNCTION bt2handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD btree2 TYPE INDEX HANDLER bt2handler;

-- Opclasses

--CREATE OPERATOR CLASS int4_ops
--DEFAULT FOR TYPE int4 USING bloom AS
--	OPERATOR	1	=(int4, int4),
--	FUNCTION	1	hashint4(int4);

--CREATE OPERATOR CLASS text_ops
--DEFAULT FOR TYPE text USING bloom AS
--	OPERATOR	1	=(text, text),
--	FUNCTION	1	hashtext(text);
