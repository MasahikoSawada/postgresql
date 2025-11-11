CREATE TABLE copy_data (a smallint, b integer, c bigint);
INSERT INTO copy_data VALUES (1, 2, 3), (12, 34, 56), (123, 456, 789);

COPY copy_data FROM stdin WITH (FORMAT 'test_format');
\.
COPY copy_data TO stdout WITH (FORMAT 'test_format');

-- Error
COPY copy_data FROM stdin WITH (FORMAT 'error');
COPY copy_data FROM stdin WITH (FORMAT 'text', FORMAT 'error');


-- Option handling, COPY FROM
COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_int 10);
\.
COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_int -10);
\.
COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_int 'a'); -- error

COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_bool 'true');
\.
COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_bool 'false');
\.
COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_bool 'hello'); -- error

COPY copy_data FROM stdin WITH (FORMAT 'test_format', common_int 100, common_bool false, from_str 'from option');
\.

COPY copy_data FROM stdin WITH (FORMAT 'test_format', invalid 'option'); -- error
COPY copy_data FROM stdin WITH (FORMAT 'test_format', format 'text'); -- error

-- Option handling, COPY FROM
COPY copy_data TO stdout WITH (FORMAT 'test_format', common_int -10, common_bool false, to_str 'to option');
COPY copy_data TO stdout WITH (FORMAT 'test_format', from_str 'to option'); -- error
