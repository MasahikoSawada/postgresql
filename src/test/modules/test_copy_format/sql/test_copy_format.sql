CREATE EXTENSION test_copy_format;
CREATE TABLE public.test (a smallint, b integer, c bigint);
INSERT INTO public.test VALUES (1, 2, 3), (12, 34, 56), (123, 456, 789);
-- 987 is accepted.
-- 654 is a hard error because ON_ERROR is stop by default.
COPY public.test FROM stdin WITH (FORMAT 'test_copy_format');
987
654
\.
-- 987 is accepted.
-- 654 is a soft error because ON_ERROR is ignore.
COPY public.test FROM stdin WITH (FORMAT 'test_copy_format', ON_ERROR ignore);
987
654
\.
-- 987 is accepted.
-- 654 is a soft error because ON_ERROR is ignore.
COPY public.test FROM stdin WITH (FORMAT 'test_copy_format', ON_ERROR ignore, LOG_VERBOSITY verbose);
987
654
\.
-- 987 is accepted.
-- 654 is a soft error because ON_ERROR is ignore.
-- 321 is a hard error.
COPY public.test FROM stdin WITH (FORMAT 'test_copy_format', ON_ERROR ignore);
987
654
321
\.
COPY public.test TO stdout WITH (FORMAT 'test_copy_format');
