CREATE EXTENSION test_copy_format;
CREATE TABLE public.test (a INT, b INT, c INT);
INSERT INTO public.test VALUES (1, 2, 3), (12, 34, 56), (123, 456, 789);
COPY public.test FROM stdin WITH (format 'testfmt');
COPY public.test TO stdout WITH (format 'testfmt');
