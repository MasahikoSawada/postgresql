CREATE EXTENSION IF NOT EXISTS buffile;

-- This test only verifies that PG is compiled with a component file size of
-- 32 kB (i.e. 4 buffers of 8 kB) instead of 1 GB. That seems appropriate for
-- testing. Some other tests may rely on it.
BEGIN;
SELECT buffile_create();
-- Skip the first component file.
SELECT buffile_seek(0, 32768);
-- Write the first byte of the second component file. We can't simply
-- buffile_seek() beyond the position 32768 as it would return EOF.
SELECT buffile_write('a');
-- Enforce BufFileFlush(), which actually adds the component file.
SELECT buffile_read(1);
-- Check that we're in the 2nd file, i.e. the file size is as expected.
SELECT buffile_assert_fileno(1);
SELECT buffile_close();
COMMIT;
