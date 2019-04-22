-- This test shows that the first component file (segment) stays empty, read
-- stops prematurely even if it starts on that segment, even though it'd
-- otherwise receive some data from the following one.
BEGIN;
SELECT buffile_create();
SELECT buffile_seek(0, 32768);
SELECT buffile_write('a');
SELECT buffile_seek(0, 32767);
SELECT buffile_read(2);
SELECT buffile_close();
COMMIT;
