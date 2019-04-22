BEGIN;
SELECT buffile_create();
-- Write data at component file boundary and try to read it.
SELECT buffile_seek(0, 32768);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 32768);
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
