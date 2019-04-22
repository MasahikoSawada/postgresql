BEGIN;
SELECT buffile_create();
-- Write data across component file boundary and try to read it.
SELECT buffile_seek(0, 32766);
SELECT buffile_write('abcd');
SELECT buffile_seek(0, 32766);
SELECT buffile_read(8);
SELECT buffile_close();
COMMIT;
