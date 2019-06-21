/*-------------------------------------------------------------------------
 *
 * buffile.c
 *	  Management of large buffered temporary files.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/buffile.c
 *
 * NOTES:
 *
 * BufFiles provide a very incomplete emulation of stdio atop virtual Files
 * (as managed by fd.c).  Currently, we only support the buffered-I/O
 * aspect of stdio: a read or write of the low-level File occurs only
 * when the buffer is filled or emptied.  This is an even bigger win
 * for virtual Files than for ordinary kernel files, since reducing the
 * frequency with which a virtual File is touched reduces "thrashing"
 * of opening/closing file descriptors.
 *
 * Note that BufFile structs are allocated with palloc(), and therefore
 * will go away automatically at query/transaction end.  Since the underlying
 * virtual Files are made with OpenTemporaryFile, all resources for
 * the file are certain to be cleaned up even if processing is aborted
 * by ereport(ERROR).  The data structures required are made in the
 * palloc context that was current when the BufFile was created, and
 * any external resources such as temp files are owned by the ResourceOwner
 * that was current at that time.
 *
 * BufFile also supports temporary files that exceed the OS file size limit
 * (by opening multiple fd.c temporary files).  This is an essential feature
 * for sorts and hashjoins on large amounts of data.
 *
 * BufFile supports temporary files that can be made read-only and shared with
 * other backends, as infrastructure for parallel execution.  Such files need
 * to be created as a member of a SharedFileSet that all participants are
 * attached to.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "commands/tablespace.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/buffile.h"
#include "storage/buf_internals.h"
#include "utils/resowner.h"

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large BufFiles to be spread across multiple
 * tablespaces when available.
 */
#define MAX_PHYSICAL_FILESIZE	0x40000000
#define BUFFILE_SEG_SIZE		(MAX_PHYSICAL_FILESIZE / BLCKSZ)

/*
 * Fields that both BufFile and TransientBufFile structures need. It must be
 * the first field of those structures.
 */
typedef struct BufFileCommon
{
	bool		dirty;			/* does buffer need to be written? */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int			curFile;		/* file index (0..n) part of current pos,
								 * always zero for TransientBufFile */
	off_t		curOffset;		/* offset part of current pos */

	bool		readOnly;		/* has the file been set to read only? */

	bool		append;			/* should new data be appended to the end? */

	PGAlignedBlock buffer;
} BufFileCommon;

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile
{
	BufFileCommon common;		/* Common fields, see above. */

	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File	   *files;			/* palloc'd array with numFiles entries */

	bool		isInterXact;	/* keep open over transactions? */

	SharedFileSet *fileset;		/* space for segment files if shared */
	const char *name;			/* name of this BufFile if shared */

	/*
	 * resowner is the ResourceOwner to use for underlying temp files.  (We
	 * don't need to remember the memory context we're using explicitly,
	 * because after creation we only repalloc our arrays larger.)
	 */
	ResourceOwner resowner;
};

/*
 * Buffered variant of a transient file. Unlike BufFile this is simpler in
 * several ways: 1) it's not split into segments, 2) there's no need of seek,
 * 3) there's no need to combine read and write access.
 */
struct TransientBufFile
{
	BufFileCommon common;		/* Common fields, see above. */

	/* The underlying file. */
	char	   *path;
	int			fd;
};

static BufFile *makeBufFileCommon(int nfiles);
static BufFile *makeBufFile(File firstfile);
static void extendBufFile(BufFile *file);
static void BufFileLoadBuffer(BufFile *file);
static void BufFileDumpBuffer(BufFile *file);
static int	BufFileFlush(BufFile *file);
static File MakeNewSharedSegment(BufFile *file, int segment);

static void BufFileLoadBufferTransient(TransientBufFile *file);
static void BufFileDumpBufferTransient(TransientBufFile *file);

static size_t BufFileReadCommon(BufFileCommon *file, void *ptr, size_t size,
				  bool is_transient);
static size_t BufFileWriteCommon(BufFileCommon *file, void *ptr, size_t size,
				   bool is_transient);

/*
 * Create BufFile and perform the common initialization.
 */
static BufFile *
makeBufFileCommon(int nfiles)
{
	BufFile    *file = (BufFile *) palloc0(sizeof(BufFile));
	BufFileCommon *fcommon = &file->common;

	fcommon->dirty = false;
	fcommon->curFile = 0;
	fcommon->curOffset = 0L;
	fcommon->pos = 0;
	fcommon->nbytes = 0;

	file->numFiles = nfiles;
	file->isInterXact = false;
	file->resowner = CurrentResourceOwner;

	return file;
}

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isInterXact if appropriate.
 */
static BufFile *
makeBufFile(File firstfile)
{
	BufFile    *file = makeBufFileCommon(1);

	file->files = (File *) palloc(sizeof(File));
	file->files[0] = firstfile;
	file->common.readOnly = false;
	file->fileset = NULL;
	file->name = NULL;

	return file;
}

/*
 * Add another component temp file.
 */
static void
extendBufFile(BufFile *file)
{
	File		pfile;
	ResourceOwner oldowner;

	/* Be sure to associate the file with the BufFile's resource owner */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = file->resowner;

	if (file->fileset == NULL)
		pfile = OpenTemporaryFile(file->isInterXact);
	else
		pfile = MakeNewSharedSegment(file, file->numFiles);

	Assert(pfile >= 0);

	CurrentResourceOwner = oldowner;

	file->files = (File *) repalloc(file->files,
									(file->numFiles + 1) * sizeof(File));
	file->files[file->numFiles] = pfile;
	file->numFiles++;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than MAX_PHYSICAL_FILESIZE bytes are
 * written to it).
 *
 * If interXact is true, the temp file will not be automatically deleted
 * at end of transaction.
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context, and with a resource owner, that will survive across
 * transaction boundaries.
 */
BufFile *
BufFileCreateTemp(bool interXact)
{
	BufFile    *file;
	File		pfile;

	/*
	 * Ensure that temp tablespaces are set up for OpenTemporaryFile to use.
	 * Possibly the caller will have done this already, but it seems useful to
	 * double-check here.  Failure to do this at all would result in the temp
	 * files always getting placed in the default tablespace, which is a
	 * pretty hard-to-detect bug.  Callers may prefer to do it earlier if they
	 * want to be sure that any required catalog access is done in some other
	 * resource context.
	 */
	PrepareTempTablespaces();

	pfile = OpenTemporaryFile(interXact);
	Assert(pfile >= 0);

	file = makeBufFile(pfile);
	file->isInterXact = interXact;

	return file;
}

/*
 * Build the name for a given segment of a given BufFile.
 */
static void
SharedSegmentName(char *name, const char *buffile_name, int segment)
{
	snprintf(name, MAXPGPATH, "%s.%d", buffile_name, segment);
}

/*
 * Create a new segment file backing a shared BufFile.
 */
static File
MakeNewSharedSegment(BufFile *buffile, int segment)
{
	char		name[MAXPGPATH];
	File		file;

	/*
	 * It is possible that there are files left over from before a crash
	 * restart with the same name.  In order for BufFileOpenShared() not to
	 * get confused about how many segments there are, we'll unlink the next
	 * segment number if it already exists.
	 */
	SharedSegmentName(name, buffile->name, segment + 1);
	SharedFileSetDelete(buffile->fileset, name, true);

	/* Create the new segment. */
	SharedSegmentName(name, buffile->name, segment);
	file = SharedFileSetCreate(buffile->fileset, name);

	/* SharedFileSetCreate would've errored out */
	Assert(file > 0);

	return file;
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are attached to the same SharedFileSet using the same name.
 *
 * The naming scheme for shared BufFiles is left up to the calling code.  The
 * name will appear as part of one or more filenames on disk, and might
 * provide clues to administrators about which subsystem is generating
 * temporary file data.  Since each SharedFileSet object is backed by one or
 * more uniquely named temporary directory, names don't conflict with
 * unrelated SharedFileSet objects.
 */
BufFile *
BufFileCreateShared(SharedFileSet *fileset, const char *name)
{
	BufFile    *file;

	file = makeBufFileCommon(1);
	file->fileset = fileset;
	file->name = pstrdup(name);
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = MakeNewSharedSegment(file, 0);
	file->common.readOnly = false;

	return file;
}

/*
 * Open a file that was previously created in another backend (or this one)
 * with BufFileCreateShared in the same SharedFileSet using the same name.
 * The backend that created the file must have called BufFileClose() or
 * BufFileExportShared() to make sure that it is ready to be opened by other
 * backends and render it read-only.
 */
BufFile *
BufFileOpenShared(SharedFileSet *fileset, const char *name)
{
	BufFile    *file;
	char		segment_name[MAXPGPATH];
	Size		capacity = 16;
	File	   *files;
	int			nfiles = 0;

	files = palloc(sizeof(File) * capacity);

	/*
	 * We don't know how many segments there are, so we'll probe the
	 * filesystem to find out.
	 */
	for (;;)
	{
		/* See if we need to expand our file segment array. */
		if (nfiles + 1 > capacity)
		{
			capacity *= 2;
			files = repalloc(files, sizeof(File) * capacity);
		}
		/* Try to load a segment. */
		SharedSegmentName(segment_name, name, nfiles);
		files[nfiles] = SharedFileSetOpen(fileset, segment_name);
		if (files[nfiles] <= 0)
			break;
		++nfiles;

		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * If we didn't find any files at all, then no BufFile exists with this
	 * name.
	 */
	if (nfiles == 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open temporary file \"%s\" from BufFile \"%s\": %m",
						segment_name, name)));

	file = makeBufFileCommon(nfiles);
	file->files = files;
	file->common.readOnly = true;	/* Can't write to files opened this way */
	file->fileset = fileset;
	file->name = pstrdup(name);

	return file;
}

/*
 * Delete a BufFile that was created by BufFileCreateShared in the given
 * SharedFileSet using the given name.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the SharedFileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given name, and should know
 * that it exists and has been exported or closed.
 */
void
BufFileDeleteShared(SharedFileSet *fileset, const char *name)
{
	char		segment_name[MAXPGPATH];
	int			segment = 0;
	bool		found = false;

	/*
	 * We don't know how many segments the file has.  We'll keep deleting
	 * until we run out.  If we don't manage to find even an initial segment,
	 * raise an error.
	 */
	for (;;)
	{
		SharedSegmentName(segment_name, name, segment);
		if (!SharedFileSetDelete(fileset, segment_name, true))
			break;
		found = true;
		++segment;

		CHECK_FOR_INTERRUPTS();
	}

	if (!found)
		elog(ERROR, "could not delete unknown shared BufFile \"%s\"", name);
}

/*
 * BufFileExportShared --- flush and make read-only, in preparation for sharing.
 */
void
BufFileExportShared(BufFile *file)
{
	/* Must be a file belonging to a SharedFileSet. */
	Assert(file->fileset != NULL);

	/* It's probably a bug if someone calls this twice. */
	Assert(!file->common.readOnly);

	BufFileFlush(file);
	file->common.readOnly = true;
}

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
void
BufFileClose(BufFile *file)
{
	int			i;

	/* flush any unwritten data */
	BufFileFlush(file);
	/* close and delete the underlying file(s) */
	for (i = 0; i < file->numFiles; i++)
		FileClose(file->files[i]);
	/* release the buffer space */
	pfree(file->files);
	pfree(file);
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
static void
BufFileLoadBuffer(BufFile *file)
{
	File		thisfile;

	/*
	 * Advance to next component file if necessary and possible.
	 */
	if (file->common.curOffset >= MAX_PHYSICAL_FILESIZE &&
		file->common.curFile + 1 < file->numFiles)
	{
		file->common.curFile++;
		file->common.curOffset = 0L;
	}

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	thisfile = file->files[file->common.curFile];
	file->common.nbytes = FileRead(thisfile,
								   file->common.buffer.data,
								   sizeof(file->common.buffer),
								   file->common.curOffset,
								   WAIT_EVENT_BUFFILE_READ);
	if (file->common.nbytes < 0)
		file->common.nbytes = 0;
	/* we choose not to advance curOffset here */

	if (file->common.nbytes > 0)
		pgBufferUsage.temp_blks_read++;
}

/*
 * BufFileDumpBuffer
 *
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
static void
BufFileDumpBuffer(BufFile *file)
{
	int			wpos = 0;
	int			bytestowrite;
	File		thisfile;

	/*
	 * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
	 * crosses a component-file boundary; so we need a loop.
	 */
	while (wpos < file->common.nbytes)
	{
		off_t		availbytes;

		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (file->common.curOffset >= MAX_PHYSICAL_FILESIZE)
		{
			while (file->common.curFile + 1 >= file->numFiles)
				extendBufFile(file);
			file->common.curFile++;
			file->common.curOffset = 0L;
		}

		/*
		 * Determine how much we need to write into this file.
		 */
		bytestowrite = file->common.nbytes - wpos;
		availbytes = MAX_PHYSICAL_FILESIZE - file->common.curOffset;

		if ((off_t) bytestowrite > availbytes)
			bytestowrite = (int) availbytes;

		thisfile = file->files[file->common.curFile];
		bytestowrite = FileWrite(thisfile,
								 file->common.buffer.data + wpos,
								 bytestowrite,
								 file->common.curOffset,
								 WAIT_EVENT_BUFFILE_WRITE);
		if (bytestowrite <= 0)
			return;				/* failed to write */
		file->common.curOffset += bytestowrite;
		wpos += bytestowrite;

		pgBufferUsage.temp_blks_written++;
	}
	file->common.dirty = false;

	/*
	 * At this point, curOffset has been advanced to the end of the buffer,
	 * ie, its original value + nbytes.  We need to make it point to the
	 * logical file position, ie, original value + pos, in case that is less
	 * (as could happen due to a small backwards seek in a dirty buffer!)
	 */
	file->common.curOffset -= (file->common.nbytes - file->common.pos);
	if (file->common.curOffset < 0) /* handle possible segment crossing */
	{
		file->common.curFile--;
		Assert(file->common.curFile >= 0);
		file->common.curOffset += MAX_PHYSICAL_FILESIZE;
	}

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	file->common.pos = 0;
	file->common.nbytes = 0;
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size.
 */
size_t
BufFileRead(BufFile *file, void *ptr, size_t size)
{
	return BufFileReadCommon(&file->common, ptr, size, false);
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
size_t
BufFileWrite(BufFile *file, void *ptr, size_t size)
{
	return BufFileWriteCommon(&file->common, ptr, size, false);
}

/*
 * BufFileFlush
 *
 * Like fflush()
 */
static int
BufFileFlush(BufFile *file)
{
	if (file->common.dirty)
	{
		BufFileDumpBuffer(file);
		if (file->common.dirty)
			return EOF;
	}

	return 0;
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by off_t.
 * We do not support relative seeks across more than that, however.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
	int			newFile;
	off_t		newOffset;

	switch (whence)
	{
		case SEEK_SET:
			if (fileno < 0)
				return EOF;
			newFile = fileno;
			newOffset = offset;
			break;
		case SEEK_CUR:

			/*
			 * Relative seek considers only the signed offset, ignoring
			 * fileno. Note that large offsets (> 1 gig) risk overflow in this
			 * add, unless we have 64-bit off_t.
			 */
			newFile = file->common.curFile;
			newOffset = (file->common.curOffset + file->common.pos) + offset;
			break;
#ifdef NOT_USED
		case SEEK_END:
			/* could be implemented, not needed currently */
			break;
#endif
		default:
			elog(ERROR, "invalid whence: %d", whence);
			return EOF;
	}
	while (newOffset < 0)
	{
		if (--newFile < 0)
			return EOF;
		newOffset += MAX_PHYSICAL_FILESIZE;
	}
	if (newFile == file->common.curFile &&
		newOffset >= file->common.curOffset &&
		newOffset <= file->common.curOffset + file->common.nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		file->common.pos = (int) (newOffset - file->common.curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	if (BufFileFlush(file) != 0)
		return EOF;

	/*
	 * At this point and no sooner, check for seek past last segment. The
	 * above flush could have created a new segment, so checking sooner would
	 * not work (at least not with this code).
	 */

	/* convert seek to "start of next seg" to "end of last seg" */
	if (newFile == file->numFiles && newOffset == 0)
	{
		newFile--;
		newOffset = MAX_PHYSICAL_FILESIZE;
	}
	while (newOffset > MAX_PHYSICAL_FILESIZE)
	{
		if (++newFile >= file->numFiles)
			return EOF;
		newOffset -= MAX_PHYSICAL_FILESIZE;
	}
	if (newFile >= file->numFiles)
		return EOF;
	/* Seek is OK! */
	file->common.curFile = newFile;
	file->common.curOffset = newOffset;
	file->common.pos = 0;
	file->common.nbytes = 0;
	return 0;
}

void
BufFileTell(BufFile *file, int *fileno, off_t *offset)
{
	*fileno = file->common.curFile;
	*offset = file->common.curOffset + file->common.pos;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * LONG_MAX bytes, but that is quite a lot; we don't work
 * with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeekBlock(BufFile *file, long blknum)
{
	return BufFileSeek(file,
					   (int) (blknum / BUFFILE_SEG_SIZE),
					   (off_t) (blknum % BUFFILE_SEG_SIZE) * BLCKSZ,
					   SEEK_SET);
}

#ifdef NOT_USED
/*
 * BufFileTellBlock --- block-oriented tell
 *
 * Any fractional part of a block in the current seek position is ignored.
 */
long
BufFileTellBlock(BufFile *file)
{
	long		blknum;

	blknum = (file->common.curOffset + file->common.pos) / BLCKSZ;
	blknum += file->common.curFile * BUFFILE_SEG_SIZE;
	return blknum;
}

#endif

/*
 * Return the current shared BufFile size.
 *
 * Counts any holes left behind by BufFileAppend as part of the size.
 * ereport()s on failure.
 */
int64
BufFileSize(BufFile *file)
{
	int64		lastFileSize;

	Assert(file->fileset != NULL);

	/* Get the size of the last physical file. */
	lastFileSize = FileSize(file->files[file->numFiles - 1]);
	if (lastFileSize < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
						FilePathName(file->files[file->numFiles - 1]),
						file->name)));

	return ((file->numFiles - 1) * (int64) MAX_PHYSICAL_FILESIZE) +
		lastFileSize;
}

/*
 * Append the contents of source file (managed within shared fileset) to
 * end of target file (managed within same shared fileset).
 *
 * Note that operation subsumes ownership of underlying resources from
 * "source".  Caller should never call BufFileClose against source having
 * called here first.  Resource owners for source and target must match,
 * too.
 *
 * This operation works by manipulating lists of segment files, so the
 * file content is always appended at a MAX_PHYSICAL_FILESIZE-aligned
 * boundary, typically creating empty holes before the boundary.  These
 * areas do not contain any interesting data, and cannot be read from by
 * caller.
 *
 * Returns the block number within target where the contents of source
 * begins.  Caller should apply this as an offset when working off block
 * positions that are in terms of the original BufFile space.
 */
long
BufFileAppend(BufFile *target, BufFile *source)
{
	long		startBlock = target->numFiles * BUFFILE_SEG_SIZE;
	int			newNumFiles = target->numFiles + source->numFiles;
	int			i;

	Assert(target->fileset != NULL);
	Assert(source->common.readOnly);
	Assert(!source->common.dirty);
	Assert(source->fileset != NULL);

	if (target->resowner != source->resowner)
		elog(ERROR, "could not append BufFile with non-matching resource owner");

	target->files = (File *)
		repalloc(target->files, sizeof(File) * newNumFiles);
	for (i = target->numFiles; i < newNumFiles; i++)
		target->files[i] = source->files[i - target->numFiles];
	target->numFiles = newNumFiles;

	return startBlock;
}

/*
 * Open TransientBufFile at given path or create one if it does not
 * exist. User will be allowed either to write to the file or to read from it,
 * according to fileFlags, but not both.
 */
TransientBufFile *
BufFileOpenTransient(const char *path, int fileFlags)
{
	bool		readOnly;
	bool		append = false;
	TransientBufFile *file;
	BufFileCommon *fcommon;
	int			fd;
	off_t		size;

	/* Either read or write mode, but not both. */
	Assert((fileFlags & O_RDWR) == 0);

	/* Check whether user wants read or write access. */
	readOnly = (fileFlags & O_WRONLY) == 0;

	/*
	 * Append mode for read access is not useful, so don't bother implementing
	 * it.
	 */
	Assert(!(readOnly && append));

	errno = 0;
	fd = OpenTransientFile(path, fileFlags);
	if (fd < 0)
	{
		/*
		 * If caller wants to read from file and the file is not there, he
		 * should be able to handle the condition on his own.
		 *
		 * XXX Shouldn't we always let caller evaluate errno?
		 */
		if (errno == ENOENT && (fileFlags & O_RDONLY))
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	file = (TransientBufFile *) palloc(sizeof(TransientBufFile));
	fcommon = &file->common;
	fcommon->dirty = false;
	fcommon->pos = 0;
	fcommon->nbytes = 0;
	fcommon->readOnly = readOnly;
	fcommon->append = append;
	fcommon->curFile = 0;

	file->path = pstrdup(path);
	file->fd = fd;

	errno = 0;
	size = lseek(file->fd, 0, SEEK_END);
	if (errno > 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not initialize TransientBufFile for file \"%s\": %m",
						path)));

	if (fcommon->append)
	{
		/* Position the buffer at the end of the file. */
		fcommon->curOffset = size;
	}
	else
		fcommon->curOffset = 0L;

	return file;
}

/*
 * Close a TransientBufFile.
 */
void
BufFileCloseTransient(TransientBufFile *file)
{
	/* Flush any unwritten data. */
	if (!file->common.readOnly &&
		file->common.dirty && file->common.nbytes > 0)
	{
		BufFileDumpBufferTransient(file);

		/*
		 * Caller of BufFileWriteTransient() recognizes the failure to flush
		 * buffer by the returned value, however this function has no return
		 * code.
		 */
		if (file->common.dirty)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not flush file \"%s\": %m", file->path)));
	}

	if (CloseTransientFile(file->fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file->path)));

	pfree(file->path);
	pfree(file);
}

/*
 * Load some data into buffer, if possible, starting from file->offset.  At
 * call, must have dirty = false, pos and nbytes = 0.  On exit, nbytes is
 * number of bytes loaded.
 */
static void
BufFileLoadBufferTransient(TransientBufFile *file)
{
	Assert(file->common.readOnly);
	Assert(!file->common.dirty);
	Assert(file->common.pos == 0 && file->common.nbytes == 0);

retry:

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_BUFFILE_READ);
	file->common.nbytes = pg_pread(file->fd,
								   file->common.buffer.data,
								   sizeof(file->common.buffer),
								   file->common.curOffset);
	pgstat_report_wait_end();

	if (file->common.nbytes < 0)
	{
		/* TODO The W32 specific code, see FileWrite. */

		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		return;
	}
	/* we choose not to advance offset here */
}

/*
 * Write contents of a transient file buffer to disk.
 */
static void
BufFileDumpBufferTransient(TransientBufFile *file)
{
	int			nwritten;

	/* This function should only be needed during write access ... */
	Assert(!file->common.readOnly);

	/* ... and if there's some work to do. */
	Assert(file->common.dirty);
	Assert(file->common.nbytes > 0);

retry:
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_BUFFILE_WRITE);
	nwritten = pg_pwrite(file->fd,
						 file->common.buffer.data,
						 file->common.nbytes,
						 file->common.curOffset);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nwritten != file->common.nbytes && errno == 0)
		errno = ENOSPC;

	if (nwritten < 0)
	{
		/* TODO The W32 specific code, see FileWrite. */

		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		return;					/* failed to write */
	}

	file->common.dirty = false;

	file->common.pos = 0;
	file->common.nbytes = 0;
}

/*
 * Like BufFileRead() except it receives pointer to TransientBufFile.
 */
size_t
BufFileReadTransient(TransientBufFile *file, void *ptr, size_t size)
{
	return BufFileReadCommon(&file->common, ptr, size, true);
}

/*
 * Like BufFileWrite() except it receives pointer to TransientBufFile.
 */
size_t
BufFileWriteTransient(TransientBufFile *file, void *ptr, size_t size)
{
	return BufFileWriteCommon(&file->common, ptr, size, true);
}

/*
 * BufFileWriteCommon
 *
 * Functionality needed by both BufFileRead() and BufFileReadTransient().
 */
static size_t
BufFileReadCommon(BufFileCommon *file, void *ptr, size_t size,
				  bool is_transient)
{
	size_t		nread = 0;
	size_t		nthistime;

	if (file->dirty)
	{
		/*
		 * Transient file currently does not allow both read and write access,
		 * so this function should not see dirty buffer.
		 */
		Assert(!is_transient);

		if (BufFileFlush((BufFile *) file) != 0)
			return 0;			/* could not flush... */
		Assert(!file->dirty);
	}

	while (size > 0)
	{
		if (file->pos >= file->nbytes)
		{
			/* Try to load more data into buffer. */
			file->curOffset += file->pos;
			file->pos = 0;
			file->nbytes = 0;

			if (!is_transient)
				BufFileLoadBuffer((BufFile *) file);
			else
				BufFileLoadBufferTransient((TransientBufFile *) file);

			if (file->nbytes <= 0)
				break;			/* no more data available */
		}

		nthistime = file->nbytes - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(ptr, file->buffer.data + file->pos, nthistime);

		file->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}

	return nread;
}

/*
 * BufFileWriteCommon
 *
 * Functionality needed by both BufFileWrite() and BufFileWriteTransient().
 */
static size_t
BufFileWriteCommon(BufFileCommon *file, void *ptr, size_t size,
				   bool is_transient)
{
	size_t		nwritten = 0;
	size_t		nthistime;

	Assert(!file->readOnly);

	while (size > 0)
	{
		if (file->pos >= BLCKSZ)
		{
			/* Buffer full, dump it out */
			if (file->dirty)
			{
				if (!is_transient)
					BufFileDumpBuffer((BufFile *) file);
				else
					BufFileDumpBufferTransient((TransientBufFile *) file);

				if (file->dirty)
					break;		/* I/O error */
			}
			else
			{
				Assert(!is_transient);

				/* Hmm, went directly from reading to writing? */
				file->curOffset += file->pos;
				file->pos = 0;
				file->nbytes = 0;
			}
		}

		nthistime = BLCKSZ - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(file->buffer.data + file->pos, ptr, nthistime);

		file->dirty = true;
		file->pos += nthistime;
		if (file->nbytes < file->pos)
			file->nbytes = file->pos;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nwritten += nthistime;
	}

	return nwritten;
}
