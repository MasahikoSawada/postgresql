/*-------------------------------------------------------------------------
 *
 * pg_page_recover.c
 *	  Database page recovery
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pg_page_recover.h"

#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "common/controldata_utils.h"
#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "common/restricted_token.h"
#include "getopt_long.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"

#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

/* logging support */
#define pg_fatal(...) do { pg_log_fatal(__VA_ARGS__); exit(1); } while(0)

static void usage(const char *progname);
static char *slurpFile(const char *datadir, const char *path, size_t *filesize);
static void digestControlFile(ControlFileData *ControlFile, char *src, size_t size,
							  int *walSegSz_p, int *BlockSz_p, XLogRecPtr *checkPoint_p,
							  TimeLineID *timeLine_p);
static void checkControlFile(ControlFileData *ControlFile);
static TimeLineHistoryEntry *getTimelineHistory(ControlFileData *controlFile,
												int *nentries);
static TimeLineHistoryEntry *pr_parseTimeLineHistory(char *buffer,
													 TimeLineID targetTLI,
													 int *nentries);
static char *getDataBlock(const char *datadir, const char *path,
						  BlockNumber blkno);
static void writeDataBlock(const char *datadir, const char *path, BlockNumber blkno,
						   char *block);
static void recoverOnePage(char *page, const char *datadir, const char *relfilepath,
						   BlockNumber blkno, XLogRecPtr startpoint, XLogRecPtr endpoint);
static int SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
							  int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
							  TimeLineID *pageTLI);
static int RestoreArchivedWAL(const char *path, const char *xlogfname,
							  off_t expectedSize, const char *restoreCommand);
static void getTargetRelFileNodeFromPath(const char *relfilepath);
static void applyOneRecord(char *page, BlockNumber targetBlock,
						   XLogReaderState *record);

static ControlFileData ControlFile_target;
static ControlFileData ControlFile_base;
static XLogRecPtr RecoveryTargetPoint;
static TimeLineID RecoveryTargetPointTLI;
static XLogRecPtr RecoveryStartPoint;

typedef struct XLogPageReadPrivate
{
	const char *datadir;
	int tliIndex;
	const char *restoreCommand;
} XLogPageReadPrivate;

/* Configuration options */
char	*targetDataDir = NULL;
char	*baseDataDir = NULL;
char	*workingDir = NULL;
char	*targetRelFilePath = NULL;
char	*restoreCommand = NULL;
BlockNumber targetBlock = InvalidBlockNumber;

RelFileNode targetRNode;
char	*targetFileNode = NULL;

/* Target history */
TimeLineHistoryEntry *targetHistory;
int			targetNentries;

static int	xlogreadfd = -1;
static XLogSegNo xlogreadsegno = -1;
static char xlogfpath[MAXPGPATH];

const char *progname;
int			WalSegSz;
int 		BlockSz;

/* base page in base backup */
char	*baseBuffer;

/*
 * List of files excluded from checksum validation.
 *
 * Note: this list should be kept in sync with what basebackup.c includes.
 */
static const char *const skip[] = {
	"pg_control",
	"pg_filenode.map",
	"pg_internal.init",
	"PG_VERSION",
#ifdef EXEC_BACKEND
	"config_exec_params",
	"config_exec_params.new",
#endif
	NULL,
};

static void
usage(const char *progname)
{
	printf(_("%s muliple page recovery.\n\n"), progname);
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));
	printf(_("  -D, --target-pgdata=DIRECTORY      target data directory to recover\n"));
	printf(_("  -B, --base-pgdata=DIRECTORY        target data directory to recover\n"));
	printf(_("  -w, --working-dir=DIRECTORY        destination of WAL archives\n"));
	printf(_("  -R, --restore-command=COMMAND      restore_command\n"));
	printf(_("  -r, --relfile-block=FILENODE:BLKNO path to target relfile and block number\n"));
	printf(_("  -?, --help                         show this help, then exit\n"));
	printf(_("\nReport bugs to <pgsql-bugs@lists.postgresql.org>.\n"));
}

static void
dp(PageHeader page, const char *msg)
{
	int fd;
	char path[128];

	sprintf(path, "/tmp/%s", msg);

	if ((fd = open(path, O_CREAT | O_WRONLY | PG_BINARY,
			 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
		pg_log_fatal("open error %s %s, %m", msg, path);
	if (write(fd, (char *) page, BlockSz) != BlockSz)
		pg_log_fatal("write error %s, %m", msg);
	close(fd);

	fprintf(stderr, "[%s]: lsn %X/%X, csum %X, flag %u, lo %u, hi %u, sp %u, ver %u, pxid %u\n",
			msg,
			page->pd_lsn.xlogid, page->pd_lsn.xrecoff,
			page->pd_checksum,
			page->pd_flags,
			page->pd_lower, page->pd_upper,
			page->pd_special, page->pd_pagesize_version,
			page->pd_prune_xid);
}

static void
recoverOnePage(char *page, const char *datadir, const char *relfilepath,
			   BlockNumber blkno, XLogRecPtr startpoint, XLogRecPtr endpoint)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char *errormsg;
	XLogPageReadPrivate private;

	private.datadir = datadir;
	private.tliIndex = RecoveryTargetPointTLI;
	private.restoreCommand = restoreCommand;
	xlogreader = XLogReaderAllocate(WalSegSz, &SimpleXLogPageRead,
									&private);

	if (xlogreader == NULL)
		pg_log_fatal("out of memory");

	do
	{
		bool		use_this_record = false;
		int			block_id;
		RelFileNode rnode;
		ForkNumber	forknum;
		BlockNumber blkno;

		record = XLogReadRecord(xlogreader, startpoint, &errormsg);

		if (record == NULL)
		{
			XLogRecPtr	errptr;

			errptr = startpoint ? startpoint : xlogreader->EndRecPtr;

			if (errormsg)
				pg_fatal("could not read WAL record at %X/%X: %s",
						 (uint32) (errptr >> 32), (uint32) (errptr),
						 errormsg);
			else
				pg_fatal("could not read WAL record at %X/%X",
						 (uint32) (startpoint >> 32),
						 (uint32) (startpoint));
		}

		/* Check if this record contains block data we are interested in */
		for (block_id = 0; block_id <= xlogreader->max_block_id; block_id++)
		{
			if (!XLogRecGetBlockTag(xlogreader, block_id, &rnode,
									&forknum, &blkno))
				continue;

			if (blkno == targetBlock &&
				RelFileNodeEquals(rnode, targetRNode) &&
				forknum == MAIN_FORKNUM)
			{
				use_this_record = true;
				break;
			}
		}

		/* apply this WAL record to the page */
		if (use_this_record)
		{
			/*
			fprintf(stderr, "read WAL record [%X/%X] tot_len %u, xid %u, info %u, rmid %u",
					(uint32) (xlogreader->ReadRecPtr >> 32),
					(uint32) (xlogreader->ReadRecPtr),
					record->xl_tot_len, record->xl_xid, record->xl_info,
					record->xl_rmid);
			*/
			applyOneRecord(page, targetBlock, xlogreader);
		}

		startpoint = InvalidXLogRecPtr; /* continue reading at next record */
	} while (xlogreader->ReadRecPtr != endpoint);

	XLogReaderFree(xlogreader);
}

/*
 * Return a buffer of the target block
 */
static char *
getDataBlock(const char *datadir, const char *path, BlockNumber blkno)
{
	char		fullpath[MAXPGPATH];
	off_t		begin;
	char		*buffer;
	int			r;
	int			fd;

	begin = blkno * BlockSz;
	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (lseek(fd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in source file: %m");

	/* Always read BlockSz byte */
	buffer = pg_malloc(BlockSz + 1); /* +1 for termination */

	r = read(fd, buffer, BlockSz);

	if (r != BlockSz)
	{
		if (r < 0)
			pg_fatal("could not read file \"%s\": %m",
					 fullpath);

		/*
		 * It's quite possible that the recovery target block doesn't exit
		 * in the base backup. So we return NULL even if we could not read
		 * the block.
		 */
		return NULL;
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[BlockSz] = '\0';

	return buffer;
}

/*
 * Return a buffer of the target block
 */
static void
writeDataBlock(const char *datadir, const char *path, BlockNumber blkno,
			   char *block)
{
	char		fullpath[MAXPGPATH];
	off_t		begin;
	int			r;
	int			fd;

	begin = blkno * BlockSz;
	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_WRONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (lseek(fd, begin, SEEK_SET) == -1)
		pg_fatal("could not seek in source file: %m");

	r = write(fd, block, BlockSz);

	if (r < 0)
		pg_fatal("could not write file \"%s\": %m",
					 fullpath);
	close(fd);
}

/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
static char *
slurpFile(const char *datadir, const char *path, size_t *filesize)
{
	int			fd;
	char	   *buffer;
	struct stat statbuf;
	char		fullpath[MAXPGPATH];
	int			len;
	int			r;

	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	if (fstat(fd, &statbuf) < 0)
		pg_fatal("could not open file \"%s\" for reading: %m",
				 fullpath);

	len = statbuf.st_size;

	buffer = pg_malloc(len + 1);

	r = read(fd, buffer, len);
	if (r != len)
	{
		if (r < 0)
			pg_fatal("could not read file \"%s\": %m",
					 fullpath);
		else
			pg_fatal("could not read file \"%s\": read %d of %zu",
					 fullpath, r, (Size) len);
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[len] = '\0';

	if (filesize)
		*filesize = len;
	return buffer;
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void
digestControlFile(ControlFileData *ControlFile, char *src, size_t size,
				  int *walSegSz_p, int *BlockSz_p, XLogRecPtr *checkPoint_p, TimeLineID *timeLine_p)
{
	if (size != PG_CONTROL_FILE_SIZE)
		pg_fatal("unexpected control file size %d, expected %d",
				 (int) size, PG_CONTROL_FILE_SIZE);

	memcpy(ControlFile, src, sizeof(ControlFileData));

	/* set and validate WalSegSz and BlockSz */
	*walSegSz_p = ControlFile->xlog_seg_size;
	*BlockSz_p = ControlFile->blcksz;

	if (!IsValidWalSegSize(WalSegSz))
		pg_fatal(ngettext("WAL segment size must be a power of two between 1 MB and 1 GB, but the control file specifies %d byte",
						  "WAL segment size must be a power of two between 1 MB and 1 GB, but the control file specifies %d bytes",
						  WalSegSz),
				 WalSegSz);

	/* set and validate min recovery target */
	*checkPoint_p = ControlFile->checkPoint;
	*timeLine_p = ControlFile->checkPointCopy.ThisTimeLineID;

	if (XLogRecPtrIsInvalid(checkPoint_p) ||
		*timeLine_p< 0)
		pg_fatal("invalid recovey point: %lu", *checkPoint_p);

	/*
	fprintf(stderr, "read control file: recovery target point %X/%X, TLI %u\n",
			(uint32) ((*checkPoint_p) >> 32),
			(uint32) (*checkPoint_p),
			*timeLine_p);
			*/

	/* Additional checks on control file */
	checkControlFile(ControlFile);
}

/*
 * Check CRC of control file
 */
static void
checkControlFile(ControlFileData *ControlFile)
{
	pg_crc32c	crc;

	/* Calculate CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	/* And simply compare it */
	if (!EQ_CRC32C(crc, ControlFile->crc))
		pg_fatal("unexpected control file CRC");
}

static TimeLineHistoryEntry *
getTimelineHistory(ControlFileData *controlFile, int *nentries)
{
	TimeLineHistoryEntry *history;
	TimeLineID	tli;
	int			i;

	tli = controlFile->checkPointCopy.ThisTimeLineID;

	/*
	 * Timeline 1 does not have a history file, so there is no need to check
	 * and fake an entry with infinite start and end positions.
	 */
	if (tli == 1)
	{
		history = (TimeLineHistoryEntry *) pg_malloc(sizeof(TimeLineHistoryEntry));
		history->tli = tli;
		history->begin = history->end = InvalidXLogRecPtr;
		*nentries = 1;
	}
	else
	{
		char		path[MAXPGPATH];
		char	   *histfile;

		TLHistoryFilePath(path, tli);

		/* Get history file from appropriate source */
		histfile = slurpFile(targetDataDir, path, NULL);
		history = pr_parseTimeLineHistory(histfile, tli, nentries);
		pg_free(histfile);
	}

	/*
	 * Print the target timeline history.
	 */
	for (i = 0; i < *nentries; i++)
	{
		TimeLineHistoryEntry *entry;

		entry = &history[i];
		pg_log_debug("%d: %X/%X - %X/%X", entry->tli,
					 (uint32) (entry->begin >> 32), (uint32) (entry->begin),
					 (uint32) (entry->end >> 32), (uint32) (entry->end));
	}

	return history;
}

static TimeLineHistoryEntry *
pr_parseTimeLineHistory(char *buffer, TimeLineID targetTLI, int *nentries)
{
	char	   *fline;
	TimeLineHistoryEntry *entry;
	TimeLineHistoryEntry *entries = NULL;
	int			nlines = 0;
	TimeLineID	lasttli = 0;
	XLogRecPtr	prevend;
	char	   *bufptr;
	bool		lastline = false;

	/*
	 * Parse the file...
	 */
	prevend = InvalidXLogRecPtr;
	bufptr = buffer;
	while (!lastline)
	{
		char	   *ptr;
		TimeLineID	tli;
		uint32		switchpoint_hi;
		uint32		switchpoint_lo;
		int			nfields;

		fline = bufptr;
		while (*bufptr && *bufptr != '\n')
			bufptr++;
		if (!(*bufptr))
			lastline = true;
		else
			*bufptr++ = '\0';

		/* skip leading whitespace and check for # comment */
		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		nfields = sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);

		if (nfields < 1)
		{
			/* expect a numeric timeline ID as first field of line */
			pg_log_error("syntax error in history file: %s", fline);
			pg_log_error("Expected a numeric timeline ID.");
			exit(1);
		}
		if (nfields != 3)
		{
			pg_log_error("syntax error in history file: %s", fline);
			pg_log_error("Expected a write-ahead log switchpoint location.");
			exit(1);
		}
		if (entries && tli <= lasttli)
		{
			pg_log_error("invalid data in history file: %s", fline);
			pg_log_error("Timeline IDs must be in increasing sequence.");
			exit(1);
		}

		lasttli = tli;

		nlines++;
		entries = pg_realloc(entries, nlines * sizeof(TimeLineHistoryEntry));

		entry = &entries[nlines - 1];
		entry->tli = tli;
		entry->begin = prevend;
		entry->end = ((uint64) (switchpoint_hi)) << 32 | (uint64) switchpoint_lo;
		prevend = entry->end;

		/* we ignore the remainder of each line */
	}

	if (entries && targetTLI <= lasttli)
	{
		pg_log_error("invalid data in history file");
		pg_log_error("Timeline IDs must be less than child timeline's ID.");
		exit(1);
	}

	/*
	 * Create one more entry for the "tip" of the timeline, which has no entry
	 * in the history file.
	 */
	nlines++;
	if (entries)
		entries = pg_realloc(entries, nlines * sizeof(TimeLineHistoryEntry));
	else
		entries = pg_malloc(1 * sizeof(TimeLineHistoryEntry));

	entry = &entries[nlines - 1];
	entry->tli = targetTLI;
	entry->begin = prevend;
	entry->end = InvalidXLogRecPtr;

	*nentries = nlines;
	return entries;
}

/* XLogreader callback function, to read a WAL page */
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI)
{
	XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
	uint32		targetPageOff;
	XLogRecPtr	targetSegEnd;
	XLogSegNo	targetSegNo;
	int			r;

	XLByteToSeg(targetPagePtr, targetSegNo, WalSegSz);
	XLogSegNoOffsetToRecPtr(targetSegNo + 1, 0, WalSegSz, targetSegEnd);
	targetPageOff = XLogSegmentOffset(targetPagePtr, WalSegSz);

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (xlogreadfd >= 0 &&
		!XLByteInSeg(targetPagePtr, xlogreadsegno, WalSegSz))
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}

	XLByteToSeg(targetPagePtr, xlogreadsegno, WalSegSz);

	if (xlogreadfd < 0)
	{
		char		xlogfname[MAXFNAMELEN];

		/*
		 * Since incomplete segments are copied into next timelines, switch to
		 * the timeline holding the required segment. Assuming this scan can
		 * be done both forward and backward, consider also switching timeline
		 * accordingly.
		 */
		while (private->tliIndex < targetNentries - 1 &&
			   targetHistory[private->tliIndex].end < targetSegEnd)
			private->tliIndex++;
		while (private->tliIndex > 0 &&
			   targetHistory[private->tliIndex].begin >= targetSegEnd)
			private->tliIndex--;

		XLogFileName(xlogfname, targetHistory[private->tliIndex].tli,
					 xlogreadsegno, WalSegSz);

		snprintf(xlogfpath, MAXPGPATH, "%s/" XLOGDIR "/%s", private->datadir, xlogfname);

		fprintf(stderr, "open \"%s\"\n", xlogfpath);

		xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);

		if (xlogreadfd < 0)
		{

			xlogreadfd = RestoreArchivedWAL(workingDir,
											xlogfname,
											WalSegSz,
											private->restoreCommand);

			if (xlogreadfd < 0)
				return -1;
			else
				pg_log_debug("using restored from archive version of file \"%s\"",
							 xlogfpath);
		}
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(xlogreadfd != -1);

	/* Read the requested page */
	if (lseek(xlogreadfd, (off_t) targetPageOff, SEEK_SET) < 0)
	{
		pg_log_error("could not seek in file \"%s\": %m", xlogfpath);
		return -1;
	}


	r = read(xlogreadfd, readBuf, XLOG_BLCKSZ);
	if (r != XLOG_BLCKSZ)
	{
		if (r < 0)
			pg_log_error("could not read file \"%s\": %m", xlogfpath);
		else
			pg_log_error("could not read file \"%s\": read %d of %zu",
						 xlogfpath, r, (Size) XLOG_BLCKSZ);

		return -1;
	}

	Assert(targetSegNo == xlogreadsegno);

	*pageTLI = targetHistory[private->tliIndex].tli;
	return XLOG_BLCKSZ;
}

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful return a file descriptor of restored WAL file, else
 * return -1.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery. If the file size is not
 * known, set expectedSize = 0.
 */
static int
RestoreArchivedWAL(const char *path, const char *xlogfname,
				   off_t expectedSize, const char *restoreCommand)
{
	char		xlogpath[MAXPGPATH],
				xlogRestoreCmd[MAXPGPATH],
			   *dp,
			   *endp;
	const char *sp;
	int			rc,
				xlogfd;
	struct stat stat_buf;

	snprintf(xlogpath, MAXPGPATH, "%s/%s", workingDir, xlogfname);

	/*
	 * Construct the command to be executed.
	 */
	dp = xlogRestoreCmd;
	endp = xlogRestoreCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = restoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of target file */
					sp++;
					StrNCpy(dp, xlogpath, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of desired file */
					sp++;
					StrNCpy(dp, xlogfname, endp - dp);
					dp += strlen(dp);
					break;
				case 'r':
					/* %r: filename of last restartpoint */
					pg_fatal("restore_command with %%r cannot be used with pg_rewind.\n");
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	/*
	 * Execute restore_command, which should copy
	 * the missing WAL file from archival storage.
	 */
	rc = system(xlogRestoreCmd);

	if (rc == 0)
	{
		/*
		 * Command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				printf(_("archive file \"%s\" has wrong size: %lu instead of %lu, %s"),
						xlogfname, (unsigned long) stat_buf.st_size,
						(unsigned long) expectedSize, strerror(errno));
			}
			else
			{
				xlogfd = open(xlogpath, O_RDONLY | PG_BINARY, 0);

				if (xlogfd < 0)
					printf(_("could not open restored from archive file \"%s\": %s\n"),
							xlogpath, strerror(errno));
				else
					return xlogfd;
			}
		}
		else
		{
			/* Stat failed */
			printf(_("could not stat file \"%s\": %s"),
					xlogpath, strerror(errno));
		}
	}

	/*
	 * If the failure was due to any sort of signal, then it will be
	 * misleading to return message 'could not restore file...' and
	 * propagate result to the upper levels. We should exit right now.
	 */
	if (wait_result_is_any_signal(rc, false))
		pg_fatal("restore_command failed due to the signal: %s\n",
				 wait_result_to_str(rc));

	printf(_("could not restore file \"%s\" from archive\n"),
			xlogfname);

	return -1;
}

static void
getTargetRelFileNodeFromPath(const char *relfilepath)
{
	char *str = pg_strdup(relfilepath);
	char *p;

	/* get relNode */
	if ((p  = strrchr(str, '/')) == NULL)
		pg_log_fatal("invalid rel file path");
	targetRNode.relNode = (Oid) atoi(p + 1);

	*p = '\0';

	/* get dbNode */
	if ((p  = strrchr(str, '/')) == NULL)
		pg_log_fatal("invalid database path");
	targetRNode.dbNode = (Oid) atoi(p + 1);

	*p = '\0';

	/* get dbNode */
	if (strncmp(str, "base", 4) == 0)
		targetRNode.spcNode = DEFAULTTABLESPACE_OID;
	else
	{
		pg_log_fatal("invalid tablespace");
		exit(1);
	}

	fprintf(stderr, "set target RelFileNode: db %u, spc %u, rel %u\n",
			targetRNode.dbNode,
			targetRNode.spcNode,
			targetRNode.relNode);

	pg_free(str);
}

static bool
skipfile(const char *fn)
{
	const char *const *f;

	for (f = skip; *f; f++)
		if (strcmp(*f, fn) == 0)
			return true;

	return false;
}

static char *
scan_directory(const char *datadir, const char *basedir, const char *subdir)
{
	char		fullpath[MAXPGPATH];
	char		relativepath[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(relativepath, sizeof(relativepath), "%s/%s", basedir, subdir);
	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, relativepath);
	dir = opendir(fullpath);
	if (!dir)
	{
		pg_log_error("could not open directory \"%s\": %m", fullpath);
		exit(1);
	}

	while ((de = readdir(dir)) != NULL)
	{
		char		fn[MAXPGPATH];
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		/* Skip temporary files */
		if (strncmp(de->d_name,
					PG_TEMP_FILE_PREFIX,
					strlen(PG_TEMP_FILE_PREFIX)) == 0)
			continue;

		/* Skip temporary folders */
		if (strncmp(de->d_name,
					PG_TEMP_FILES_DIR,
					strlen(PG_TEMP_FILES_DIR)) == 0)
			continue;

		snprintf(fn, sizeof(fn), "%s/%s", fullpath, de->d_name);
		if (lstat(fn, &st) < 0)
		{
			pg_log_error("could not stat file \"%s\": %m", fn);
			exit(1);
		}
		if (S_ISREG(st.st_mode))
		{
			char		fnonly[MAXPGPATH];
			char	   *forkpath,
					   *segmentpath;
			BlockNumber segmentno = 0;

			if (skipfile(de->d_name))
				continue;

			/*
			 * Cut off at the segment boundary (".") to get the segment number
			 * in order to mix it into the checksum. Then also cut off at the
			 * fork boundary, to get the filenode the file belongs to for
			 * filtering.
			 */
			strlcpy(fnonly, de->d_name, sizeof(fnonly));
			segmentpath = strchr(fnonly, '.');
			if (segmentpath != NULL)
			{
				*segmentpath++ = '\0';
				segmentno = atoi(segmentpath);
				if (segmentno == 0)
				{
					pg_log_error("invalid segment number %d in file name \"%s\"",
								 segmentno, fn);
					exit(1);
				}
			}

			/* Skip forks */
			forkpath = strchr(fnonly, '_');
			if (forkpath != NULL)
				continue;

			if (strcmp(targetFileNode, fnonly) == 0)
			{
				char abspath[MAXPGPATH];

				/* Make a relative path to the relfile */
				snprintf(abspath, sizeof(abspath), "%s/%s", relativepath, fnonly);

				/* Found! */
				return strdup(abspath);
			}
		}
#ifndef WIN32
		else if (S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode))
#else
		else if (S_ISDIR(st.st_mode) || pgwin32_is_junction(fn))
#endif
		{
			char *p;
			if ((p = scan_directory(datadir, relativepath, de->d_name)) != NULL)
				return p;
		}
	}

	closedir(dir);

	/* could not find file */
	return NULL;
}

static void
applyOneRecord(char *page, BlockNumber targetBlock, XLogReaderState *record)
{
	RmgrId rmid = XLogRecGetRmid(record);

	switch (rmid)
	{
		case RM_HEAP_ID:
			heap_pageredo(page, targetBlock, record);
			break;
		default:
			pg_log_error("unexpected rmgr record: %u", rmid);
	}
}

int
main (int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"target-pgdata", required_argument, NULL, 'D'},
		{"base-pgdata", required_argument, NULL, 'B'},
		{"working-dir", required_argument, NULL, 'w'},
		{"relfile-block", required_argument, NULL, 'r'},
		{"restore-command", required_argument, NULL, 'R'},
		{NULL, 0, NULL, 0}
	};
	int	option_index;
	int c;
	size_t size;
	char *buffer;
	TimeLineID baseTimeline;
	int			baseWalSegSz;
	char	*orig;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_page_recover"));
	progname = get_progname(argv[0]);

	/* Process command-line arguments */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_page_recover (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:w:x:B:r:R:", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case '?':
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);

			case 'D':
				targetDataDir = pg_strdup(optarg);
				break;

			case 'B':
				baseDataDir = pg_strdup(optarg);
				break;

			case 'w':
				workingDir = pg_strdup(optarg);
				break;

			case 'R':
				restoreCommand = pg_strdup(optarg);
				break;

			case 'r':
			{
				char *p;

				targetFileNode = pg_strdup(optarg);

				if ((p = strchr(targetFileNode, ':')) == NULL)
				{
					pg_log_error("invalid relfile path and block number (--relfile-block)");
					fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
					exit(1);
				}

				*p = '\0';
				targetBlock = atoi(p + 1);
				break;
			}

		}
	}

	if (targetDataDir == NULL)
	{
		pg_log_error("no target cluster specified (--target-pgdata)");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (baseDataDir == NULL)
	{
		pg_log_error("no base cluster specified (--base-pgdata)");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (targetFileNode == NULL)
	{
		pg_log_error("no relfile node path specified (--relflie-block)");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (restoreCommand == NULL)
	{
		pg_log_error("no restore command specified (--restore-command)");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if ((targetRelFilePath = scan_directory(targetDataDir, "global", "")) == NULL &&
		(targetRelFilePath = scan_directory(targetDataDir, "base", "")) == NULL &&
		(targetRelFilePath = scan_directory(targetDataDir, "pg_tblspc", "")) == NULL)
	{
		pg_log_error("could not find relfile");
		exit(1);
	}

	/* Parse the relfile path and set targetRelFileNode */
	getTargetRelFileNodeFromPath(targetRelFilePath);

	/*
	 * Don't allow pg_page_recover to be run as root, to avoid overwriting the
	 * ownership of files in the data directory. We need only check for root
	 * -- any other user won't have sufficient permissions to modify files in
	 * the data directory.
	 */
#ifndef WIN32
	if (geteuid() == 0)
	{
		pg_log_error("cannot be executed by \"root\"");
		fprintf(stderr, _("You must run %s as the PostgreSQL superuser.\n"),
				progname);
		exit(1);
	}
#endif

	get_restricted_token();

	/* Set mask based on PGDATA permissions */
	if (!GetDataDirectoryCreatePerm(targetDataDir))
	{
		pg_log_error("could not read permissions of directory \"%s\": %m",
					 targetDataDir);
		exit(1);
	}

	umask(pg_mode_mask);

	/* Ok, we have all the options and we're ready to start */
	buffer = slurpFile(targetDataDir, "global/pg_control", &size);

	/*
	 * Read and check the ControlFile in target cluster. This also set both
	 * WAL segement size and recovery target LSN.
	 */
	digestControlFile(&ControlFile_target, buffer, size,
					  &WalSegSz, &BlockSz, &RecoveryTargetPoint, &RecoveryTargetPointTLI);

	targetHistory = getTimelineHistory(&ControlFile_target, &targetNentries);

	/*
	 * Read the target block from base database cluster and set the start
	 * LSN to read.
	 *
	 * @@@: it's possible the target block doesn't exist on base backup in case
	 * where the target relation has been created after backup.
	 */
	baseBuffer = getDataBlock(baseDataDir, targetRelFilePath, targetBlock);

	/*
	 * Find the recovery start point by checking the control file of base backup.
	 * We can always start to recovery from REDO point of the latest checkpoint
	 * of base backup; it's also possible that the PageLSN has a newer LSN if the
	 * page has been modified during checkpoint but we use the REDO point for safety.
	 */
	buffer = slurpFile(baseDataDir, "global/pg_control", &size);
	digestControlFile(&ControlFile_base, buffer, size,
					  &baseWalSegSz, &BlockSz, &RecoveryStartPoint, &baseTimeline);

	if (WalSegSz != baseWalSegSz)
		pg_log_error("WAL block size error");

	/*
	fprintf(stderr, "recovery %X/%X to %X/%X\n",
			(uint32) (RecoveryStartPoint >> 32),
			(uint32) (RecoveryStartPoint),
			(uint32) (RecoveryTargetPoint >> 32),
			(uint32) (RecoveryTargetPoint)
		);
	*/

	/* debug */
	orig = pg_malloc(BlockSz);
	memcpy(orig, baseBuffer, BlockSz);

	recoverOnePage(baseBuffer, targetDataDir, targetRelFilePath,
				   targetBlock, RecoveryStartPoint, RecoveryTargetPoint);

	if (ControlFile_target.data_checksum_version == 1)
	{
		uint16 csum;
		csum = pg_checksum_page(baseBuffer, targetBlock);
		((PageHeader) baseBuffer)->pd_checksum = csum;
		dp((PageHeader) baseBuffer, "rwc");
	}

	/* Write the recovered block data */
	writeDataBlock(targetDataDir, targetRelFilePath, targetBlock, baseBuffer);

	/* Sync target database cluster */
	fsync_pgdata(targetDataDir, PG_VERSION_NUM);

	{
		char *trg = getDataBlock(targetDataDir, targetRelFilePath, targetBlock);

		dp((PageHeader) orig, "bas");
		dp((PageHeader) baseBuffer, "res");
		dp((PageHeader) trg, "trg");
	}

	fprintf(stderr, "hello\n");
	return 0;
}
