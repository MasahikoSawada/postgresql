/*
 *	file.c
 *
 *	file system operations
 *
 *	Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/file.c
 */

#include "postgres_fe.h"

#include "access/visibilitymap.h"
#include "pg_upgrade.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"

#include <fcntl.h>



#ifndef WIN32
static int	copy_file(const char *fromfile, const char *tofile, bool force);
#else
static int	win32_pghardlink(const char *src, const char *dst);
#endif

/* table for fast rewriting vm file in order to add all-frozen information */
static const uint16 rewrite_vm_table[256] = {
	0,     1,     4,     5,     16,    17,    20,    21,    64,    65,    68,    69,    80,    81,    84,    85,
	256,   257,   260,   261,   272,   273,   276,   277,   320,   321,   324,   325,   336,   337,   340,   341,
	1024,  1025,  1028,  1029,  1040,  1041,  1044,  1045,  1088,  1089,  1092,  1093,  1104,  1105,  1108,  1109,
	1280,  1281,  1284,  1285,  1296,  1297,  1300,  1301,  1344,  1345,  1348,  1349,  1360,  1361,  1364,  1365,
	4096,  4097,  4100,  4101,  4112,  4113,  4116,  4117,  4160,  4161,  4164,  4165,  4176,  4177,  4180,  4181,
	4352,  4353,  4356,  4357,  4368,  4369,  4372,  4373,  4416,  4417,  4420,  4421,  4432,  4433,  4436,  4437,
	5120,  5121,  5124,  5125,  5136,  5137,  5140,  5141,  5184,  5185,  5188,  5189,  5200,  5201,  5204,  5205,
	5376,  5377,  5380,  5381,  5392,  5393,  5396,  5397,  5440,  5441,  5444,  5445,  5456,  5457,  5460,  5461,
	16384, 16385, 16388, 16389, 16400, 16401, 16404, 16405, 16448, 16449, 16452, 16453, 16464, 16465, 16468, 16469,
	16640, 16641, 16644, 16645, 16656, 16657, 16660, 16661, 16704, 16705, 16708, 16709, 16720, 16721, 16724, 16725,
	17408, 17409, 17412, 17413, 17424, 17425, 17428, 17429, 17472, 17473, 17476, 17477, 17488, 17489, 17492, 17493,
	17664, 17665, 17668, 17669, 17680, 17681, 17684, 17685, 17728, 17729, 17732, 17733, 17744, 17745, 17748, 17749,
	20480, 20481, 20484, 20485, 20496, 20497, 20500, 20501, 20544, 20545, 20548, 20549, 20560, 20561, 20564, 20565,
	20736, 20737, 20740, 20741, 20752, 20753, 20756, 20757, 20800, 20801, 20804, 20805, 20816, 20817, 20820, 20821,
	21504, 21505, 21508, 21509, 21520, 21521, 21524, 21525, 21568, 21569, 21572, 21573, 21584, 21585, 21588, 21589,
	21760, 21761, 21764, 21765, 21776, 21777, 21780, 21781, 21824, 21825, 21828, 21829, 21840, 21841, 21844, 21845
};

/*
 * copyAndUpdateFile()
 *
 *	Copies a relation file from src to dst.
 */
const char *
copyAndUpdateFile(const char *src, const char *dst, bool force)
{
#ifndef WIN32
		if (copy_file(src, dst, force) == -1)
#else
		if (CopyFile(src, dst, !force) == 0)
#endif
			return getErrorText();
		else
			return NULL;
}


/*
 * linkAndUpdateFile()
 *
 * Creates a hard link between the given relation files. We use
 * this function to perform a true in-place update. If the on-disk
 * format of the new cluster is bit-for-bit compatible with the on-disk
 * format of the old cluster, we can simply link each relation
 * instead of copying the data from the old cluster to the new cluster.
 */
const char *
linkAndUpdateFile(const char *src, const char *dst)
{
	if (pg_link_file(src, dst) == -1)
		return getErrorText();
	else
		return NULL;
}


#ifndef WIN32
static int
copy_file(const char *srcfile, const char *dstfile, bool force)
{
#define COPY_BUF_SIZE (50 * BLCKSZ)

	int			src_fd;
	int			dest_fd;
	char	   *buffer;
	int			ret = 0;
	int			save_errno = 0;

	if ((srcfile == NULL) || (dstfile == NULL))
	{
		errno = EINVAL;
		return -1;
	}

	if ((src_fd = open(srcfile, O_RDONLY, 0)) < 0)
		return -1;

	if ((dest_fd = open(dstfile, O_RDWR | O_CREAT | (force ? 0 : O_EXCL), S_IRUSR | S_IWUSR)) < 0)
	{
		save_errno = errno;

		if (src_fd != 0)
			close(src_fd);

		errno = save_errno;
		return -1;
	}

	buffer = (char *) pg_malloc(COPY_BUF_SIZE);

	/* perform data copying i.e read src source, write to destination */
	while (true)
	{
		ssize_t		nbytes = read(src_fd, buffer, COPY_BUF_SIZE);

		if (nbytes < 0)
		{
			save_errno = errno;
			ret = -1;
			break;
		}

		if (nbytes == 0)
			break;

		errno = 0;

		if (write(dest_fd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			save_errno = errno;
			ret = -1;
			break;
		}
	}

	pg_free(buffer);

	if (src_fd != 0)
		close(src_fd);

	if (dest_fd != 0)
		close(dest_fd);

	if (save_errno != 0)
		errno = save_errno;

	return ret;
}
#endif


/*
 * rewriteVisibilityMap()
 *
 * Copies a visibility map file while adding all-frozen bit(0) into each bit.
 */
const char *
rewriteVisibilityMap(const char *fromfile, const char *tofile, bool force)
{
	int			src_fd = 0;
	int			dst_fd = 0;
	char		buffer[BLCKSZ];
	ssize_t 	bytesRead;
	int			rewriteVmBytesPerPage = (BLCKSZ - SizeOfPageHeaderData) / 2;
	BlockNumber	blkno = 0;

	/* Reset errno */
	errno = 0;

	if ((fromfile == NULL) || (tofile == NULL))
		return getErrorText();

	if ((src_fd = open(fromfile, O_RDONLY, 0)) < 0)
		goto err;

	if ((dst_fd = open(tofile, O_RDWR | O_CREAT | (force ? 0 : O_EXCL), S_IRUSR | S_IWUSR)) < 0)
		goto err;

	/* Perform data rewriting per page */
	while ((bytesRead = read(src_fd, buffer, BLCKSZ)) == BLCKSZ)
	{
		char	*cur, *end, *blkend;
		PageHeaderData	pageheader;
		uint16	vm_bits;

		/* Save the page header data */
		memcpy(&pageheader, buffer, SizeOfPageHeaderData);

		cur = buffer + SizeOfPageHeaderData;
		end = buffer + SizeOfPageHeaderData + rewriteVmBytesPerPage;
		blkend = buffer + bytesRead;

		while (blkend >= end)
		{
			char	vmbuf[BLCKSZ];
			char	*vmtmp = vmbuf;

			/* Copy page header in advance */
			memcpy(vmbuf, &pageheader, SizeOfPageHeaderData);

			vmtmp += SizeOfPageHeaderData;

			/* Rewrite visibility map bit one by one */
			while (end > cur)
			{
				/* Write rewritten bit from table and its string representation */
				vm_bits = rewrite_vm_table[(uint8) *cur];
				memcpy(vmtmp, &vm_bits, BITS_PER_HEAPBLOCK);

				cur++;
				vmtmp += BITS_PER_HEAPBLOCK;
			}

			/* Set new checksum for a visibility map page, If enabled */
			if (old_cluster.controldata.data_checksum_version != 0 &&
				new_cluster.controldata.data_checksum_version != 0)
				((PageHeader) vmbuf)->pd_checksum = pg_checksum_page(vmbuf, blkno);

			if (write(dst_fd, vmbuf, BLCKSZ) != BLCKSZ)
			{
					if (errno == 0)
						errno = ENOSPC;
					goto err;
			}

			end += rewriteVmBytesPerPage;
			blkno++;
		}
	}

err:
	if (src_fd != 0)
		close(src_fd);

	if (dst_fd != 0)
		close(dst_fd);

	return (errno == 0) ? NULL : getErrorText();
}

void
check_hard_link(void)
{
	char		existing_file[MAXPGPATH];
	char		new_link_file[MAXPGPATH];

	snprintf(existing_file, sizeof(existing_file), "%s/PG_VERSION", old_cluster.pgdata);
	snprintf(new_link_file, sizeof(new_link_file), "%s/PG_VERSION.linktest", new_cluster.pgdata);
	unlink(new_link_file);		/* might fail */

	if (pg_link_file(existing_file, new_link_file) == -1)
	{
		pg_fatal("Could not create hard link between old and new data directories: %s\n"
				 "In link mode the old and new data directories must be on the same file system volume.\n",
				 getErrorText());
	}
	unlink(new_link_file);
}

#ifdef WIN32
static int
win32_pghardlink(const char *src, const char *dst)
{
	/*
	 * CreateHardLinkA returns zero for failure
	 * http://msdn.microsoft.com/en-us/library/aa363860(VS.85).aspx
	 */
	if (CreateHardLinkA(dst, src, NULL) == 0)
		return -1;
	else
		return 0;
}
#endif


/* fopen() file with no group/other permissions */
FILE *
fopen_priv(const char *path, const char *mode)
{
	mode_t		old_umask = umask(S_IRWXG | S_IRWXO);
	FILE	   *fp;

	fp = fopen(path, mode);
	umask(old_umask);

	return fp;
}
