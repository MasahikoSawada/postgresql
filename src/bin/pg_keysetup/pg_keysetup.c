/*-------------------------------------------------------------------------
 *
 * pg_keysetup.c - Turn password into encryption key.
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_keysetup/pg_keysetup.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <unistd.h>

#include "port/pg_crc32c.h"
#include "storage/encryption.h"
#include "getopt_long.h"

#ifdef USE_ENCRYPTION
static const char *progname;

static void
usage(const char *progname)
{
	printf(_("%s derives encryption key from a password.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D] DATADIR    data directory\n"));
	printf(_("  -?, --help             show this help, then exit\n\n"));
	printf(_("Password is read from stdin and the key is sent to stdout\n"));
}

static void fatal_error(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * Big red button to push when things go horribly wrong.
 */
static void
fatal_error(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, _("%s: FATAL:  "), progname);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);

	exit(EXIT_FAILURE);
}

/*
 * This function does the same as read_kdf_file() in the backend. However most
 * of the calls look different on frontend side, it seems better to write it
 * here from scratch than to use too many #ifdef-#else-#endif constructs.
 */
static void
read_kdf_file(char *path)
{
	pg_crc32c	crc;
	int			fd;

	KDFParams = palloc(KDF_PARAMS_FILE_SIZE);

	fd = open(path, O_RDONLY | PG_BINARY, S_IRUSR);

	if (fd < 0)
		fatal_error("could not open key setup file \"%s\": %m",
					KDF_PARAMS_FILE);

	if (read(fd, KDFParams, sizeof(KDFParamsData)) != sizeof(KDFParamsData))
		fatal_error("could not read from key setup file: %m");

	close(fd);

	/* Now check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) KDFParams,
				offsetof(KDFParamsData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, KDFParams->crc))
		fatal_error("incorrect checksum in key setup file");

	if (KDFParams->function != KDF_OPENSSL_PKCS5_PBKDF2_HMAC_SHA)
		fatal_error("unsupported KDF function %d", KDFParams->function);
}
#endif							/* USE_ENCRYPTION */

int
main(int argc, char **argv)
{
#ifdef USE_ENCRYPTION
	int			c;
	char	   *DataDir = NULL;
	char		fpath[MAXPGPATH];
	int			fd;
	char		password[ENCRYPTION_PWD_MAX_LENGTH];
	size_t		pwd_len,
				i;

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_controldata (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt(argc, argv, "D:")) != -1)
	{
		switch (c)
		{
			case 'D':
				DataDir = optarg;
				break;

			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	if (DataDir == NULL)
	{
		if (optind < argc)
			DataDir = argv[optind++];
		else
			DataDir = getenv("PGDATA");
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (DataDir == NULL)
	{
		fprintf(stderr, _("%s: no data directory specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	snprintf(fpath, MAXPGPATH, "%s/%s", DataDir, KDF_PARAMS_FILE);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		fatal_error("could not open file \"%s\"", fpath);

	read_kdf_file(fpath);
	close(fd);

	/*
	 * Read the password.
	 */
	pwd_len = 0;
	while (true)
	{
		int			c = getchar();

		if (c == EOF || c == '\n')
			break;

		if (pwd_len >= ENCRYPTION_PWD_MAX_LENGTH)
			fatal_error("The password is too long");

		password[pwd_len++] = c;
	}

	if (pwd_len < ENCRYPTION_PWD_MIN_LENGTH)
		fatal_error("The password is too short");

	/*
	 * Run the key derivation function.
	 */
	setup_encryption_key(password, false, pwd_len);

	/*
	 * Finally print the encryption key.
	 */
	for (i = 0; i < ENCRYPTION_KEY_LENGTH; i++)
		printf("%.2x", encryption_key[i]);
	printf("\n");
#else
	ENCRYPTION_NOT_SUPPORTED_MSG;
#endif							/* USE_ENCRYPTION */
	return 0;
}
