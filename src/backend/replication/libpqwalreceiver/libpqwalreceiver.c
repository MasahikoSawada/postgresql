/*-------------------------------------------------------------------------
 *
 * libpqwalreceiver.c
 *
 * This file contains the libpq-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * libpq.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/libpqwalreceiver/libpqwalreceiver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>

#include "libpq-fe.h"
#include "pqexpbuffer.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "replication/walreceiver.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

PG_MODULE_MAGIC;

struct WalReceiverConnHandle {
	/* Current connection to the primary, if any */
	PGconn *streamConn;
	/* Buffer for currently read records */
	char   *recvBuf;
};

PGDLLEXPORT WalReceiverConnHandle *_PG_walreceirver_conn_init(WalReceiverConnAPI *wrcapi);

/* Prototypes for interface functions */
static void libpqrcv_connect(WalReceiverConnHandle *handle, char *conninfo,
							 bool logical, const char *connname);
static char *libpqrcv_get_conninfo(WalReceiverConnHandle *handle);
static char *libpqrcv_identify_system(WalReceiverConnHandle *handle,
									  TimeLineID *primary_tli,
									  char **dbname);
static void libpqrcv_readtimelinehistoryfile(WalReceiverConnHandle *handle,
											 TimeLineID tli, char **filename,
											 char **content, int *len);
static char *libpqrcv_create_slot(WalReceiverConnHandle *handle,
								  char *slotname, bool logical,
								  XLogRecPtr *lsn);
static void libpqrcv_drop_slot(WalReceiverConnHandle *handle, char *slotname);
static bool libpqrcv_startstreaming_physical(WalReceiverConnHandle *handle,
								 TimeLineID tli, XLogRecPtr startpoint,
								 char *slotname);
static bool libpqrcv_startstreaming_logical(WalReceiverConnHandle *handle,
								XLogRecPtr startpoint, char *slotname,
								char *options);
static void libpqrcv_endstreaming(WalReceiverConnHandle *handle,
								  TimeLineID *next_tli);
static int	libpqrcv_receive(WalReceiverConnHandle *handle, char **buffer,
							 pgsocket *wait_fd);
static void libpqrcv_send(WalReceiverConnHandle *handle, const char *buffer,
						  int nbytes);
static List *libpqrcv_list_tables(WalReceiverConnHandle *handle,
								 char *slotname, char *options);
static bool libpqrcv_copy_table(WalReceiverConnHandle *handle,
								char *slotname, char *nspname,
								char *relname, char *options);
static void libpqrcv_disconnect(WalReceiverConnHandle *handle);

/* Prototypes for private functions */
static bool libpq_select(WalReceiverConnHandle *handle,
						 int timeout_ms);
static PGresult *libpqrcv_PQexec(WalReceiverConnHandle *handle,
								 const char *query);

/*
 * Module initialization callback
 */
WalReceiverConnHandle *
_PG_walreceirver_conn_init(WalReceiverConnAPI *wrcapi)
{
	WalReceiverConnHandle *handle;

	handle = palloc0(sizeof(WalReceiverConnHandle));

	/* Tell caller how to reach us */
	wrcapi->connect = libpqrcv_connect;
	wrcapi->get_conninfo = libpqrcv_get_conninfo;
	wrcapi->identify_system = libpqrcv_identify_system;
	wrcapi->readtimelinehistoryfile = libpqrcv_readtimelinehistoryfile;
	wrcapi->create_slot = libpqrcv_create_slot;
	wrcapi->drop_slot = libpqrcv_drop_slot;
	wrcapi->startstreaming_physical = libpqrcv_startstreaming_physical;
	wrcapi->startstreaming_logical = libpqrcv_startstreaming_logical;
	wrcapi->endstreaming = libpqrcv_endstreaming;
	wrcapi->receive = libpqrcv_receive;
	wrcapi->send = libpqrcv_send;
	wrcapi->disconnect = libpqrcv_disconnect;
	wrcapi->copy_table = libpqrcv_copy_table;
	wrcapi->list_tables = libpqrcv_list_tables;

	return handle;
}

/*
 * Establish the connection to the primary server for XLOG streaming
 */
static void
libpqrcv_connect(WalReceiverConnHandle *handle, char *conninfo, bool logical,
				 const char *connname)
{
	const char *keys[5];
	const char *vals[5];
	int			i = 0;

	/*
	 * We use the expand_dbname parameter to process the connection string (or
	 * URI), and pass some extra options. The deliberately undocumented
	 * parameter "replication=true" makes it a replication connection. The
	 * database name is ignored by the server in replication mode, but specify
	 * "replication" for .pgpass lookup.
	 */
	keys[i] = "dbname";
	vals[i] = conninfo;
	keys[++i] = "replication";
	vals[i] = logical ? "database" : "true";
	if (!logical)
	{
		keys[++i] = "dbname";
		vals[i] = "replication";
	}
	keys[++i] = "fallback_application_name";
	vals[i] = connname;
	keys[++i] = NULL;
	vals[i] = NULL;

	handle->streamConn = PQconnectdbParams(keys, vals,
											   /* expand_dbname = */ true);
	if (PQstatus(handle->streamConn) != CONNECTION_OK)
		ereport(ERROR,
				(errmsg("could not connect to the primary server: %s",
						PQerrorMessage(handle->streamConn))));
}

/*
 * Return a user-displayable conninfo string.  Any security-sensitive fields
 * are obfuscated.
 */
static char *
libpqrcv_get_conninfo(WalReceiverConnHandle *handle)
{
	PQconninfoOption *conn_opts;
	PQconninfoOption *conn_opt;
	PQExpBufferData buf;
	char	   *retval;

	Assert(handle->streamConn != NULL);

	initPQExpBuffer(&buf);
	conn_opts = PQconninfo(handle->streamConn);

	if (conn_opts == NULL)
		ereport(ERROR,
				(errmsg("could not parse connection string: %s",
						_("out of memory"))));

	/* build a clean connection string from pieces */
	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		bool		obfuscate;

		/* Skip debug and empty options */
		if (strchr(conn_opt->dispchar, 'D') ||
			conn_opt->val == NULL ||
			conn_opt->val[0] == '\0')
			continue;

		/* Obfuscate security-sensitive options */
		obfuscate = strchr(conn_opt->dispchar, '*') != NULL;

		appendPQExpBuffer(&buf, "%s%s=%s",
						  buf.len == 0 ? "" : " ",
						  conn_opt->keyword,
						  obfuscate ? "********" : conn_opt->val);
	}

	PQconninfoFree(conn_opts);

	retval = PQExpBufferDataBroken(buf) ? NULL : pstrdup(buf.data);
	termPQExpBuffer(&buf);
	return retval;
}

/*
 * Check that primary's system identifier matches ours, and fetch the current
 * timeline ID of the primary.
 */
static char *
libpqrcv_identify_system(WalReceiverConnHandle *handle,
						 TimeLineID *primary_tli,
						 char **dbname)
{
	char	   *sysid;
	PGresult   *res;

	/*
	 * Get the system identifier and timeline ID as a DataRow message from the
	 * primary server.
	 */
	res = libpqrcv_PQexec(handle, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not receive database system identifier and timeline ID from "
						"the primary server: %s",
						PQerrorMessage(handle->streamConn))));
	}
	if (PQnfields(res) < 3 || PQntuples(res) != 1)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		ereport(ERROR,
				(errmsg("invalid response from primary server"),
				 errdetail("Could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields.",
						   ntuples, nfields, 3, 1)));
	}
	sysid = pstrdup(PQgetvalue(res, 0, 0));
	*primary_tli = pg_atoi(PQgetvalue(res, 0, 1), 4, 0);
	if (dbname)
	{
		if (PQgetisnull(res, 0, 3))
			*dbname = NULL;
		else
			*dbname = pstrdup(PQgetvalue(res, 0, 3));
	}

	PQclear(res);

	return sysid;
}

/*
 * Create new replication slot.
 */
static char *
libpqrcv_create_slot(WalReceiverConnHandle *handle, char *slotname,
					 bool logical, XLogRecPtr *lsn)
{
	PGresult	   *res;
	char			cmd[256];
	char		   *snapshot;

	if (logical)
		snprintf(cmd, sizeof(cmd),
				 "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
				 slotname, "pgoutput");
	else
		snprintf(cmd, sizeof(cmd),
				 "CREATE_REPLICATION_SLOT \"%s\"", slotname);

	res = libpqrcv_PQexec(handle, cmd);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(ERROR, "could not crate replication slot \"%s\": %s\n",
			 slotname, PQerrorMessage(handle->streamConn));
	}

	*lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
					  CStringGetDatum(PQgetvalue(res, 0, 1))));
	snapshot = pstrdup(PQgetvalue(res, 0, 2));

	PQclear(res);

	return snapshot;
}

/*
 * Drop replication slot.
 */
static void
libpqrcv_drop_slot(WalReceiverConnHandle *handle, char *slotname)
{
	PGresult	   *res;
	char			cmd[256];

	snprintf(cmd, sizeof(cmd),
			 "DROP_REPLICATION_SLOT \"%s\"", slotname);

	res = libpqrcv_PQexec(handle, cmd);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		elog(ERROR, "could not drop replication slot \"%s\": %s\n",
			 slotname, PQerrorMessage(handle->streamConn));
	}

	PQclear(res);
}

/*
 * Start streaming WAL data from given startpoint and timeline.
 *
 * Returns true if we switched successfully to copy-both mode. False
 * means the server received the command and executed it successfully, but
 * didn't switch to copy-mode.  That means that there was no WAL on the
 * requested timeline and starting point, because the server switched to
 * another timeline at or before the requested starting point. On failure,
 * throws an ERROR.
 */
static bool
libpqrcv_startstreaming_physical(WalReceiverConnHandle *handle,
								 TimeLineID tli, XLogRecPtr startpoint,
								 char *slotname)
{
	char		cmd[256];
	PGresult   *res;

	/* Start streaming from the point requested by startup process */
	if (slotname != NULL)
		snprintf(cmd, sizeof(cmd),
				 "START_REPLICATION SLOT \"%s\" %X/%X TIMELINE %u", slotname,
				 (uint32) (startpoint >> 32), (uint32) startpoint, tli);
	else
		snprintf(cmd, sizeof(cmd),
				 "START_REPLICATION %X/%X TIMELINE %u",
				 (uint32) (startpoint >> 32), (uint32) startpoint, tli);
	res = libpqrcv_PQexec(handle, cmd);

	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		PQclear(res);
		return false;
	}
	else if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not start WAL streaming: %s",
						PQerrorMessage(handle->streamConn))));
	}
	PQclear(res);
	return true;
}

/*
 * Same as above but for logical stream.
 *
 * The ERROR scenario can be that the options were incorrect for given
 * slot.
 */
static bool
libpqrcv_startstreaming_logical(WalReceiverConnHandle *handle,
								XLogRecPtr startpoint, char *slotname,
								char *options)
{
	StringInfoData	cmd;
	PGresult	   *res;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
					 slotname,
					 (uint32) (startpoint >> 32),
					 (uint32) startpoint);

	/* Add options */
	if (options)
		appendStringInfo(&cmd, "( %s )", options);

	res = libpqrcv_PQexec(handle, cmd.data);

	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		PQclear(res);
		return false;
	}
	else if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not start WAL streaming: %s",
						PQerrorMessage(handle->streamConn))));
	}
	PQclear(res);
	pfree(cmd.data);
	return true;
}


/*
 * Stop streaming WAL data. Returns the next timeline's ID in *next_tli, as
 * reported by the server, or 0 if it did not report it.
 */
static void
libpqrcv_endstreaming(WalReceiverConnHandle *handle, TimeLineID *next_tli)
{
	PGresult   *res;

	if (PQputCopyEnd(handle->streamConn, NULL) <= 0 ||
		PQflush(handle->streamConn))
		ereport(ERROR,
			(errmsg("could not send end-of-streaming message to primary: %s",
					PQerrorMessage(handle->streamConn))));

	*next_tli = 0;

	/*
	 * After COPY is finished, we should receive a result set indicating the
	 * next timeline's ID, or just CommandComplete if the server was shut
	 * down.
	 *
	 * If we had not yet received CopyDone from the backend, PGRES_COPY_OUT
	 * is also possible in case we aborted the copy in mid-stream.
	 */
	res = PQgetResult(handle->streamConn);
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		/*
		 * Read the next timeline's ID. The server also sends the timeline's
		 * starting point, but it is ignored.
		 */
		if (PQnfields(res) < 2 || PQntuples(res) != 1)
			ereport(ERROR,
					(errmsg("unexpected result set after end-of-streaming")));
		*next_tli = pg_atoi(PQgetvalue(res, 0, 0), sizeof(uint32), 0);
		PQclear(res);

		/* the result set should be followed by CommandComplete */
		res = PQgetResult(handle->streamConn);
	}
	else if (PQresultStatus(res) == PGRES_COPY_OUT)
	{
		PQclear(res);

		/* End the copy */
		PQendcopy(handle->streamConn);

		/* CommandComplete should follow */
		res = PQgetResult(handle->streamConn);
	}

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		ereport(ERROR,
				(errmsg("error reading result of streaming command: %s",
						PQerrorMessage(handle->streamConn))));
	PQclear(res);

	/* Verify that there are no more results */
	res = PQgetResult(handle->streamConn);
	if (res != NULL)
		ereport(ERROR,
				(errmsg("unexpected result after CommandComplete: %s",
						PQerrorMessage(handle->streamConn))));
}

/*
 * Fetch the timeline history file for 'tli' from primary.
 */
static void
libpqrcv_readtimelinehistoryfile(WalReceiverConnHandle *handle,
								 TimeLineID tli, char **filename,
								 char **content, int *len)
{
	PGresult   *res;
	char		cmd[64];

	/*
	 * Request the primary to send over the history file for given timeline.
	 */
	snprintf(cmd, sizeof(cmd), "TIMELINE_HISTORY %u", tli);
	res = libpqrcv_PQexec(handle, cmd);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not receive timeline history file from "
						"the primary server: %s",
						PQerrorMessage(handle->streamConn))));
	}
	if (PQnfields(res) != 2 || PQntuples(res) != 1)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		ereport(ERROR,
				(errmsg("invalid response from primary server"),
				 errdetail("Expected 1 tuple with 2 fields, got %d tuples with %d fields.",
						   ntuples, nfields)));
	}
	*filename = pstrdup(PQgetvalue(res, 0, 0));

	*len = PQgetlength(res, 0, 1);
	*content = palloc(*len);
	memcpy(*content, PQgetvalue(res, 0, 1), *len);
	PQclear(res);
}

/*
 * Wait until we can read WAL stream, or timeout.
 *
 * Returns true if data has become available for reading, false if timed out
 * or interrupted by signal.
 *
 * This is based on pqSocketCheck.
 */
static bool
libpq_select(WalReceiverConnHandle *handle, int timeout_ms)
{
	int			ret;

	Assert(handle->streamConn != NULL);
	if (PQsocket(handle->streamConn) < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("invalid socket: %s",
						PQerrorMessage(handle->streamConn))));

	/* We use poll(2) if available, otherwise select(2) */
	{
#ifdef HAVE_POLL
		struct pollfd input_fd;

		input_fd.fd = PQsocket(handle->streamConn);
		input_fd.events = POLLIN | POLLERR;
		input_fd.revents = 0;

		ret = poll(&input_fd, 1, timeout_ms);
#else							/* !HAVE_POLL */

		fd_set		input_mask;
		struct timeval timeout;
		struct timeval *ptr_timeout;

		FD_ZERO(&input_mask);
		FD_SET(PQsocket(handle->streamConn), &input_mask);

		if (timeout_ms < 0)
			ptr_timeout = NULL;
		else
		{
			timeout.tv_sec = timeout_ms / 1000;
			timeout.tv_usec = (timeout_ms % 1000) * 1000;
			ptr_timeout = &timeout;
		}

		ret = select(PQsocket(handle->streamConn) + 1, &input_mask,
					 NULL, NULL, ptr_timeout);
#endif   /* HAVE_POLL */
	}

	if (ret == 0 || (ret < 0 && errno == EINTR))
		return false;
	if (ret < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("select() failed: %m")));
	return true;
}

/*
 * Send a query and wait for the results by using the asynchronous libpq
 * functions and the backend version of select().
 *
 * We must not use the regular blocking libpq functions like PQexec()
 * since they are uninterruptible by signals on some platforms, such as
 * Windows.
 *
 * We must also not use vanilla select() here since it cannot handle the
 * signal emulation layer on Windows.
 *
 * The function is modeled on PQexec() in libpq, but only implements
 * those parts that are in use in the walreceiver.
 *
 * Queries are always executed on the connection in streamConn.
 */
static PGresult *
libpqrcv_PQexec(WalReceiverConnHandle *handle, const char *query)
{
	PGresult   *result = NULL;
	PGresult   *lastResult = NULL;

	/*
	 * PQexec() silently discards any prior query results on the connection.
	 * This is not required for walreceiver since it's expected that walsender
	 * won't generate any such junk results.
	 */

	/*
	 * Submit a query. Since we don't use non-blocking mode, this also can
	 * block. But its risk is relatively small, so we ignore that for now.
	 */
	if (!PQsendQuery(handle->streamConn, query))
		return NULL;

	for (;;)
	{
		/*
		 * Receive data until PQgetResult is ready to get the result without
		 * blocking.
		 */
		while (PQisBusy(handle->streamConn))
		{
			/*
			 * We don't need to break down the sleep into smaller increments,
			 * since we'll get interrupted by signals and can either handle
			 * interrupts here or  elog(FATAL) within SIGTERM signal handler
			 * if the signal arrives in the middle of establishment of
			 * replication connection.
			 */
			if (!libpq_select(handle, -1))
			{
				CHECK_FOR_INTERRUPTS();
				continue;		/* interrupted */
			}
			if (PQconsumeInput(handle->streamConn) == 0)
				return NULL;	/* trouble */
		}

		/*
		 * Emulate the PQexec()'s behavior of returning the last result when
		 * there are many. Since walsender will never generate multiple
		 * results, we skip the concatenation of error messages.
		 */
		result = PQgetResult(handle->streamConn);
		if (result == NULL)
			break;				/* query is complete */

		PQclear(lastResult);
		lastResult = result;

		if (PQresultStatus(lastResult) == PGRES_COPY_IN ||
			PQresultStatus(lastResult) == PGRES_COPY_OUT ||
			PQresultStatus(lastResult) == PGRES_COPY_BOTH ||
			PQstatus(handle->streamConn) == CONNECTION_BAD)
			break;
	}

	return lastResult;
}

/*
 * Run the LIST_TABLES command which will send list of the tables to copy
 * in whatever format the plugin choses.
 */
static List *
libpqrcv_list_tables(WalReceiverConnHandle *handle, char *slotname,
					 char *options)
{
	StringInfoData	cmd;
	PGresult	   *res;
	int				i;
	List		   *tablelist = NIL;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "LIST_TABLES SLOT \"%s\"",
					 slotname);

	/* Add options */
	if (options)
		appendStringInfo(&cmd, "( %s )", options);

	res = libpqrcv_PQexec(handle, cmd.data);
	pfree(cmd.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not receive list of replicated tables from the provider: %s",
						PQerrorMessage(handle->streamConn))));
	}
	if (PQnfields(res) != 3)
	{
		int nfields = PQnfields(res);
		PQclear(res);
		ereport(ERROR,
				(errmsg("invalid response from provider"),
				 errdetail("Expected 3 fields, got %d fields.", nfields)));
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		LogicalRepTableListEntry *entry;

		entry = palloc(sizeof(LogicalRepTableListEntry));
		entry->nspname = pstrdup(PQgetvalue(res, i, 0));
		entry->relname = pstrdup(PQgetvalue(res, i, 1));
		if (!PQgetisnull(res, i, 2))
			entry->info = pstrdup(PQgetvalue(res, i, 2));
		else
			entry->info = NULL;

		tablelist = lappend(tablelist, entry);
	}

	PQclear(res);

	return tablelist;
}

/*
 * Run the COPY_TABLE command which will start streaming the existing data
 * in the table.
 */
static bool
libpqrcv_copy_table(WalReceiverConnHandle *handle, char *slotname,
					char *nspname, char *relname, char *options)
{
	StringInfoData	cmd;
	PGresult	   *res;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "COPY_TABLE SLOT \"%s\" TABLE \"%s\" \"%s\"",
					 slotname, nspname, relname);

	/* Add options */
	if (options)
		appendStringInfo(&cmd, "( %s )", options);

	res = libpqrcv_PQexec(handle, cmd.data);

	if (PQresultStatus(res) == PGRES_COMMAND_OK)
	{
		PQclear(res);
		return false;
	}
	else if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not start initial table contents streaming: %s",
						PQerrorMessage(handle->streamConn))));
	}
	PQclear(res);
	pfree(cmd.data);
	return true;
}

/*
 * Disconnect connection to primary, if any.
 */
static void
libpqrcv_disconnect(WalReceiverConnHandle *handle)
{
	PQfinish(handle->streamConn);
	handle->streamConn = NULL;
}

/*
 * Receive a message available from XLOG stream.
 *
 * Returns:
 *
 *	 If data was received, returns the length of the data. *buffer is set to
 *	 point to a buffer holding the received message. The buffer is only valid
 *	 until the next libpqrcv_* call.
 *
 *	 If no data was available immediately, returns 0, and *wait_fd is set to a
 *	 socket descriptor which can be waited on before trying again.
 *
 *	 -1 if the server ended the COPY.
 *
 * ereports on error.
 */
static int
libpqrcv_receive(WalReceiverConnHandle *handle, char **buffer,
				 pgsocket *wait_fd)
{
	int			rawlen;

	if (handle->recvBuf != NULL)
		PQfreemem(handle->recvBuf);
	handle->recvBuf = NULL;

	/* Try to receive a CopyData message */
	rawlen = PQgetCopyData(handle->streamConn, &handle->recvBuf, 1);
	if (rawlen == 0)
	{
		/* Try consuming some data. */
		if (PQconsumeInput(handle->streamConn) == 0)
			ereport(ERROR,
					(errmsg("could not receive data from WAL stream: %s",
							PQerrorMessage(handle->streamConn))));

		/* Now that we've consumed some input, try again */
		rawlen = PQgetCopyData(handle->streamConn, &handle->recvBuf, 1);
		if (rawlen == 0)
		{
			/* Tell caller to try again when our socket is ready. */
			*wait_fd = PQsocket(handle->streamConn);
			return 0;
		}
	}
	if (rawlen == -1)			/* end-of-streaming or error */
	{
		PGresult   *res;

		res = PQgetResult(handle->streamConn);
		if (PQresultStatus(res) == PGRES_COMMAND_OK ||
			PQresultStatus(res) == PGRES_COPY_IN)
		{
			PQclear(res);
			return -1;
		}
		else
		{
			PQclear(res);
			ereport(ERROR,
					(errmsg("could not receive data from WAL stream: %s",
							PQerrorMessage(handle->streamConn))));
		}
	}
	if (rawlen < -1)
		ereport(ERROR,
				(errmsg("could not receive data from WAL stream: %s",
						PQerrorMessage(handle->streamConn))));

	/* Return received messages to caller */
	*buffer = handle->recvBuf;
	return rawlen;
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
static void
libpqrcv_send(WalReceiverConnHandle *handle, const char *buffer, int nbytes)
{
	if (PQputCopyData(handle->streamConn, buffer, nbytes) <= 0 ||
		PQflush(handle->streamConn))
		ereport(ERROR,
				(errmsg("could not send data to WAL stream: %s",
						PQerrorMessage(handle->streamConn))));
}
