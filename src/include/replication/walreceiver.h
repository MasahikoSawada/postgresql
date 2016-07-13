/*-------------------------------------------------------------------------
 *
 * walreceiver.h
 *	  Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgtime.h"

/* user-settable parameters */
extern int	wal_receiver_status_interval;
extern int	wal_receiver_timeout;
extern bool hot_standby_feedback;

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO		1024

/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() (EnableHotStandby && max_wal_senders > 0)

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum
{
	WALRCV_STOPPED,				/* stopped and mustn't start up again */
	WALRCV_STARTING,			/* launched, but the process hasn't
								 * initialized yet */
	WALRCV_STREAMING,			/* walreceiver is streaming */
	WALRCV_WAITING,				/* stopped streaming, waiting for orders */
	WALRCV_RESTARTING,			/* asked to restart streaming */
	WALRCV_STOPPING				/* requested to stop, but still running */
} WalRcvState;

/* Shared memory area for management of walreceiver process */
typedef struct
{
	/*
	 * PID of currently active walreceiver process, its current state and
	 * start time (actually, the time at which it was requested to be
	 * started).
	 */
	pid_t		pid;
	WalRcvState walRcvState;
	pg_time_t	startTime;

	/*
	 * receiveStart and receiveStartTLI indicate the first byte position and
	 * timeline that will be received. When startup process starts the
	 * walreceiver, it sets these to the point where it wants the streaming to
	 * begin.
	 */
	XLogRecPtr	receiveStart;
	TimeLineID	receiveStartTLI;

	/*
	 * receivedUpto-1 is the last byte position that has already been
	 * received, and receivedTLI is the timeline it came from.  At the first
	 * startup of walreceiver, these are set to receiveStart and
	 * receiveStartTLI. After that, walreceiver updates these whenever it
	 * flushes the received WAL to disk.
	 */
	XLogRecPtr	receivedUpto;
	TimeLineID	receivedTLI;

	/*
	 * latestChunkStart is the starting byte position of the current "batch"
	 * of received WAL.  It's actually the same as the previous value of
	 * receivedUpto before the last flush to disk.  Startup process can use
	 * this to detect whether it's keeping up or not.
	 */
	XLogRecPtr	latestChunkStart;

	/*
	 * Time of send and receive of any message received.
	 */
	TimestampTz lastMsgSendTime;
	TimestampTz lastMsgReceiptTime;

	/*
	 * Latest reported end of WAL on the sender
	 */
	XLogRecPtr	latestWalEnd;
	TimestampTz latestWalEndTime;

	/*
	 * connection string; initially set to connect to the primary, and later
	 * clobbered to hide security-sensitive fields.
	 */
	char		conninfo[MAXCONNINFO];

	/*
	 * replication slot name; is also used for walreceiver to connect with the
	 * primary
	 */
	char		slotname[NAMEDATALEN];

	slock_t		mutex;			/* locks shared variables shown above */

	/*
	 * force walreceiver reply?  This doesn't need to be locked; memory
	 * barriers for ordering are sufficient.
	 */
	bool		force_reply;

	/* set true once conninfo is ready to display (obfuscated pwds etc) */
	bool		ready_to_display;

	/*
	 * Latch used by startup process to wake up walreceiver after telling it
	 * where to start streaming (after setting receiveStart and
	 * receiveStartTLI), and also to tell it to send apply feedback to the
	 * primary whenever specially marked commit records are applied.
	 */
	Latch		latch;
} WalRcvData;

extern WalRcvData *WalRcv;

struct WalReceiverConnHandle;
typedef struct WalReceiverConnHandle WalReceiverConnHandle;

/* libpqwalreceiver hooks */
typedef void (*walrcvconn_connect_fn) (
									WalReceiverConnHandle *handle,
									char *conninfo, bool logical,
									const char *connname);
typedef char *(*walrcvconn_get_conninfo_fn) (WalReceiverConnHandle *handle);
typedef char *(*walrcvconn_identify_system_fn) (WalReceiverConnHandle *handle,
												TimeLineID *primary_tli,
												char **dbname);
typedef void (*walrcvconn_readtimelinehistoryfile_fn) (
									WalReceiverConnHandle *handle,
									TimeLineID tli, char **filename,
									char **content, int *size);
typedef char *(*walrcvconn_create_slot_fn) (
									WalReceiverConnHandle *handle,
									char *slotname, bool logical,
									XLogRecPtr *lsn);
typedef void (*walrcvconn_drop_slot_fn) (
									WalReceiverConnHandle *handle,
									char *slotname);
typedef bool (*walrcvconn_startstreaming_physical_fn) (
									WalReceiverConnHandle *handle,
									TimeLineID tli, XLogRecPtr startpoint,
									char *slotname);
typedef bool (*walrcvconn_startstreaming_logical_fn) (
									WalReceiverConnHandle *handle,
									XLogRecPtr startpoint, char *slotname,
									char *options);
typedef void (*walrcvconn_endstreaming_fn) (WalReceiverConnHandle *handle,
											TimeLineID *next_tli);
typedef int (*walrcvconn_receive_fn) (WalReceiverConnHandle *handle,
									  char **buffer, pgsocket *wait_fd);
typedef void (*walrcvconn_send_fn) (WalReceiverConnHandle *handle,
									const char *buffer, int nbytes);
typedef void (*walrcvconn_disconnect_fn) (WalReceiverConnHandle *handle);

typedef struct WalReceiverConnAPI {
	walrcvconn_connect_fn					connect;
	walrcvconn_get_conninfo_fn				get_conninfo;
	walrcvconn_identify_system_fn			identify_system;
	walrcvconn_readtimelinehistoryfile_fn	readtimelinehistoryfile;
	walrcvconn_create_slot_fn				create_slot;
	walrcvconn_drop_slot_fn					drop_slot;
	walrcvconn_startstreaming_physical_fn	startstreaming_physical;
	walrcvconn_startstreaming_logical_fn	startstreaming_logical;
	walrcvconn_endstreaming_fn				endstreaming;
	walrcvconn_receive_fn					receive;
	walrcvconn_send_fn						send;
	walrcvconn_disconnect_fn				disconnect;
} WalReceiverConnAPI;

typedef WalReceiverConnHandle *(*walrcvconn_init_fn)(WalReceiverConnAPI *wrconn);

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void) pg_attribute_noreturn();
extern Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS);

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvStreaming(void);
extern bool WalRcvRunning(void);
extern void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr,
					 const char *conninfo, const char *slotname);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
extern int	GetReplicationApplyDelay(void);
extern int	GetReplicationTransferLatency(void);
extern void WalRcvForceReply(void);

#endif   /* _WALRECEIVER_H */
