/*-------------------------------------------------------------------------
 *
 * pgoutput.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pgoutput.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"

#include "catalog/catalog.h"
#include "catalog/pg_publication.h"

#include "mb/pg_wchar.h"

#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/origin.h"
#include "replication/pgoutput.h"

#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pgoutput_startup(LogicalDecodingContext * ctx,
							  OutputPluginOptions *opt, bool is_init);
static void pgoutput_shutdown(LogicalDecodingContext * ctx);
static void pgoutput_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pgoutput_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pgoutput_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);
static bool pgoutput_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id);

static bool publications_valid;

static List *LoadPublications(List *pubnames);
static void publication_invalidation_cb(Datum arg, int cacheid,
										uint32 hashvalue);

/* Entry in the map used to remember which relation schemas we sent. */
typedef struct RelationSyncEntry
{
	Oid		relid;			/* relation oid */
	bool	schema_sent;	/* did we send the schema? */
	bool	replicate_valid;
	bool	replicate_insert;
	bool	replicate_update;
	bool	replicate_delete;
} RelationSyncEntry;

/* Map used to remember which relation schemas we sent. */
static HTAB *RelationSyncCache = NULL;

static void init_rel_sync_cache(MemoryContext decoding_context);
static void destroy_rel_sync_cache(void);
static RelationSyncEntry *get_rel_sync_entry(PGOutputData *data, Oid relid);
static void rel_sync_cache_relation_cb(Datum arg, Oid relid);
static void rel_sync_cache_publication_cb(Datum arg, int cacheid,
										  uint32 hashvalue);

/*
 * Specify output plugin callbacks
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pgoutput_startup;
	cb->begin_cb = pgoutput_begin_txn;
	cb->change_cb = pgoutput_change;
	cb->commit_cb = pgoutput_commit_txn;
	cb->filter_by_origin_cb = pgoutput_origin_filter;
	cb->shutdown_cb = pgoutput_shutdown;
}

/*
 * Initialize this plugin
 */
static void
pgoutput_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	PGOutputData   *data = palloc0(sizeof(PGOutputData));
	int				client_encoding;

	/* Create our memory context for private allocations. */
	data->context = AllocSetContextCreate(ctx->context,
										  "logical replication output context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		/* We can only do binary */
		if (opt->output_type != OUTPUT_PLUGIN_BINARY_OUTPUT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("only binary mode is supported for logical replication protocol")));

		/* Parse the params and ERROR if we see any we don't recognise */
		pgoutput_process_parameters(ctx->output_plugin_options, data);

		/* Check if we support requested protol */
		if (data->protocol_version > LOGICALREP_PROTO_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent protocol_version=%d but we only support protocol %d or lower",
					 data->protocol_version, LOGICALREP_PROTO_VERSION_NUM)));

		if (data->protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent protocol_version=%d but we only support protocol %d or higher",
				 	data->protocol_version, LOGICALREP_PROTO_MIN_VERSION_NUM)));

		/* Check for encoding match */
		if (data->client_encoding == NULL ||
			strlen(data->client_encoding) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("encoding parameter missing")));

		client_encoding = pg_char_to_encoding(data->client_encoding);

		if (client_encoding == -1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognised encoding name %s passed to expected_encoding",
							data->client_encoding)));

		if (client_encoding != GetDatabaseEncoding())
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("encoding conversion for logical replication is not supported yet"),
						 errdetail("encoding %s must be unset or match server_encoding %s",
							 data->client_encoding, GetDatabaseEncodingName())));

		if (list_length(data->publication_names) < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("publication_names parameter missing")));

		/* Init publication state. */
		data->publications = NIL;
		publications_valid = false;
		CacheRegisterSyscacheCallback(PUBLICATIONOID,
									  publication_invalidation_cb,
									  (Datum) 0);

		/* Initialize relation schema cache. */
		init_rel_sync_cache(CacheMemoryContext);
	}
}

/*
 * BEGIN callback
 */
static void
pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	bool	send_replication_origin = txn->origin_id != InvalidRepOriginId;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin(ctx->out, txn);

	if (send_replication_origin)
	{
		char *origin;

		/* Message boundary */
		OutputPluginWrite(ctx, false);
		OutputPluginPrepareWrite(ctx, true);

		/*
		 * XXX: which behaviour we want here?
		 *
		 * Alternatives:
		 *  - don't send origin message if origin name not found
		 *    (that's what we do now)
		 *  - throw error - that will break replication, not good
		 *  - send some special "unknown" origin
		 */
		if (replorigin_by_oid(txn->origin_id, true, &origin))
			logicalrep_write_origin(ctx->out, origin, txn->origin_lsn);
	}

	OutputPluginWrite(ctx, true);
}

/*
 * COMMIT callback
 */
static void
pgoutput_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

/*
 * Sends the decoded DML over wire.
 */
static void
pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	PGOutputData	   *data = (PGOutputData *) ctx->output_plugin_private;
	MemoryContext		old;
	RelationSyncEntry  *relentry;

	relentry = get_rel_sync_entry(data, RelationGetRelid(relation));

	/* First check the table filter */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (!relentry->replicate_insert)
				return;
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!relentry->replicate_update)
				return;
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!relentry->replicate_delete)
				return;
			break;
		default:
			Assert(false);
	}

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/*
	 * Write the relation schema if the current schema haven't been sent yet.
	 */
	if (!relentry->schema_sent)
	{
		OutputPluginPrepareWrite(ctx, false);
		logicalrep_write_rel(ctx->out, relation);
		OutputPluginWrite(ctx, false);
		relentry->schema_sent = true;
	}

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			logicalrep_write_insert(ctx->out, relation,
									&change->data.tp.newtuple->tuple);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple oldtuple = change->data.tp.oldtuple ?
					&change->data.tp.oldtuple->tuple : NULL;

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_update(ctx->out, relation, oldtuple,
										&change->data.tp.newtuple->tuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_delete(ctx->out, relation,
										&change->data.tp.oldtuple->tuple);
				OutputPluginWrite(ctx, true);
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
	}

	/* Cleanup */
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

/*
 * Currently we always forward.
 */
static bool
pgoutput_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id)
{
	return false;
}

/*
 * Shutdown the output plugin.
 *
 * Note, we don't need to clean the data->context as it's child context
 * of the ctx->context so it will be cleaned up by logical decoding machinery.
 */
static void
pgoutput_shutdown(LogicalDecodingContext * ctx)
{
	destroy_rel_sync_cache();
}

/*
 * Load publications from the list of publication names.
 */
static List *
LoadPublications(List *pubnames)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach (lc, pubnames)
	{
		char		   *pubname = (char *) lfirst(lc);
		Publication	   *pub = GetPublicationByName(pubname, false);

		result = lappend(result, pub);
	}

	return result;
}

/*
 * Publication cache invalidation callback.
 */
static void
publication_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	publications_valid = false;

	/*
	 * Also invalidate per-relation cache so that next time the filtering
	 * info is checked it will be updated with the new publication
	 * settings.
	 */
	rel_sync_cache_publication_cb(arg, cacheid, hashvalue);
}

/*
 * Initialize the relation schema sync cache for a decoding session.
 *
 * The hash table is destoyed at the end of a decoding session. While
 * relcache invalidations still exist and will still be invoked, they
 * will just see the null hash table global and take no action.
 */
static void
init_rel_sync_cache(MemoryContext cachectx)
{
	HASHCTL	ctl;
	int		hash_flags;
	MemoryContext old_ctxt;

	if (RelationSyncCache != NULL)
		return;

	/* Make a new hash table for the cache */
	hash_flags = HASH_ELEM | HASH_CONTEXT;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RelationSyncEntry);
	ctl.hcxt = cachectx;

	hash_flags |= HASH_BLOBS;

	old_ctxt = MemoryContextSwitchTo(cachectx);
	RelationSyncCache = hash_create("logical replication output relation cache",
									128, &ctl, hash_flags);
	(void) MemoryContextSwitchTo(old_ctxt);

	Assert(RelationSyncCache != NULL);

	CacheRegisterRelcacheCallback(rel_sync_cache_relation_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(PUBLICATIONREL,
								  rel_sync_cache_publication_cb,
								  (Datum) 0);
}

/*
 * Remove all the entries from our relation cache.
 */
static void
destroy_rel_sync_cache(void)
{
	HASH_SEQ_STATUS		status;
	RelationSyncEntry  *entry;

	if (RelationSyncCache == NULL)
		return;

	hash_seq_init(&status, RelationSyncCache);

	while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(RelationSyncCache, (void *) &entry->relid,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}

	RelationSyncCache = NULL;
}

/*
 * Find or create entry in the relation schema cache.
 */
static RelationSyncEntry *
get_rel_sync_entry(PGOutputData *data, Oid relid)
{
	RelationSyncEntry  *entry;
	bool				found;
	MemoryContext		oldctx;

	Assert(RelationSyncCache != NULL);

	/* Find cached function info, creating if not found */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);
	entry = (RelationSyncEntry *) hash_search(RelationSyncCache,
											  (void *) &relid,
											  HASH_ENTER, &found);
	MemoryContextSwitchTo(oldctx);
	Assert(entry != NULL);

	/* Not found means schema wasn't sent */
	if (!found || !entry->replicate_valid)
	{
		List	   *pubids = GetRelationPublications(relid);
		ListCell   *lc;

		/* Reload publications if needed before use. */
		if (!publications_valid)
		{
			oldctx = MemoryContextSwitchTo(CacheMemoryContext);
			if (data->publications)
				list_free_deep(data->publications);

			data->publications = LoadPublications(data->publication_names);
			MemoryContextSwitchTo(oldctx);
			publications_valid = true;
		}

		entry->replicate_insert = entry->replicate_update =
			entry->replicate_delete = false;

		foreach(lc, data->publications)
		{
			Publication *pub = lfirst(lc);

			if (pub->alltables || list_member_oid(pubids, pub->oid))
			{
				if (pub->replicate_insert)
					entry->replicate_insert = true;
				if (pub->replicate_update)
					entry->replicate_update = true;
				if (pub->replicate_delete)
					entry->replicate_delete = true;
			}

			if (entry->replicate_insert && entry->replicate_update &&
				entry->replicate_update)
			break;
		}

		list_free(pubids);

		entry->replicate_valid = true;
	}

	if (!found)
		entry->schema_sent = false;

	return entry;
}

/*
 * Relcache invalidation callback
 */
static void
rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
	RelationSyncEntry  *entry;

	/*
	 * We can get here if the plugin was used in SQL interface as the
	 * RelSchemaSyncCache is detroyed when the decoding finishes, but there
	 * is no way to unregister the relcache invalidation callback.
	 */
	if (RelationSyncCache == NULL)
		return;

	/*
	 * Nobody keeps pointers to entries in this hash table around outside
	 * logical decoding callback calls - but invalidation events can come in
	 * *during* a callback if we access the relcache in the callback. Because
	 * of that we must mark the cache entry as invalid but not remove it from
	 * the hash while it could still be referenced, then prune it at a later
	 * safe point.
	 *
	 * Getting invalidations for relations that aren't in the table is
	 * entirely normal, since there's no way to unregister for an
	 * invalidation event. So we don't care if it's found or not.
	 */
	entry = (RelationSyncEntry *) hash_search(RelationSyncCache, &relid,
											  HASH_FIND, NULL);

	/*
	 * Reset schema sent status as the relation definition may have
	 * changed.
	 */
	if (entry != NULL)
		entry->schema_sent = false;
}

/*
 * Publication relation map syscache invalidation callback
 */
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS		status;
	RelationSyncEntry  *entry;

	/*
	 * We can get here if the plugin was used in SQL interface as the
	 * RelSchemaSyncCache is detroyed when the decoding finishes, but there
	 * is no way to unregister the relcache invalidation callback.
	 */
	if (RelationSyncCache == NULL)
		return;

	/*
	 * There is no way to find which entry in our cache the hash belongs to
	 * so mark the whole cache as invalid.
	 */
	hash_seq_init(&status, RelationSyncCache);
	while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
		entry->replicate_valid = false;
}
