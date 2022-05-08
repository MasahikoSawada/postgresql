/*-------------------------------------------------------------------------
 *
 * ddl_deparse.c
 *	  Functions to convert utility commands to machine-parseable
 *	  representation
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 * This is intended to provide JSON blobs representing DDL commands, which can
 * later be re-processed into plain strings by well-defined sprintf-like
 * expansion.  These JSON objects are intended to allow for machine-editing of
 * the commands, by replacing certain nodes within the objects.
 *
 * Much of the information in the output blob actually comes from system
 * catalogs, not from the command parse node, as it is impossible to reliably
 * construct a fully-specified command (i.e. one not dependent on search_path
 * etc) looking only at the parse node.
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddl_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "tcop/ddl_deparse.h"

#include "access/amapi.h"
#include "access/table.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "lib/ilist.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


/*
 * Before they are turned into JSONB representation, each command is
 * represented as an object tree, using the structs below.
 */
typedef enum
{
	ObjTypeNull,
	ObjTypeBool,
	ObjTypeString,
	ObjTypeArray,
	ObjTypeInteger,
	ObjTypeFloat,
	ObjTypeObject
} ObjType;

typedef struct ObjTree
{
	slist_head	params;
	int			numParams;
} ObjTree;

typedef struct ObjElem
{
	char	   *name;
	ObjType		objtype;

	union
	{
		bool		boolean;
		char	   *string;
		int64		integer;
		float8		flt;
		ObjTree	   *object;
		List	   *array;
	} value;
	slist_node	node;
} ObjElem;

static ObjElem *new_null_object(void);
static ObjElem *new_bool_object(bool value);
static ObjElem *new_string_object(char *value);
static ObjElem *new_object_object(ObjTree *value);
static ObjElem *new_array_object(List *array);
static ObjElem *new_integer_object(int64 value);
static ObjElem *new_float_object(float8 value);
static void append_null_object(ObjTree *tree, char *name);
static void append_bool_object(ObjTree *tree, char *name, bool value);
static void append_string_object(ObjTree *tree, char *name, char *value);
static void append_object_object(ObjTree *tree, char *name, ObjTree *value);
static void append_array_object(ObjTree *tree, char *name, List *array);
static inline void append_premade_object(ObjTree *tree, ObjElem *elem);
static JsonbValue *objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state);
static void format_type_detailed(Oid type_oid, int32 typemod,
					 Oid *nspid, char **typname, char **typemodstr,
					 bool *typarray);
static char *printTypmod(const char *typname, int32 typmod, Oid typmodout);

static char *RelationGetColumnDefault(Relation rel, AttrNumber attno, List *dpcontext);

/*
 * Similar to format_type_internal, except we return each bit of information
 * separately:
 *
 * - nspid is the schema OID.  For certain SQL-standard types which have weird
 *   typmod rules, we return InvalidOid; caller is expected to not schema-
 *   qualify the name nor add quotes to the type name in this case.
 *
 * - typename is set to the type name, without quotes
 *
 * - typmod is set to the typemod, if any, as a string with parens
 *
 * - typarray indicates whether []s must be added
 *
 * We don't try to decode type names to their standard-mandated names, except
 * in the cases of types with unusual typmod rules.
 */
static void
format_type_detailed(Oid type_oid, int32 typemod,
					 Oid *nspid, char **typname, char **typemodstr,
					 bool *typarray)
{
	HeapTuple	tuple;
	Form_pg_type typeform;
	Oid			array_base_type;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", type_oid);

	typeform = (Form_pg_type) GETSTRUCT(tuple);

	/*
	 * Special-case crock for types with strange typmod rules.
	 */
	if (type_oid == INTERVALOID ||
		type_oid == TIMESTAMPOID ||
		type_oid == TIMESTAMPTZOID ||
		type_oid == TIMEOID ||
		type_oid == TIMETZOID)
	{
		*typarray = false;

peculiar_typmod:
		switch (type_oid)
		{
			case INTERVALOID:
				*typname = pstrdup("INTERVAL");
				break;
			case TIMESTAMPTZOID:
				if (typemod < 0)
					*typname = pstrdup("TIMESTAMP WITH TIME ZONE");
				else
					*typname = pstrdup("TIMESTAMP");
				break;
				/* otherwise, WITH TZ is added by typmod, so fall through */
			case TIMESTAMPOID:
				*typname = pstrdup("TIMESTAMP");
				break;
			case TIMETZOID:
				if (typemod < 0)
					*typname = pstrdup("TIME WITH TIME ZONE");
				else
					*typname = pstrdup("TIME");
				break;
				/* otherwise, WITH TZ is added by typmode, so fall through */
			case TIMEOID:
				*typname = pstrdup("TIME");
				break;
		}
		*nspid = InvalidOid;

		if (typemod >= 0)
			*typemodstr = printTypmod("", typemod, typeform->typmodout);
		else
			*typemodstr = pstrdup("");

		ReleaseSysCache(tuple);
		return;
	}

	/*
	 * Check if it's a regular (variable length) array type.  As above,
	 * fixed-length array types such as "name" shouldn't get deconstructed.
	 */
	array_base_type = typeform->typelem;

	if (array_base_type != InvalidOid &&
		typeform->typstorage != 'p')
	{
		/* Switch our attention to the array element type */
		ReleaseSysCache(tuple);
		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", type_oid);

		typeform = (Form_pg_type) GETSTRUCT(tuple);
		type_oid = array_base_type;
		*typarray = true;

		/*
		 * If it's an array of one of the types with special typmod rules,
		 * have the element type be processed as above, but now with typarray
		 * set to true.
		 */
		if (type_oid == INTERVALOID ||
			type_oid == TIMESTAMPTZOID ||
			type_oid == TIMESTAMPOID ||
			type_oid == TIMETZOID ||
			type_oid == TIMEOID)
			goto peculiar_typmod;
	}
	else
		*typarray = false;

	*nspid = typeform->typnamespace;
	*typname = pstrdup(NameStr(typeform->typname));

	if (typemod >= 0)
		*typemodstr = printTypmod("", typemod, typeform->typmodout);
	else
		*typemodstr = pstrdup("");

	ReleaseSysCache(tuple);
}

/*
 * Add typmod decoration to the basic type name
 */
static char *
printTypmod(const char *typname, int32 typmod, Oid typmodout)
{
	char	   *res;

	/* Shouldn't be called if typmod is -1 */
	Assert(typmod >= 0);

	if (typmodout == InvalidOid)
	{
		/* Default behavior: just print the integer typmod with parens */
		res = psprintf("%s(%d)", typname, (int) typmod);
	}
	else
	{
		/* Use the type-specific typmodout procedure */
		char	   *tmstr;

		tmstr = DatumGetCString(OidFunctionCall1(typmodout,
												 Int32GetDatum(typmod)));
		res = psprintf("%s%s", typname, tmstr);
	}

	return res;
}

/*
 * Obtain the deparsed default value for the given column of the given table.
 *
 * Caller must have set a correct deparse context.
 */
static char *
RelationGetColumnDefault(Relation rel, AttrNumber attno, List *dpcontext)
{
	Node *defval;
	char *defstr;

	defval = build_column_default(rel, attno);
	defstr = deparse_expression(defval, dpcontext, false, false);

	return defstr;
}

/*
 * Allocate a new object tree to store parameter values.
 */
static ObjTree *
new_objtree(void)
{
	ObjTree    *params;

	params = palloc(sizeof(ObjTree));
	params->numParams = 0;
	slist_init(&params->params);

	return params;
}

/*
 * Allocate a new object tree to store parameter values -- varargs version.
 *
 * The "fmt" argument is used to append as a "fmt" element in the output blob.
 * numobjs indicates the number of extra elements to append; for each one, a
 * name (string), type (from the ObjType enum) and value must be supplied.  The
 * value must match the type given; for instance, ObjTypeInteger requires an
 * int64, ObjTypeString requires a char *, ObjTypeArray requires a list (of
 * ObjElem), ObjTypeObject requires an ObjTree, and so on.  Each element type *
 * must match the conversion specifier given in the format string, as described
 * in ddl_deparse_expand_command, q.v.
 *
 * Note we don't have the luxury of sprintf-like compiler warnings for
 * malformed argument lists.
 */
static ObjTree *
new_objtree_VA(char *fmt, int numobjs,...)
{
	ObjTree    *tree;
	va_list		args;
	int			i;

	/* Set up the toplevel object and its "fmt" */
	tree = new_objtree();
	append_string_object(tree, "fmt", fmt);

	/* And process the given varargs */
	va_start(args, numobjs);
	for (i = 0; i < numobjs; i++)
	{
		char	   *name;
		ObjType		type;
		ObjElem	   *elem;

		name = va_arg(args, char *);
		type = va_arg(args, ObjType);

		/*
		 * For all other param types there must be a value in the varargs.
		 * Fetch it and add the fully formed subobject into the main object.
		 */
		switch (type)
		{
			case ObjTypeBool:
				elem = new_bool_object(va_arg(args, int));
				break;
			case ObjTypeString:
				elem = new_string_object(va_arg(args, char *));
				break;
			case ObjTypeObject:
				elem = new_object_object(va_arg(args, ObjTree *));
				break;
			case ObjTypeArray:
				elem = new_array_object(va_arg(args, List *));
				break;
			case ObjTypeInteger:
				elem = new_integer_object(va_arg(args, int64));
				break;
			case ObjTypeFloat:
				elem = new_float_object(va_arg(args, double));
				break;
			case ObjTypeNull:
				/* Null params don't have a value (obviously) */
				elem = new_null_object();
				break;
			default:
				elog(ERROR, "invalid ObjTree element type %d", type);
		}

		elem->name = name;
		append_premade_object(tree, elem);
	}

	va_end(args);
	return tree;
}

/* Allocate a new parameter with a NULL value */
static ObjElem *
new_null_object(void)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));

	param->name = NULL;
	param->objtype = ObjTypeNull;

	return param;
}

/* Append a NULL object to a tree */
static void
append_null_object(ObjTree *tree, char *name)
{
	ObjElem    *param;

	param = new_null_object();
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new boolean parameter */
static ObjElem *
new_bool_object(bool value)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeBool;
	param->value.boolean = value;

	return param;
}

/* Append a boolean parameter to a tree */
static void
append_bool_object(ObjTree *tree, char *name, bool value)
{
	ObjElem    *param;

	param = new_bool_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new string object */
static ObjElem *
new_string_object(char *value)
{
	ObjElem    *param;

	Assert(value);

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeString;
	param->value.string = value;

	return param;
}

/*
 * Append a string parameter to a tree.
 */
static void
append_string_object(ObjTree *tree, char *name, char *value)
{
	ObjElem	   *param;

	Assert(name);
	param = new_string_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

static ObjElem *
new_integer_object(int64 value)
{
	ObjElem	   *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeInteger;
	param->value.integer = value;

	return param;
}

static ObjElem *
new_float_object(float8 value)
{
	ObjElem	   *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeFloat;
	param->value.flt = value;

	return param;
}

/* Allocate a new object parameter */
static ObjElem *
new_object_object(ObjTree *value)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeObject;
	param->value.object = value;

	return param;
}

/* Append an object parameter to a tree */
static void
append_object_object(ObjTree *tree, char *name, ObjTree *value)
{
	ObjElem    *param;

	Assert(name);
	param = new_object_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new array parameter */
static ObjElem *
new_array_object(List *array)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeArray;
	param->value.array = array;

	return param;
}

/* Append an array parameter to a tree */
static void
append_array_object(ObjTree *tree, char *name, List *array)
{
	ObjElem    *param;

	param = new_array_object(array);
	param->name = name;
	append_premade_object(tree, param);
}

/* Append a preallocated parameter to a tree */
static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
	slist_push_head(&tree->params, &elem->node);
	tree->numParams++;
}

/*
 * Helper for objtree_to_jsonb: process an individual element from an object or
 * an array into the output parse state.
 */
static void
objtree_to_jsonb_element(JsonbParseState *state, ObjElem *object,
						 JsonbIteratorToken elem_token)
{
	ListCell   *cell;
	JsonbValue	val;

	switch (object->objtype)
	{
		case ObjTypeNull:
			val.type = jbvNull;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeString:
			val.type = jbvString;
			val.val.string.len = strlen(object->value.string);
			val.val.string.val = object->value.string;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeInteger:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeFloat:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(float8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeBool:
			val.type = jbvBool;
			val.val.boolean = object->value.boolean;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeObject:
			/* recursively add the object into the existing parse state */
			objtree_to_jsonb_rec(object->value.object, state);
			break;

		case ObjTypeArray:
			pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
			foreach(cell, object->value.array)
			{
				ObjElem   *elem = lfirst(cell);

				objtree_to_jsonb_element(state, elem, WJB_ELEM);
			}
			pushJsonbValue(&state, WJB_END_ARRAY, NULL);
			break;

		default:
			elog(ERROR, "unrecognized object type %d", object->objtype);
			break;
	}
}

/*
 * Recursive helper for objtree_to_jsonb
 */
static JsonbValue *
objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state)
{
	slist_iter	iter;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	slist_foreach(iter, &tree->params)
	{
		ObjElem    *object = slist_container(ObjElem, node, iter.cur);
		JsonbValue	key;

		/* Push the key first */
		key.type = jbvString;
		key.val.string.len = strlen(object->name);
		key.val.string.val = object->name;
		pushJsonbValue(&state, WJB_KEY, &key);

		/* Then process the value according to its type */
		objtree_to_jsonb_element(state, object, WJB_VALUE);
	}

	return pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Create a JSONB representation from an ObjTree.
 */
static Jsonb *
objtree_to_jsonb(ObjTree *tree)
{
	JsonbValue *value;

	value = objtree_to_jsonb_rec(tree, NULL);
	return JsonbValueToJsonb(value);
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(Oid typeId, int32 typmod)
{
	ObjTree    *typeParam;
	Oid			typnspid;
	char	   *typnsp;
	char	   *typename = NULL;
	char	   *typmodstr;
	bool		typarray;

	format_type_detailed(typeId, typmod,
						 &typnspid, &typename, &typmodstr, &typarray);

	if (!OidIsValid(typnspid))
		typnsp = pstrdup("");
	else if (isAnyTempNamespace(typnspid))
		typnsp = pstrdup("pg_temp");
	else
		typnsp = get_namespace_name(typnspid);

	/* We don't use new_objtree_VA here because types don't have a "fmt" */
	typeParam = new_objtree();
	append_string_object(typeParam, "schemaname", typnsp);
	append_string_object(typeParam, "typename", typename);
	append_string_object(typeParam, "typmod", typmodstr);
	append_bool_object(typeParam, "typarray", typarray);

	return typeParam;
}

/*
 * A helper routine to setup %{}D and %{}O elements
 *
 * Elements "schemaname" and "objname" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the objname will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static ObjTree *
new_objtree_for_qualname(Oid nspid, char *name)
{
	ObjTree    *qualified;
	char	   *namespace;

	/*
	 * We don't use new_objtree_VA here because these names don't have a "fmt"
	 */
	qualified = new_objtree();
	if (isAnyTempNamespace(nspid))
		namespace = pstrdup("pg_temp");
	else
		namespace = get_namespace_name(nspid);
	append_string_object(qualified, "schemaname", namespace);
	append_string_object(qualified, "objname", pstrdup(name));

	return qualified;
}

/*
 * A helper routine to setup %{}D and %{}O elements, with the object specified
 * by classId/objId
 *
 * Elements "schemaname" and "objname" are set.  If the object is a temporary
 * object, the schema name is set to "pg_temp".
 */
static ObjTree *
new_objtree_for_qualname_id(Oid classId, Oid objectId)
{
	ObjTree    *qualified;
	Relation	catalog;
	HeapTuple	catobj;
	Datum		objnsp;
	Datum		objname;
	AttrNumber	Anum_name;
	AttrNumber	Anum_namespace;
	AttrNumber	Anum_oid = get_object_attnum_oid(classId);
	bool		isnull;

	catalog = table_open(classId, AccessShareLock);

	catobj = get_catalog_object_by_oid(catalog, Anum_oid, objectId);
	if (!catobj)
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(catalog));
	Anum_name = get_object_attnum_name(classId);
	Anum_namespace = get_object_attnum_namespace(classId);

	objnsp = heap_getattr(catobj, Anum_namespace, RelationGetDescr(catalog),
						  &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL namespace");
	objname = heap_getattr(catobj, Anum_name, RelationGetDescr(catalog),
						   &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL name");

	qualified = new_objtree_for_qualname(DatumGetObjectId(objnsp),
										 NameStr(*DatumGetName(objname)));
	table_close(catalog, AccessShareLock);

	return qualified;
}

/*
 * Return the string representation of the given RELPERSISTENCE value
 */
static char *
get_persistence_str(char persistence)
{
	switch (persistence)
	{
		case RELPERSISTENCE_TEMP:
			return "TEMPORARY";
		case RELPERSISTENCE_UNLOGGED:
			return "UNLOGGED";
		case RELPERSISTENCE_PERMANENT:
			return "";
		default:
			elog(ERROR, "unexpected persistence marking %c", persistence);
			return "";		/* make compiler happy */
	}
}

/*
 * deparse_ColumnDef
 *		Subroutine for CREATE TABLE deparsing
 *
 * Deparse a ColumnDef node within a regular (non typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway.)
 */
static ObjTree *
deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
				  ColumnDef *coldef, bool is_alter)
{
	ObjTree    *column;
	ObjTree    *tmp;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	/*
	 * Inherited columns without local definitions must not be emitted. XXX --
	 * maybe it is useful to have them with "present = false" or some such?
	 */
	if (!coldef->is_local)
		return NULL;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/* Composite types use a slightly simpler format string */
	if (composite)
		column = new_objtree_VA("%{name}I %{coltype}T %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(typid, typmod));
	else
		column = new_objtree_VA("%{name}I %{coltype}T %{default}s %{not_null}s %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(typid, typmod));

	tmp = new_objtree_VA("COLLATE %{name}D", 0);
	if (OidIsValid(typcollation))
	{
		ObjTree *collname;

		collname = new_objtree_for_qualname_id(CollationRelationId,
											   typcollation);
		append_object_object(tmp, "name", collname);
	}
	else
		append_bool_object(tmp, "present", false);
	append_object_object(column, "collation", tmp);

	if (!composite)
	{
		/*
		 * Emit a NOT NULL declaration if necessary.  Note that we cannot trust
		 * pg_attribute.attnotnull here, because that bit is also set when
		 * primary keys are specified; and we must not emit a NOT NULL
		 * constraint in that case, unless explicitely specified.  Therefore,
		 * we scan the list of constraints attached to this column to determine
		 * whether we need to emit anything.
		 * (Fortunately, NOT NULL constraints cannot be table constraints.)
		 *
		 * In the ALTER TABLE cases, we also add a NOT NULL if the colDef is
		 * marked is_not_null.
		 */
		saw_notnull = false;
		foreach(cell, coldef->constraints)
		{
			Constraint *constr = (Constraint *) lfirst(cell);

			if (constr->contype == CONSTR_NOTNULL)
				saw_notnull = true;
		}
		if (is_alter && coldef->is_not_null)
			saw_notnull = true;

		if (saw_notnull)
			append_string_object(column, "not_null", "NOT NULL");
		else
			append_string_object(column, "not_null", "");

		tmp = new_objtree_VA("DEFAULT %{default}s", 0);
		if (attrForm->atthasdef)
		{
			char *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext);

			append_string_object(tmp, "default", defstr);
		}
		else
			append_bool_object(tmp, "present", false);
		append_object_object(column, "default", tmp);
	}

	ReleaseSysCache(attrTup);

	return column;
}

/*
 * deparse_ColumnDef_Typed
 *		Subroutine for CREATE TABLE OF deparsing
 *
 * Deparse a ColumnDef node within a typed table creation.	This is simpler
 * than the regular case, because we don't have to emit the type declaration,
 * collation, or default.  Here we only return something if the column is being
 * declared NOT NULL.
 *
 * As in deparse_ColumnDef, any other constraint is processed elsewhere.
 *
 * FIXME --- actually, what about default values?
 */
static ObjTree *
deparse_ColumnDef_typed(Relation relation, List *dpcontext, ColumnDef *coldef)
{
	ObjTree    *column = NULL;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/*
	 * Search for a NOT NULL declaration.  As in deparse_ColumnDef, we rely on
	 * finding a constraint on the column rather than coldef->is_not_null.
	 * (This routine is never used for ALTER cases.)
	 */
	saw_notnull = false;
	foreach(cell, coldef->constraints)
	{
		Constraint *constr = (Constraint *) lfirst(cell);

		if (constr->contype == CONSTR_NOTNULL)
		{
			saw_notnull = true;
			break;
		}
	}

	if (saw_notnull)
		column = new_objtree_VA("%{name}I WITH OPTIONS NOT NULL", 2,
								"type", ObjTypeString, "column_notnull",
								"name", ObjTypeString, coldef->colname);

	ReleaseSysCache(attrTup);

	return column;
}

/*
 * deparseTableElements
 *		Subroutine for CREATE TABLE deparsing
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static List *
deparseTableElements(Relation relation, List *tableElements, List *dpcontext,
					 bool typed, bool composite)
{
	List	   *elements = NIL;
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node	   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ObjTree	   *tree;

					tree = typed ?
						deparse_ColumnDef_typed(relation, dpcontext,
												(ColumnDef *) elt) :
						deparse_ColumnDef(relation, dpcontext,
										  composite, (ColumnDef *) elt,
										  false);
					if (tree != NULL)
					{
						ObjElem    *column;

						column = new_object_object(tree);
						elements = lappend(elements, column);
					}
				}
				break;
			case T_Constraint:
				break;
			default:
				elog(ERROR, "invalid node type %d", nodeTag(elt));
		}
	}

	return elements;
}

/*
 * obtainConstraints
 *		Subroutine for CREATE TABLE/CREATE DOMAIN deparsing
 *
 * Given a table OID or domain OID, obtain its constraints and append them to
 * the given elements list.  The updated list is returned.
 *
 * This works for typed tables, regular tables, and domains.
 *
 * Note that CONSTRAINT_FOREIGN constraints are always ignored.
 */
static List *
obtainConstraints(List *elements, Oid relationId, Oid domainId)
{
	Relation	conRel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	ObjTree    *tmp;

	/* only one may be valid */
	Assert(OidIsValid(relationId) ^ OidIsValid(domainId));

	/*
	 * scan pg_constraint to fetch all constraints linked to the given
	 * relation.
	 */
	conRel = table_open(ConstraintRelationId, AccessShareLock);
	if (OidIsValid(relationId))
	{
		ScanKeyInit(&key,
					Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relationId));
		scan = systable_beginscan(conRel, ConstraintRelidTypidNameIndexId,
								  true, NULL, 1, &key);
	}
	else
	{
		Assert(OidIsValid(domainId));
		ScanKeyInit(&key,
					Anum_pg_constraint_contypid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(domainId));
		scan = systable_beginscan(conRel, ConstraintTypidIndexId,
								  true, NULL, 1, &key);
	}

	/*
	 * For each constraint, add a node to the list of table elements.  In
	 * these nodes we include not only the printable information ("fmt"), but
	 * also separate attributes to indicate the type of constraint, for
	 * automatic processing.
	 */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint constrForm;
		char	   *contype;

		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		switch (constrForm->contype)
		{
			case CONSTRAINT_CHECK:
				contype = "check";
				break;
			case CONSTRAINT_FOREIGN:
				continue;	/* not here */
			case CONSTRAINT_PRIMARY:
				contype = "primary key";
				break;
			case CONSTRAINT_UNIQUE:
				contype = "unique";
				break;
			case CONSTRAINT_TRIGGER:
				contype = "trigger";
				break;
			case CONSTRAINT_EXCLUSION:
				contype = "exclusion";
				break;
			default:
				elog(ERROR, "unrecognized constraint type");
		}

		/*
		 * "type" and "contype" are not part of the printable output, but are
		 * useful to programmatically distinguish these from columns and among
		 * different constraint types.
		 *
		 * XXX it might be useful to also list the column names in a PK, etc.
		 */
		tmp = new_objtree_VA("CONSTRAINT %{name}I %{definition}s",
							 4,
							 "type", ObjTypeString, "constraint",
							 "contype", ObjTypeString, contype,
						 "name", ObjTypeString, NameStr(constrForm->conname),
							 "definition", ObjTypeString,
						  pg_get_constraintdef_command(constrForm->oid));
		elements = lappend(elements, new_object_object(tmp));
	}

	systable_endscan(scan);
	table_close(conRel, AccessShareLock);

	return elements;
}

/*
 * deparse the ON COMMMIT ... clause for CREATE ... TEMPORARY ...
 */
static ObjTree *
deparse_OnCommitClause(OnCommitAction option)
{
	ObjTree	   *tmp;

	tmp = new_objtree_VA("ON COMMIT %{on_commit_value}s", 0);
	switch (option)
	{
		case ONCOMMIT_DROP:
			append_string_object(tmp, "on_commit_value", "DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			append_string_object(tmp, "on_commit_value", "DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			append_string_object(tmp, "on_commit_value", "PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			append_null_object(tmp, "on_commit_value");
			append_bool_object(tmp, "present", false);
			break;
	}

	return tmp;
}

/*
 * Deparse DefElems, as used e.g. by ALTER COLUMN ... SET, into a list of SET
 * (...)  or RESET (...) contents.
 */
static ObjTree *
deparse_DefElem(DefElem *elem, bool is_reset)
{
	ObjTree	   *set;
	ObjTree	   *optname;

	if (elem->defnamespace != NULL)
		optname = new_objtree_VA("%{schema}I.%{label}I", 1,
								 "schema", ObjTypeString, elem->defnamespace);
	else
		optname = new_objtree_VA("%{label}I", 0);

	append_string_object(optname, "label", elem->defname);

	if (is_reset)
		set = new_objtree_VA("%{label}s", 0);
	else
		set = new_objtree_VA("%{label}s = %{value}L", 1,
							 "value", ObjTypeString,
							 elem->arg ? defGetString(elem) :
							 defGetBoolean(elem) ? "TRUE" : "FALSE");

	append_object_object(set, "label", optname);
	return set;
}

/*
 * deparse_CreateStmt
 *		Deparse a CreateStmt (CREATE TABLE)
 *
 * Given a table OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree    *createStmt;
	ObjTree    *tmp;
	List	   *list;
	ListCell   *cell;
	char	   *fmtstr;

	/*
	 * Typed tables use a slightly different format string: we must not put
	 * table_elements with parents directly in the fmt string, because if
	 * there are no options the parens must not be emitted; and also, typed
	 * tables do not allow for inheritance.
	 */
	if (node->ofTypename)
		fmtstr = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D "
			"OF %{of_type}T %{table_elements}s "
			"%{with_clause}s %{on_commit}s %{tablespace}s";
	else
		fmtstr = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D "
			"(%{table_elements:, }s) %{inherits}s "
			"%{with_clause}s %{on_commit}s %{tablespace}s";

	createStmt =
		new_objtree_VA(fmtstr, 1,
					   "persistence", ObjTypeString,
					   get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createStmt, "identity", tmp);

	append_string_object(createStmt, "if_not_exists",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
									objectId);

	if (node->ofTypename)
	{
		List	   *tableelts = NIL;

		/*
		 * We can't put table elements directly in the fmt string as an array
		 * surrounded by parens here, because an empty clause would cause a
		 * syntax error.  Therefore, we use an indirection element and set
		 * present=false when there are no elements.
		 */
		append_string_object(createStmt, "table_kind", "typed");

		tmp = new_objtree_for_type(relation->rd_rel->reloftype, -1);
		append_object_object(createStmt, "of_type", tmp);

		tableelts = deparseTableElements(relation, node->tableElts, dpcontext,
										 true,		/* typed table */
										 false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);
		if (tableelts == NIL)
			tmp = new_objtree_VA("", 1,
								 "present", ObjTypeBool, false);
		else
			tmp = new_objtree_VA("(%{elements:, }s)", 1,
								 "elements", ObjTypeArray, tableelts);
		append_object_object(createStmt, "table_elements", tmp);
	}
	else
	{
		List	   *tableelts = NIL;

		/*
		 * There is no need to process LIKE clauses separately; they have
		 * already been transformed into columns and constraints.
		 */
		append_string_object(createStmt, "table_kind", "plain");

		/*
		 * Process table elements: column definitions and constraints.	Only
		 * the column definitions are obtained from the parse node itself.	To
		 * get constraints we rely on pg_constraint, because the parse node
		 * might be missing some things such as the name of the constraints.
		 */
		tableelts = deparseTableElements(relation, node->tableElts, dpcontext,
										 false,		/* not typed table */
										 false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		append_array_object(createStmt, "table_elements", tableelts);

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		tmp = new_objtree_VA("INHERITS (%{parents:, }D)", 0);
		if (list_length(node->inhRelations) > 0)
		{
			List	   *parents = NIL;
			Relation	inhRel;
			SysScanDesc scan;
			ScanKeyData key;
			HeapTuple	tuple;

			inhRel = table_open(InheritsRelationId, RowExclusiveLock);

			ScanKeyInit(&key,
						Anum_pg_inherits_inhrelid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(objectId));

			scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId,
									  true, NULL, 1, &key);

			while (HeapTupleIsValid(tuple = systable_getnext(scan)))
			{
				ObjTree    *parent;
				Form_pg_inherits formInh = (Form_pg_inherits) GETSTRUCT(tuple);

				parent = new_objtree_for_qualname_id(RelationRelationId,
													 formInh->inhparent);
				parents = lappend(parents, new_object_object(parent));
			}

			systable_endscan(scan);
			table_close(inhRel, RowExclusiveLock);

			append_array_object(tmp, "parents", parents);
		}
		else
		{
			append_null_object(tmp, "parents");
			append_bool_object(tmp, "present", false);
		}
		append_object_object(createStmt, "inherits", tmp);
	}

	tmp = new_objtree_VA("TABLESPACE %{tablespace}I", 0);
	if (node->tablespacename)
		append_string_object(tmp, "tablespace", node->tablespacename);
	else
	{
		append_null_object(tmp, "tablespace");
		append_bool_object(tmp, "present", false);
	}
	append_object_object(createStmt, "tablespace", tmp);

	append_object_object(createStmt, "on_commit",
						 deparse_OnCommitClause(node->oncommit));

	/* WITH clause */
	tmp = new_objtree_VA("WITH (%{with:, }s)", 0);
	list = NIL;

	foreach(cell, node->options)
	{
		ObjTree	   *tmp2;
		DefElem	*opt = (DefElem *) lfirst(cell);

		tmp2 = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmp2));
	}

	if (list)
		append_array_object(tmp, "with", list);
	else
		append_bool_object(tmp, "present", false);

	append_object_object(createStmt, "with_clause", tmp);

	relation_close(relation, AccessShareLock);

	return createStmt;
}

/*
 * Handle deparsing of simple commands.
 *
 * This function should cover all cases handled in ProcessUtilitySlow.
 */
static ObjTree *
deparse_simple_command(CollectedCommand *cmd)
{
	Oid			objectId;
	Node	   *parsetree;
	ObjTree	   *command;

	Assert(cmd->type == SCT_Simple);

	parsetree = cmd->parsetree;
	objectId = cmd->d.simple.address.objectId;

	/* This switch needs to handle everything that ProcessUtilitySlow does */
	switch (nodeTag(parsetree))
	{
		case T_CreateStmt:
			command = deparse_CreateStmt(objectId, parsetree);
			break;

		default:
			command = NULL;
			elog(LOG, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
	}

	return command;
}

char *
deparse_utility_command(CollectedCommand *cmd)
{
	OverrideSearchPath *overridePath;
	MemoryContext	oldcxt;
	MemoryContext	tmpcxt;
	ObjTree		   *tree;
	char		   *command;
	StringInfoData  str;

	/*
	 * Allocate everything done by the deparsing routines into a temp context,
	 * to avoid having to sprinkle them with memory handling code; but allocate
	 * the output StringInfo before switching.
	 */
	initStringInfo(&str);
	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "deparse ctx",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/*
	 * Many routines underlying this one will invoke ruleutils.c functionality
	 * in order to obtain deparsed versions of expressions.  In such results,
	 * we want all object names to be qualified, so that results are "portable"
	 * to environments with different search_path settings.  Rather than inject
	 * what would be repetitive calls to override search path all over the
	 * place, we do it centrally here.
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = false;
	overridePath->addTemp = true;
	PushOverrideSearchPath(overridePath);

	switch (cmd->type)
	{
		case SCT_Simple:
			tree = deparse_simple_command(cmd);
			break;
		default:
			elog(ERROR, "unexpected deparse node type %d", cmd->type);
	}

	PopOverrideSearchPath();

	if (tree)
	{
		Jsonb *jsonb;

		jsonb = objtree_to_jsonb(tree);
		command = JsonbToCString(&str, &jsonb->root, 128);
	}
	else
		command = NULL;

	/*
	 * Clean up.  Note that since we created the StringInfo in the caller's
	 * context, the output string is not deleted here.
	 */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	return command;
}

/*
 * Given a CollectedCommand, return a JSON representation of it.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 */
Datum
ddl_deparse_to_json(PG_FUNCTION_ARGS)
{
	CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
	char		   *command;

	command = deparse_utility_command(cmd);

	if (command)
		PG_RETURN_TEXT_P(CStringGetTextDatum(command));
	else
		PG_RETURN_NULL();
}
