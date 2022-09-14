#if defined(RT_NODE_LEVEL_INNER)
#define RT_NODE4_TYPE RT_NODE_INNER_4
#define RT_NODE32_TYPE RT_NODE_INNER_32
#define RT_NODE125_TYPE RT_NODE_INNER_125
#define RT_NODE256_TYPE RT_NODE_INNER_256
#elif defined(RT_NODE_LEVEL_LEAF)
#define RT_NODE4_TYPE RT_NODE_LEAF_4
#define RT_NODE32_TYPE RT_NODE_LEAF_32
#define RT_NODE125_TYPE RT_NODE_LEAF_125
#define RT_NODE256_TYPE RT_NODE_LEAF_256
#else
#error node level must be either inner or leaf
#endif

	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);

#ifdef RT_NODE_LEVEL_LEAF
	uint64		value = 0;

	Assert(NODE_IS_LEAF(node));
#else
#ifndef RT_ACTION_UPDATE
	RT_PTR_ALLOC child = RT_INVALID_PTR_ALLOC;
#endif
	Assert(!NODE_IS_LEAF(node));
#endif

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE4_TYPE *n4 = (RT_NODE4_TYPE *) node;
				int			idx = RT_NODE_4_SEARCH_EQ((RT_NODE_BASE_4 *) n4, chunk);

#ifdef RT_ACTION_UPDATE
				Assert(idx >= 0);
				n4->children[idx] = new_child;
#else
				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				value = n4->values[idx];
#else
				child = n4->children[idx];
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node;
				int			idx = RT_NODE_32_SEARCH_EQ((RT_NODE_BASE_32 *) n32, chunk);

#ifdef RT_ACTION_UPDATE
				Assert(idx >= 0);
				n32->children[idx] = new_child;
#else
				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				value = n32->values[idx];
#else
				child = n32->children[idx];
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node;
				int			slotpos = n125->base.slot_idxs[chunk];

#ifdef RT_ACTION_UPDATE
				Assert(slotpos != RT_NODE_125_INVALID_IDX);
				n125->children[slotpos] = new_child;
#else
				if (slotpos == RT_NODE_125_INVALID_IDX)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				value = RT_NODE_LEAF_125_GET_VALUE(n125, chunk);
#else
				child = RT_NODE_INNER_125_GET_CHILD(n125, chunk);
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node;

#ifdef RT_ACTION_UPDATE
				RT_NODE_INNER_256_SET(n256, chunk, new_child);
#else
#ifdef RT_NODE_LEVEL_LEAF
				if (!RT_NODE_LEAF_256_IS_CHUNK_USED(n256, chunk))
#else
				if (!RT_NODE_INNER_256_IS_CHUNK_USED(n256, chunk))
#endif
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				value = RT_NODE_LEAF_256_GET_VALUE(n256, chunk);
#else
				child = RT_NODE_INNER_256_GET_CHILD(n256, chunk);
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
	}

#ifdef RT_ACTION_UPDATE
	return;
#else
#ifdef RT_NODE_LEVEL_LEAF
	Assert(value_p != NULL);
	*value_p = value;
#else
	Assert(child_p != NULL);
	*child_p = child;
#endif

	return true;
#endif							/* RT_ACTION_UPDATE */

#undef RT_NODE4_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
