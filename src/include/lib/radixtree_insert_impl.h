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
	bool		chunk_exists = false;
	RT_PTR_LOCAL newnode = NULL;
	RT_PTR_ALLOC allocnode;

#ifdef RT_NODE_LEVEL_LEAF
	const bool inner = false;
	Assert(NODE_IS_LEAF(node));
#else
	const bool inner = true;
	Assert(!NODE_IS_LEAF(node));
#endif

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE4_TYPE *n4 = (RT_NODE4_TYPE *) node;
				int			idx;

				idx = RT_NODE_4_SEARCH_EQ(&n4->base, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
#ifdef RT_NODE_LEVEL_LEAF
					n4->values[idx] = value;
#else
					n4->children[idx] = child;
#endif
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n4)))
				{
					RT_NODE32_TYPE *new32;
					const uint8 new_kind = RT_NODE_KIND_32;
					const RT_SIZE_CLASS new_class = RT_KIND_MIN_SIZE_CLASS[new_kind];

					/* grow node from 4 to 32 */
					allocnode = RT_ALLOC_NODE(tree, new_class, inner);
					newnode = RT_PTR_GET_LOCAL(tree, allocnode);
					RT_INIT_NODE(newnode, new_kind, new_class, inner);
					RT_COPY_NODE(newnode, node);
					//newnode = RT_GROW_NODE_KIND(tree, node, RT_NODE_KIND_32);
					new32 = (RT_NODE32_TYPE *) newnode;
#ifdef RT_NODE_LEVEL_LEAF
					RT_CHUNK_VALUES_ARRAY_COPY(n4->base.chunks, n4->values,
											  new32->base.chunks, new32->values);
#else
					RT_CHUNK_CHILDREN_ARRAY_COPY(n4->base.chunks, n4->children,
											  new32->base.chunks, new32->children);
#endif
					Assert(parent != NULL);
					RT_REPLACE_NODE(tree, parent, nodep, allocnode, key);
					node = newnode;
				}
				else
				{
					int			insertpos = RT_NODE_4_GET_INSERTPOS(&n4->base, chunk);
					int			count = n4->base.n.count;

					/* shift chunks and children */
					if (insertpos < count)
					{
						Assert(count > 0);
#ifdef RT_NODE_LEVEL_LEAF
						RT_CHUNK_VALUES_ARRAY_SHIFT(n4->base.chunks, n4->values,
												   count, insertpos);
#else
						RT_CHUNK_CHILDREN_ARRAY_SHIFT(n4->base.chunks, n4->children,
												   count, insertpos);
#endif
					}

					n4->base.chunks[insertpos] = chunk;
#ifdef RT_NODE_LEVEL_LEAF
					n4->values[insertpos] = value;
#else
					n4->children[insertpos] = child;
#endif
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_32:
			{
				const RT_SIZE_CLASS_ELEM class32_min = RT_SIZE_CLASS_INFO[RT_CLASS_32_PARTIAL];
				const RT_SIZE_CLASS_ELEM class32_max = RT_SIZE_CLASS_INFO[RT_CLASS_32_FULL];
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node;
				int			idx;

				idx = RT_NODE_32_SEARCH_EQ(&n32->base, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
#ifdef RT_NODE_LEVEL_LEAF
					n32->values[idx] = value;
#else
					n32->children[idx] = child;
#endif
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n32)) &&
					n32->base.n.fanout == class32_min.fanout)
				{
					/* grow to the next size class of this kind */
					const RT_SIZE_CLASS new_class = RT_CLASS_32_FULL;

					allocnode = RT_ALLOC_NODE(tree, new_class, inner);
					newnode = RT_PTR_GET_LOCAL(tree, allocnode);
#ifdef RT_NODE_LEVEL_LEAF
					memcpy(newnode, node, class32_min.leaf_size);
#else
					memcpy(newnode, node, class32_min.inner_size);
#endif
					newnode->fanout = class32_max.fanout;

					Assert(parent != NULL);
					RT_REPLACE_NODE(tree, parent, nodep, allocnode, key);
					node = newnode;

					/* also update pointer for this kind */
					n32 = (RT_NODE32_TYPE *) newnode;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n32)))
				{
					RT_NODE125_TYPE *new125;
					const uint8 new_kind = RT_NODE_KIND_125;
					const RT_SIZE_CLASS new_class = RT_KIND_MIN_SIZE_CLASS[new_kind];

					Assert(n32->base.n.fanout == class32_max.fanout);

					/* grow node from 32 to 125 */
					allocnode = RT_ALLOC_NODE(tree, new_class, inner);
					newnode = RT_PTR_GET_LOCAL(tree, allocnode);
					RT_INIT_NODE(newnode, new_kind, new_class, inner);
					RT_COPY_NODE(newnode, node);
					//newnode = RT_GROW_NODE_KIND(tree, node, RT_NODE_KIND_125);
					new125 = (RT_NODE125_TYPE *) newnode;

					for (int i = 0; i < class32_max.fanout; i++)
					{
						new125->base.slot_idxs[n32->base.chunks[i]] = i;
#ifdef RT_NODE_LEVEL_LEAF
						new125->values[i] = n32->values[i];
#else
						new125->children[i] = n32->children[i];
#endif
					}

					Assert(class32_max.fanout <= sizeof(bitmapword) * BITS_PER_BYTE);
					new125->base.isset[0] = (bitmapword) (((uint64) 1 << class32_max.fanout) - 1);

					Assert(parent != NULL);
					RT_REPLACE_NODE(tree, parent, nodep, allocnode, key);
					node = newnode;
				}
				else
				{
					int	insertpos = RT_NODE_32_GET_INSERTPOS(&n32->base, chunk);
					int count = n32->base.n.count;

					if (insertpos < count)
					{
						Assert(count > 0);
#ifdef RT_NODE_LEVEL_LEAF
						RT_CHUNK_VALUES_ARRAY_SHIFT(n32->base.chunks, n32->values,
												   count, insertpos);
#else
						RT_CHUNK_CHILDREN_ARRAY_SHIFT(n32->base.chunks, n32->children,
												   count, insertpos);
#endif
					}

					n32->base.chunks[insertpos] = chunk;
#ifdef RT_NODE_LEVEL_LEAF
					n32->values[insertpos] = value;
#else
					n32->children[insertpos] = child;
#endif
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node;
				int			slotpos = n125->base.slot_idxs[chunk];
				int			cnt = 0;

				if (slotpos != RT_NODE_125_INVALID_IDX)
				{
					/* found the existing chunk */
					chunk_exists = true;
#ifdef RT_NODE_LEVEL_LEAF
					n125->values[slotpos] = value;
#else
					n125->children[slotpos] = child;
#endif
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n125)))
				{
					RT_NODE256_TYPE *new256;
					const uint8 new_kind = RT_NODE_KIND_256;
					const RT_SIZE_CLASS new_class = RT_KIND_MIN_SIZE_CLASS[new_kind];

					/* grow node from 125 to 256 */
					allocnode = RT_ALLOC_NODE(tree, new_class, inner);
					newnode = RT_PTR_GET_LOCAL(tree, allocnode);
					RT_INIT_NODE(newnode, new_kind, new_class, inner);
					RT_COPY_NODE(newnode, node);
					//newnode = RT_GROW_NODE_KIND(tree, node, RT_NODE_KIND_256);
					new256 = (RT_NODE256_TYPE *) newnode;
					for (int i = 0; i < RT_NODE_MAX_SLOTS && cnt < n125->base.n.count; i++)
					{
						if (!RT_NODE_125_IS_CHUNK_USED(&n125->base, i))
							continue;
#ifdef RT_NODE_LEVEL_LEAF
						RT_NODE_LEAF_256_SET(new256, i, RT_NODE_LEAF_125_GET_VALUE(n125, i));
#else
						RT_NODE_INNER_256_SET(new256, i, RT_NODE_INNER_125_GET_CHILD(n125, i));
#endif
						cnt++;
					}

					Assert(parent != NULL);
					RT_REPLACE_NODE(tree, parent, nodep, allocnode, key);
					node = newnode;
				}
				else
				{
					int			idx;
					bitmapword	inverse;

					/* get the first word with at least one bit not set */
					for (idx = 0; idx < BM_IDX(128); idx++)
					{
						if (n125->base.isset[idx] < ~((bitmapword) 0))
							break;
					}

					/* To get the first unset bit in X, get the first set bit in ~X */
					inverse = ~(n125->base.isset[idx]);
					slotpos = idx * BITS_PER_BITMAPWORD;
					slotpos += bmw_rightmost_one_pos(inverse);
					Assert(slotpos < node->fanout);

					/* mark the slot used */
					n125->base.isset[idx] |= bmw_rightmost_one(inverse);
					n125->base.slot_idxs[chunk] = slotpos;

#ifdef RT_NODE_LEVEL_LEAF
					n125->values[slotpos] = value;
#else
					n125->children[slotpos] = child;
#endif
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node;

#ifdef RT_NODE_LEVEL_LEAF
				chunk_exists = RT_NODE_LEAF_256_IS_CHUNK_USED(n256, chunk);
#else
				chunk_exists = RT_NODE_INNER_256_IS_CHUNK_USED(n256, chunk);
#endif
				Assert(chunk_exists || FIXED_NODE_HAS_FREE_SLOT(n256, RT_CLASS_256));

#ifdef RT_NODE_LEVEL_LEAF
				RT_NODE_LEAF_256_SET(n256, chunk, value);
#else
				RT_NODE_INNER_256_SET(n256, chunk, child);
#endif
				break;
			}
	}

	/* Update statistics */
	if (!chunk_exists)
		node->count++;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	RT_VERIFY_NODE(node);

	return chunk_exists;

#undef RT_NODE4_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
