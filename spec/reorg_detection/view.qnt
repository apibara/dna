// -*- mode: Bluespec; -*-

module view {
    import types.* from "./types"

    type View = {
        // Finalized block number.
        finalized: int,
        // Head block number.
        head_: int,
        // Canonical chain.
        canonical: int -> BlockHash,
        // Reorgs by block number.
        reorgs: int -> (BlockHash -> Cursor),
    }

    type Connect =
        | Continue
        | OfflineReorg(Cursor)

    pure def newView(genesis: Block): View = {
        {
            finalized: genesis.number,
            head_: genesis.number,
            canonical: Map(genesis.number -> genesis.hash),
            reorgs: Map(genesis.number -> Map()),
        }
    }

    // Connect to the stream, warning of offline reorgs.
    pure def connect(view: View, cursor: Cursor): Connect = {
        val isCanonical = and {
            view.canonical.keys().contains(cursor.number),
            view.canonical.get(cursor.number) == cursor.hash,
        }

        if (isCanonical) {
            Continue
        } else {
            val reorgs = view.reorgs.get(cursor.number)
            val target = reorgs.get(cursor.hash)

            // Quint has no recursion or loops.
            // So check another time if the target is canonical.
            // In practice we would keep iterating until we find a target
            // that is canonical.
            val isCanonical = and {
                view.canonical.keys().contains(target.number),
                view.canonical.get(target.number) == target.hash,
            }
            if (isCanonical) {
                OfflineReorg(target)
            } else {
                val reorgs_ = view.reorgs.get(target.number)
                val target_ = reorgs_.get(target.hash)
                OfflineReorg(target_)
            }
        }
    }

    pure def canFinalize(view: View, number: int): bool = and {
        view.finalized < number,
        view.head_ >= number,
    }

    pure def finalize(view: View, number: int): View = {
        {
            finalized: number,
            head_: view.head_,
            canonical: mapRemoveRange(view.canonical, view.finalized, number - 1),
            reorgs: mapRemoveRange(view.reorgs, view.finalized, number - 1),
        }
    }

    // Check if the block can be applied to the view.
    pure def canGrow(view: View, block: Block): bool = and {
        view.head_ + 1 == block.number,
        view.canonical.get(view.head_) == block.parent,
    }

    // Add a block to the view.
    pure def grow(view: View, block: Block): View = {
        val newReorgs =
            if (view.reorgs.keys().contains(block.number)) {
                view.reorgs
            } else {
                view.reorgs.put(block.number, Map())
            }
        {
            finalized: view.finalized,
            head_: block.number,
            canonical: view.canonical.put(block.number, block.hash),
            reorgs: newReorgs,
        }
    }

    pure def canShrink(view: View, block: Block): bool = and {
        block.number > view.finalized,
        block.number < view.head_,
        view.canonical.get(block.number) == block.hash,
    }

    pure def shrink(view: View, block: Cursor): View = {
        // Update reorgs by adding the target `block` to the reorg
        // for each block removed.
        val new_reorgs = view.reorgs
            .keys()
            .mapBy(n => {
                if (n <= block.number or n > view.head_) {
                    view.reorgs.get(n)
                } else {
                    val canon = view.canonical.get(n)
                    val reorgs = view.reorgs.get(n)
                    reorgs.put(canon, { number: block.number, hash: block.hash })
                }
            })
        {
            finalized: view.finalized,
            head_: block.number,
            canonical: mapRemoveRange(view.canonical, block.number + 1, view.head_),
            reorgs: new_reorgs,
        }
    }

    pure def blockCount(view: View): int = view.head_ - view.finalized + 1

    pure def viewInvariant(view: View): bool = and {
        // obviously
        view.finalized <= view.head_,
        view.canonical.keys().contains(view.finalized),
        view.canonical.keys().contains(view.head_),
        view.canonical.keys().size() == view.blockCount(),
        // reorgs can have more blocks right after shrinking
        view.canonical.keys().size() <= view.reorgs.keys().size(),
        // no gaps in the canonical chain
        view.canonical.keys().size() >= (view.head_ - view.finalized + 1),
        /*
        view.finalized.to(view.head_).forall(n => and {
            view.canonical.keys().contains(n),
            view.reorgs.keys().contains(n),
        }),
        */
    }
}
