// -*- mode: Bluespec; -*-

module chain {
    import types.* from "./types"

    type Chain = {
        // Used to generate unique block hashes.
        block_hash_gen: BlockHash,
        // The head of the chain.
        head_: int,
        // The finalized block.
        finalized: int,
        // Canonical chain. Map block height to block hash.
        canonical: int -> BlockHash,
        // Map block hash to parent block hash.
        parent: BlockHash -> BlockHash,
        // Map block hash to block number.
        block_number: BlockHash -> int,
        // Number of reorgs.
        reorg_count: int,
    }

    /*********************************************************
     *
     * JSON-RPC METHODS
     *
     *********************************************************/

    // Get the block at the given height.
    //
    // Returns `None` if the height is not in the chain.
    pure def getBlockByNumber(chain: Chain, i: int): Option[Block] = {
        if (chain.canonical.keys().contains(i)) {
            val hash = chain.canonical.get(i)
            val parent = chain.parent.get(hash)
            Some({ number: i, hash: hash, parent: parent })
        } else {
            None
        }
    }

    // Get the block at the given hash.
    //
    // Assumes that the block existed at any point in time.
    pure def getBlockByHash(chain: Chain, hash: BlockHash): Block = {
        val number = chain.block_number.get(hash)
        { number: number, hash: hash, parent: chain.parent.get(hash) }
    }

    // Get the head of the chain.
    pure def getHead(chain: Chain): Block = {
        val hash = chain.canonical.get(chain.head_)
        val parent = chain.parent.get(hash)
        { number: chain.head_, hash: hash, parent: parent }
    }

    // Get the finalized block.
    pure def getFinalized(chain: Chain): Block = {
        val hash = chain.canonical.get(chain.finalized)
        val parent = chain.parent.get(hash)
        { number: chain.finalized, hash: hash, parent: parent }
    }

    /*********************************************************
     *
     * CHAIN CONTROL API
     *
     *********************************************************/

    pure def newChain(genesis: Block): Chain = {
        {
            block_hash_gen: genesis.hash,
            head_: 0,
            finalized: 0,
            canonical: Map(0 -> genesis.hash),
            parent: Map(genesis.hash -> genesis.parent),
            block_number: Map(genesis.hash -> genesis.number),
            reorg_count: 0,
        }
    }

    // Advance the finalized block by `jump` blocks.
    pure def advanceFinalizedBy(chain: Chain, jump: int): Chain = {
        val max_finalized_jump = chain.head_ - chain.finalized
        val actual_jump = if (jump > max_finalized_jump) {
            max_finalized_jump
        } else {
            jump
        }
        {
            finalized: chain.finalized + actual_jump,
            ...chain,
        }
    }

    pure def maxFinalizedJump(chain: Chain): int = chain.head_ - chain.finalized

    // Advance the head of the chain by one block.
    pure def advanceHead(chain: Chain): Chain = {
        val number = chain.head_ + 1
        val hash = chain.block_hash_gen + 1
        val current_hash = chain.canonical.get(chain.head_)
        {
            block_hash_gen: hash,
            head_: number,
            finalized: chain.finalized,
            canonical: chain.canonical.put(number, hash),
            parent: chain.parent.put(hash, current_hash),
            block_number: chain.block_number.put(hash, number),
            reorg_count: chain.reorg_count,
        }
    }

    // Returns `true` if the chain can be reorged to the given height.
    pure def canReorg(chain: Chain, new_head: int): bool = and {
        chain.head_ >= new_head,
        chain.finalized < new_head,
    }

    // Shrink the chain to the given height.
    pure def reorg(chain: Chain, new_head: int): Chain = {
        val new_canonical = chain.canonical
            .keys()
            .filter(i => i <= new_head)
            .mapBy(i => chain.canonical.get(i))
        {
            block_hash_gen: chain.block_hash_gen,
            head_: new_head,
            finalized: chain.finalized,
            block_number: chain.block_number,
            canonical: new_canonical,
            parent: chain.parent,
            reorg_count: chain.reorg_count + 1,
        }
    }

    pure def chainInvariant(chain: Chain): bool = and {
        chain.finalized <= chain.head_,
        chain.canonical.keys().contains(chain.finalized),
        chain.canonical.keys().contains(chain.head_),
        (chain.finalized + 1).to(chain.head_).forall(i => {
            chain.canonical.get(i - 1) == chain.parent.get(chain.canonical.get(i))
        }),
    }
}
