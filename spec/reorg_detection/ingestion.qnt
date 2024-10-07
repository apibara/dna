// -*- mode: Bluespec; -*-

/*
 * The goal of ingestion is to keep the view in sync with the chain.
 */
module ingestion {
    import chain.* from "./chain"
    import view.* from "./view"
    import types.* from "./types"

    const MAX_BLOCK_NUMBER: int
    const MAX_REORGS: int

    type IngestionState =
        | Init({ view: View })
        | ForceHeadRefresh({ view: View, head_: Cursor })
        | Ingest({ view: View, head_: Cursor })
        | FetchParentAndRecover({
            view: View,
            existing_head: Cursor,
            parent_hash: BlockHash,
        })
        | Recover({
            view: View,
            existing_head: Cursor,
            incoming_head: Block,
            removed: List[Cursor]
        })

    var chain: Chain
    var state: IngestionState

    action init = {
        val view = newView({ number: 0, hash: 1_000_000, parent: 0 })
        all {
            chain' = newChain({ number: 0, hash: 1_000_000, parent: 0 }),
            state' = Init({ view: view }),
        }
    }

    action stutter = all {
        state' = state,
        chain' = chain,
    }

    action chainAdvanceHead = {
        if (chain.head_ < MAX_BLOCK_NUMBER) {
            all {
                chain' = advanceHead(chain),
                state' = state,
            }
        } else {
            stutter
        }
    }

    action chainAdvanceFinalizedBy(jump: int): bool = {
        all {
            chain' = advanceFinalizedBy(chain, jump),
            state' = state,
        }
    }

    action chainAdvanceFinalized = {
        // nondet jump = 1.to(maxFinalizedJump(chain)).oneOf()
        // Not important for this spec.
        val jump = 1
        chainAdvanceFinalizedBy(jump)
    }

    action chainReorgTo(new_head: int): bool = {
        all {
            chain' = reorg(chain, new_head),
            state' = state,
        }
    }

    action chainReorg = {
        nondet new_head = chain.finalized.to(chain.head_ - 1).oneOf()
        all {
            chain.reorg_count < MAX_REORGS,
            chainReorgTo(new_head)
        }
    }

    action chainStep = {
        if (maxFinalizedJump(chain) > 0) {
            any {
                chainAdvanceHead,
                chainAdvanceFinalized,
                chainReorg,
            }
        } else {
            any {
                chainAdvanceHead,
            }
        }
    }

    action addNextBlock(view: View, head_: Cursor, block: Block): bool = {
        // If we can grow the view, do so.
        if (view.canGrow(block)) {
            val new_view = view.grow(block)
            all {
                chain' = chain,
                state' = Ingest({ view: new_view, head_: head_ }),
            }
        } else {
            // Block cannot be applied to the view.
            // We need to fetch the parent and try to recover from there.
            all {
                chain' = chain,
                state' = FetchParentAndRecover({
                    view: view,
                    existing_head: head_,
                    parent_hash: block.parent,
                }),
            }
        }
    }

    action refreshHead(view: View, head_: Cursor): bool = {
        val block = chain.getHead()
        val new_head = { number: block.number, hash: block.hash }

        if (block.number < view.head_) {
            // TODO: add removed blocks to the Recover state.
            val existing_head_hash = view.canonical.get(block.number)
            all {
                chain' = chain,
                state' = Recover({
                    view: view,
                    existing_head: { number: block.number, hash: existing_head_hash },
                    incoming_head: block,
                    removed: List(),
                }),
            }
        } else if (block.number == view.head_) {
            // Nothing to do.
            if (block.hash == view.canonical.get(block.number)) {
                all {
                    chain' = chain,
                    state' = Ingest({ view: view, head_: new_head }),
                }
            } else {
                all {
                    chain' = chain,
                    state' = Recover({
                        view: view,
                        existing_head: head_,
                        incoming_head: block,
                        removed: List(),
                    }),
                }
            }
        } else if (block.number == view.head_ + 1) {
            addNextBlock(view, new_head, block)
        } else {
            // Continue ingestion.
            all {
                chain' = chain,
                state' = Ingest({ view: view, head_: new_head }),
            }
        }
    }

    action refreshFinalized(view: View, head_: Cursor): bool = {
        val new_finalized = chain.getFinalized()
        val clamped_finalized = if (new_finalized.number > view.head_) {
            view.head_
        } else {
            new_finalized.number
        }
        val new_view = view.finalize(clamped_finalized)

        all {
            chain' = chain,
            state' = Ingest({ view: new_view, head_: head_ }),
        }
    }

    action ingestNextBlock(view: View, head_: Cursor): bool = {
        val block = chain.getBlockByNumber(view.head_ + 1)
        match block {
            | Some(b) => {
                addNextBlock(view, head_, b)
            }
            | None => {
                all {
                    chain' = chain,
                    state' = ForceHeadRefresh({ view: view, head_: head_ }),
                }
            }
        }
    }

    action fetchParentAndRecover(view: View, existing_head: Cursor, parent_hash: BlockHash): bool = {
        val block = chain.getBlockByHash(parent_hash)
        all {
            chain' = chain,
            state' = Recover({
                view: view,
                existing_head: existing_head,
                incoming_head: block,
                removed: List(),
            }),
        }
    }

    action recover(view: View, existing_head: Cursor, incoming_head: Block, removed: List[Cursor]): bool = {
        if (incoming_head.number == existing_head.number and incoming_head.hash == existing_head.hash) {
            // Recovered
            val new_view = view.shrink(existing_head)
            all {
                chain' = chain,
                state' = Ingest({ view: new_view, head_: existing_head }),
            }
        } else {
            val parent = chain.getBlockByHash(incoming_head.parent)
            val existing_parent_hash = view.canonical.get(parent.number)
            // TODO: add removed blocks to the Recover state.
            all {
                chain' = chain,
                state' = Recover({
                    view: view,
                    existing_head: { number: parent.number, hash: existing_parent_hash },
                    incoming_head: parent,
                    removed: removed,
                }),
            }
        }
    }

    action initView(view: View): bool = {
        val block = chain.getHead()
        val cursor = { number: block.number, hash: block.hash }
        all {
            chain' = chain,
            state' = Ingest({ view: view, head_: cursor }),
        }
    }

    action viewStep = {
        match state {
            | Init(s) => {
                initView(s.view)
            }
            | ForceHeadRefresh(s) => {
                refreshHead(s.view, s.head_)
            }
            | FetchParentAndRecover(s) => {
                fetchParentAndRecover(s.view, s.existing_head, s.parent_hash)
            }
            | Recover(s) => {
                recover(s.view, s.existing_head, s.incoming_head, s.removed)
            }
            | Ingest(s) => {
                if (s.view.head_ < s.head_.number) {
                    any {
                        refreshHead(s.view, s.head_),
                        refreshFinalized(s.view, s.head_),
                        ingestNextBlock(s.view, s.head_),
                    }
                } else {
                    any {
                        refreshHead(s.view, s.head_),
                        refreshFinalized(s.view, s.head_),
                    }
                }
            }
        }
    }

    action step = any {
        all {
            chain.head_ <= MAX_BLOCK_NUMBER,
            chainStep
        },
        all {
            match state {
                | Ingest(s) => { s.view.head_ <= MAX_BLOCK_NUMBER }
                | _ => true
            },
            viewStep
        },
    }

    val invariant = and {
        chain.chainInvariant(),
        match state {
            | Init(s) => { s.view.viewInvariant() }
            | Ingest(s) => { s.view.viewInvariant() }
            | Recover(s) => { s.view.viewInvariant() }
            | ForceHeadRefresh(s) => { s.view.viewInvariant() }
            | FetchParentAndRecover(s) => { s.view.viewInvariant() }
        }
    }

    val chainReachesMax = and {
        chain.head_ == MAX_BLOCK_NUMBER,
    }

    val chainIsIngested = {
        match state {
            | Init(s) => { false }
            | Recover(s) => { false }
            | ForceHeadRefresh(s) => { false }
            | FetchParentAndRecover(s) => { false }
            | Ingest(s) => and {
                s.view.head_ <= chain.head_,
                s.view.finalized <= chain.finalized,
                0.to(MAX_BLOCK_NUMBER).forall(n => and {
                    if (s.view.canonical.keys().contains(n)) {
                        s.view.canonical.get(n) == chain.canonical.get(n)
                    } else {
                        true
                    }
                })
            }
        }
    }

    temporal ingestion = eventually(chainIsIngested)
}
