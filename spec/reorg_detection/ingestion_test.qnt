// -*- mode: Bluespec; -*-

module ingestion_test {
    import ingestion(MAX_BLOCK_NUMBER = 10, MAX_REORGS = 10).* from "./ingestion"
    import types.* from "./types"


    pure def ingestedHead(state: IngestionState): int = match state {
        | Init(s) => s.view.head_
        | Ingest(s) => s.view.head_
        | Recover(s) => s.view.head_
        | ForceHeadRefresh(s) => s.view.head_
        | FetchParentAndRecover(s) => s.view.head_
    }

    pure def ingestedFinalized(state: IngestionState): int = match state {
        | Init(s) => s.view.finalized
        | Ingest(s) => s.view.finalized
        | Recover(s) => s.view.finalized
        | ForceHeadRefresh(s) => s.view.head_
        | FetchParentAndRecover(s) => s.view.head_
    }

    run linearIngestionTest =
        init
            .then(5.reps(_ => chainAdvanceHead))
            .expect(all {
                chain.head_ == 5,
                ingestedHead(state) == 0,
                ingestedFinalized(state) == 0,
            })
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(all {
                chainAdvanceFinalizedBy(2),
            })
            .then(all {
                match state {
                    | Ingest(s) => refreshFinalized(s.view, s.head_)
                    | _ => stutter
                }
            })
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                // Finalized cannot be higher than the ingested head.
                // TODO: probably we should allow this.
                ingestedFinalized(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })

    // Detect a reorg caused by a chain shrink while ingesting block n + 1.
    run recoverFromChainShrinkOnIngestNextBlockTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(5.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })
            .then(all {
                chainReorgTo(3),
            })
            .then(all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | ForceHeadRefresh(_) => true
                    | _ => false
                }
            })
            .then(viewStep)
            .then(viewStep)
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 3,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003),
                        v.view.reorgs.get(4) == Map(1_000_004 -> { number: 3, hash: 1_000_003 }),
                        v.view.reorgs.get(5) == Map(1_000_005 -> { number: 3, hash: 1_000_003 }),
                    }
                    | _ => false
                }
            })

    // Detect a reorg caused by a chain shrink while refreshing head.
    run recoverFromChainShrinkOnHeadRefreshTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(5.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })
            .then(all {
                chainReorgTo(3),
            })
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Recover(_) => true
                    | _ => false
                }
            })
            .then(viewStep)
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 3,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003),
                        v.view.reorgs.get(4) == Map(1_000_004 -> { number: 3, hash: 1_000_003 }),
                        v.view.reorgs.get(5) == Map(1_000_005 -> { number: 3, hash: 1_000_003 }),
                    }
                    | _ => false
                }
            })

    // Detect a reorg caused by a chain shrink + growth.
    run recoverFromChangedHeadOnHeadRefreshTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(5.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })
            .then(all {
                chainReorgTo(4),
            })
            .then(chainAdvanceHead)
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Recover(_) => true
                    | _ => false
                }
            })
            .then(viewStep)
            .then(viewStep)
            .then(all {
                match state {
                    | Ingest(v) => ingestNextBlock(v.view, v.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 5,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003, 4 -> 1_000_004, 5 -> 1_000_006),
                        v.view.reorgs.get(5) == Map(1_000_005 -> { number: 4, hash: 1_000_004 }),
                    }
                    | _ => false
                }
            })

    run recoverFromWrongNextBlockOnHeadRefreshTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(5.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })
            .then(all {
                chainReorgTo(4),
            })
            .then(2.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | FetchParentAndRecover(_) => true
                    | _ => false
                }
            })
            .then(viewStep)
            .then(viewStep)
            .then(viewStep)
            .then(all {
                match state {
                    | Ingest(v) => ingestNextBlock(v.view, v.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 5,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003, 4 -> 1_000_004, 5 -> 1_000_006),
                        v.view.reorgs.get(5) == Map(1_000_005 -> { number: 4, hash: 1_000_004 }),
                    }
                    | _ => false
                }
            })

    // Detect a reorg that results in a longer chain,
    // but the fork point is in the past.
    run recoverFromLongerChainReorgTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(5.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(5.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 5,
            })
            .then(all {
                chainReorgTo(4),
            })
            .then(4.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Ingest(_) => true
                    | _ => false
                }
            })
            .then(all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | FetchParentAndRecover(_) => true
                    | _ => false
                }
            })
            .then(viewStep)
            .then(viewStep)
            .then(viewStep)
            .then(3.reps(_ => all {
                match state {
                    | Ingest(v) => ingestNextBlock(v.view, v.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 7,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003, 4 -> 1_000_004, 5 -> 1_000_006, 6 -> 1_000_007, 7 -> 1_000_008),
                        v.view.reorgs.get(5) == Map(1_000_005 -> { number: 4, hash: 1_000_004 }),
                    }
                    | _ => false
                }
            })

    // Detect a reorg that results in a longer chain,
    // but the fork point is not ingested yet.
    run ingestUnaffectedByReorgTest =
        init
            .then(all {
                match state {
                    | Init(s) => initView(s.view)
                    | _ => stutter
                }
            })
            .then(8.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 0,
            })
            .then(2.reps(_ => all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                ingestedFinalized(state) == 0,
                ingestedHead(state) == 2,
            })
            .then(all {
                chainReorgTo(4),
            })
            .then(4.reps(_ => chainAdvanceHead))
            .then(all {
                match state {
                    | Ingest(s) => refreshHead(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Ingest(_) => true
                    | _ => false
                }
            })
            .then(all {
                match state {
                    | Ingest(s) => ingestNextBlock(s.view, s.head_)
                    | _ => stutter
                }
            })
            .expect(all {
                match state {
                    | Ingest(_) => true
                    | _ => false
                }
            })
            .then(4.reps(_ => all {
                match state {
                    | Ingest(v) => ingestNextBlock(v.view, v.head_)
                    | _ => stutter
                }
            }))
            .expect(all {
                match state {
                    | Ingest(v) => all {
                        v.view.head_ == 7,
                        v.view.canonical == Map(0 -> 1_000_000, 1 -> 1_000_001, 2 -> 1_000_002, 3 -> 1_000_003, 4 -> 1_000_004, 5 -> 1_000_009, 6 -> 1_000_010, 7 -> 1_000_011),
                        v.view.reorgs.get(5) == Map()
                    }
                    | _ => false
                }
            })
}
