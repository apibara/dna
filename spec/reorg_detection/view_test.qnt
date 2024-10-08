// -*- mode: Bluespec; -*-

module view_test {
    import view.* from "./view"
    import types.* from "./types"

    var view: View

    action init = {
        view' = newView({
            number: 0,
            hash: 1_000_000,
            parent: 0,
        })
    }

    run growTest =
        init
            .expect(all {
                view.viewInvariant(),
                not(view.canFinalize(1)),
                view.canGrow({
                    number: 1,
                    hash: 1_000_001,
                    parent: 1_000_000
                })
            })
            .then(9.reps(n => {
                val block = {
                    number: 1 + n,
                    hash: 1_000_001 + n,
                    parent: 1_000_000 + n
                }
                view' = grow(view, block)
            }))
            .expect(all {
                view.viewInvariant(),
                view.blockCount() == 10,
            })
            .then(all {
                view' = view.finalize(5),
            })
            .expect(all {
                view.viewInvariant(),
                view.blockCount() == 5,
            })

    run shrinkTest =
        init
            .then(9.reps(n => {
                val block = {
                    number: 1 + n,
                    hash: 1_000_001 + n,
                    parent: 1_000_000 + n
                }
                view' = grow(view, block)
            }))
            .then(all {
                view' = view.finalize(4),
            })
            .expect(all {
                view.viewInvariant(),
                not(view.canShrink({
                    number: 4,
                    hash: 1_000_004,
                    parent: 1_000_003
                })),
                not(view.canShrink({
                    number: 40,
                    hash: 1_000_040,
                    parent: 1_000_030
                })),
                view.canShrink({
                    number: 5,
                    hash: 1_000_005,
                    parent: 1_000_004
                })
            })
            .then(all {
                view' = view.shrink({
                    number: 5,
                    hash: 1_000_005,
                }),
            })
            .expect(all {
                view.viewInvariant(),
                view.blockCount() == 2,
                view.reorgs.get(5).keys().size() == 0,
                6.to(9).forall(n =>
                    view.reorgs.get(n).get(1_000_000 + n) == {
                        number: 5,
                        hash: 1_000_005,
                    }
                )
            })
            .expect(all {
                view.connect({ number: 7, hash: 1_000_007 }) == OfflineReorg({
                    number: 5,
                    hash: 1_000_005,
                })
            })

    run reorgOnTopOfReorgTest =
        init
            .then(9.reps(n => {
                val block = {
                    number: 1 + n,
                    hash: 1_000_001 + n,
                    parent: 1_000_000 + n
                }
                view' = grow(view, block)
            }))
            .then(all {
                view' = view.finalize(2),
            })
            .then(all {
                view' = view.shrink({
                    number: 5,
                    hash: 1_000_005,
                }),
            })
            .then(all {
                view' = view.grow({
                    number: 6,
                    hash: 2_000_006,
                    parent: 1_000_005
                })
            })
            .then(2.reps(n => {
                val block = {
                    number: 7 + n,
                    hash: 2_000_007 + n,
                    parent: 2_000_006 + n
                }
                all {
                    view.canGrow(block),
                    view' = view.grow(block)
                }
            }))
            .then(all {
                view' = view.shrink({
                    number: 3,
                    hash: 1_000_003,
                }),
            })
            .expect(all {
                view.viewInvariant(),
                view.blockCount() == 2,
            })
            .expect(all {
                // Connect from original chain.
                view.connect({ number: 3, hash: 1_000_003 }) == Continue,
                // Connect from the latest reorged block.
                view.connect({ number: 6, hash: 2_000_006 }) == OfflineReorg({
                    number: 3,
                    hash: 1_000_003,
                }),
                // Connect from first reorged block.
                view.connect({ number: 7, hash: 1_000_007 }) == OfflineReorg({
                    number: 3,
                    hash: 1_000_003,
                })
            })
}
