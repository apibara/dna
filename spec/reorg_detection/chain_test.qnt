// -*- mode: Bluespec; -*-

module chain_test {
    import chain.* from "./chain"
    import types.* from "./types"

    var chain: Chain

    action init = {
        chain' = newChain({
            number: 0,
            hash: 1_000_000,
            parent: 0,
        })
    }

    run chainBehaviorTest =
        init
            .expect(all {
                chain.chainInvariant(),
                chain.getHead() == {
                    number: 0,
                    hash: 1_000_000,
                    parent: 0,
                }
            })
            .then(10.reps(_ => all {
                chain' = chain.advanceHead()
            }))
            .expect(all {
                chain.chainInvariant(),
                chain.canReorg(1),
                chain.getHead() == {
                    number: 10,
                    hash: 1_000_010,
                    parent: 1_000_009,
                }
            })
            .then(all {
                chain' = chain.advanceFinalizedBy(5),
            })
            .expect(all {
                chain.chainInvariant(),
                not(chain.canReorg(1)),
                chain.getFinalized() == {
                    number: 5,
                    hash: 1_000_005,
                    parent: 1_000_004,
                }
            })
            .then(all {
                chain' = chain.reorg(6),
            })
            .expect(all {
                chain.chainInvariant(),
                chain.getHead() == {
                    number: 6,
                    hash: 1_000_006,
                    parent: 1_000_005,
                }
            })
            .then(all {
                chain' = chain.advanceHead()
            })
            .expect(all {
                chain.chainInvariant(),
                chain.getHead() == {
                    number: 7,
                    hash: 1_000_011,
                    parent: 1_000_006,
                }
            })
}
