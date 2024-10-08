// -*- mode: Bluespec; -*-

module types {
    type Cursor = {
        number: int,
        hash: BlockHash,
    }

    type Option[a] =
        | Some(a)
        | None

    // Unique block hash.
    type BlockHash = int

    // A block.
    type Block = {
        number: int,
        hash: BlockHash,
        parent: BlockHash,
    }

    type IngestionMessage = {
        block: Block,
    }

    type InvalidationMessage = {
        head: Block,
        removed: List[Block],
    }

    type Message =
        | MessageIngest(IngestionMessage)
        | MessageInvalidate(InvalidationMessage)


    pure def unwrap_or(opt: Option[a], default: a): a = {
        match opt {
            | Some(x) => x
            | None => default
        }
    }

    pure def option_map(opt: Option[a], f: a => b): Option[b] = {
        match opt {
            | Some(x) => Some(f(x))
            | None => None
        }
    }

    pure def removeFromMap(_map: a -> b, _pred: (a) => bool): a -> b = {
        _map
            .keys()
            .filter(i => not(_pred(i)))
            .map(i => (i, _map.get(i)))
            .setToMap()
    }

    pure def mapRemoveRange(m: int -> b, lb: int, ub: int): int -> b =
        m.keys().filter(i => i < lb or i > ub).mapBy(i => m.get(i))
}
