// -*- mode: Bluespec; -*-

module simple {
    var servers: Set[str]
    var online: Set[str]

    pure val servers_initial = Set("a", "b", "c")

    action init = all {
        servers' = servers_initial,
        online' = servers_initial,
    }

    action step = {
        nondet server = servers.oneOf()
        any {
            all {
                online' = online,
                servers' = servers
            },
            all {
                not(online.contains(server)),
                online' = online.union(Set(server)),
                servers' = servers,
            },
            all {
                online.contains(server),
                online.size() > 1,
                online' = online.exclude(Set(server)),
                servers' = servers
            }
        }
    }

    val invariant = {
        servers.exists(s => online.contains(s))
    }

    temporal safety = {
        always(invariant)
    }

    temporal liveness = {
        not(always(servers == online))
    }
}
