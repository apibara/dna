// -*- mode: Bluespec; -*-

module ingestion_short {
    import ingestion(MAX_BLOCK_NUMBER = 5, MAX_REORGS = 1).* from "./ingestion"
}
