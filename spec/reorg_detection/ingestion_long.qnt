// -*- mode: Bluespec; -*-

module ingestion_long {
    import ingestion(MAX_BLOCK_NUMBER = 10, MAX_REORGS = 3).* from "./ingestion"
}
