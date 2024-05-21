pub mod io;

deno_core::extension!(
    indexer_main,
    esm_entry_point = "ext:indexer_main/bootstrap.js",
    esm = [dir "js", "bootstrap.js"],
);
