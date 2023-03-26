use std::path::PathBuf;

use erigon_dna::snapshot::decompress::Decompressor;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tracing_test::traced_test;

static LOREM: &'static str = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum";
static WHITE_FOX: &'static str = "the white brown fox jumped over the lazy dog the white brown fox jumped over the lazy dog the white brown fox jumped over the lazy dog the white brown fox jumped over the lazy dog the white brown fox jumped over the lazy dog the white brown fox jumped over the lazy dog";

fn prepare_lorem() -> (Decompressor, Vec<String>) {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/lorem.dat");

    let decomprosser = Decompressor::new(dat).unwrap();

    let words: Vec<_> = LOREM
        .split(" ")
        .enumerate()
        .map(|(i, w)| format!("{} {}", w, i))
        .collect();

    (decomprosser, words)
}

fn prepare_fox() -> (Decompressor, Vec<String>) {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/fox.dat");

    let decomprosser = Decompressor::new(dat).unwrap();

    let words: Vec<_> = WHITE_FOX
        .split(" ")
        .enumerate()
        .map(|(i, w)| format!("{} {}", w, i))
        .collect();

    (decomprosser, words)
}

#[test]
#[traced_test]
fn test_decompress_skip() {
    let (decomprosser, expected) = prepare_lorem();

    let mut getter = decomprosser.getter();

    let mut i = 0;
    let mut buf = [0u8; 2048];
    while getter.has_next_word() {
        if i % 2 == 0 {
            getter.skip_word().expect("skip word");
        } else {
            {
                let size = getter.next_word(&mut buf).expect("next word");
                let word = std::str::from_utf8(&buf[..size]).expect("utf8");
                info!(word = ?word, "got token");
                assert_eq!(word, expected[i]);
            }
        }
        i += 1;
    }
}

#[test]
#[traced_test]
fn test_decompress_skip_with_pattern() {
    let (decomprosser, expected) = prepare_fox();

    let mut getter = decomprosser.getter();

    let mut i = 0;
    let mut buf = [0u8; 2048];
    while getter.has_next_word() {
        if i % 2 == 0 {
            getter.skip_word().expect("skip word");
        } else {
            {
                let size = getter.next_word(&mut buf).expect("next word");
                let word = std::str::from_utf8(&buf[..size]).expect("utf8");
                info!(word = ?word, "got token");
                assert_eq!(word, expected[i]);
            }
        }
        i += 1;
    }
}

#[test]
#[traced_test]
fn test_decompress_bodies() {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-008630-008631-bodies.seg");

    let decomprosser = Decompressor::new(dat).unwrap();
    info!(size = decomprosser.size(), "decompressed size");

    let mut getter = decomprosser.getter();

    let mut count = 0;
    let mut buf = [0u8; 2048];
    while let Some(size) = getter.next_word(&mut buf) {
        assert!(buf.len() > 0);
        info!(token = ?buf[..size], "got token");
        count += 1;
    }

    assert_eq!(1000, count);
}

#[test]
#[traced_test]
fn test_decompress_headers() {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-008630-008631-headers.seg");

    let decomprosser = Decompressor::new(dat).unwrap();
    info!(size = decomprosser.size(), "decompressed size");

    let mut getter = decomprosser.getter();

    let mut count = 0;
    let mut buf = [0u8; 2048];
    while let Some(size) = getter.next_word(&mut buf) {
        assert!(size > 0);
        info!(i = count, token = hex::encode(&buf[..size]), "got token");
        count += 1;
    }

    assert_eq!(1000, count);
}
