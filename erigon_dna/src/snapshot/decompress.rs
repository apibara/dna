use std::{
    fs,
    io::{Cursor, Read},
    path::PathBuf,
    time::SystemTime,
};

use byteorder::{BigEndian, ReadBytesExt};
use memmap::Mmap;
use tracing::{info, trace};

/// Tables with bitlen greater than this will be condensed.
/// To disable at all, set to 9.
const CONDENSED_PATTERN_TABLE_THRESHOLD: u8 = 9;

#[derive(Debug, thiserror::Error)]
pub enum DecompressorError {}

#[derive(Debug)]
pub struct Decompressor {
    /// The file being decompressed.
    file_name: PathBuf,
    /// When the file was last modified.
    mod_time: SystemTime,
    /// The size of the file, in bytes.
    size: u64,
    /// Mmapped content of the file.
    mmap: Mmap,
    /// Number of words in the file.
    words_count: u64,
    /// Number of empty words in the file.
    empty_words_count: u64,
    /// Start of the words in the `mmap`.
    words_start: usize,
    /// Positions table.
    position_table: Option<PositionTable>,
    /// Pattern table.
    pattern_table: Option<PatternTable>,
}

pub struct Getter<'a> {
    /// The decompressor.
    decompressor: &'a Decompressor,
    /// Current data.
    data: &'a [u8],
    /// Position in `data`.
    pos: usize,
    /// Value 0..7 - position of the bit
    bit: u8,
}

impl Decompressor {
    pub fn new(file_name: PathBuf) -> Result<Self, DecompressorError> {
        let stat = fs::metadata(&file_name).expect("failed to stat file");

        let file = fs::File::open(&file_name).expect("failed to open file");

        let mmap = unsafe { Mmap::map(&file).expect("failed to mmap file") };

        let mut cursor = Cursor::new(&mmap);
        let words_count = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read words count");
        let empty_words_count = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read empty words count");

        info!(
            words_count = words_count,
            empty_words_count = empty_words_count,
            "read file header"
        );

        // read patterns
        let patterns_size = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read patterns size");
        let starting_pos = cursor.position();
        let mut patterns = Vec::with_capacity(patterns_size as usize);
        let mut pattern_max_depth = 0;

        trace!(pattern_size = patterns_size, "read patterns");

        while cursor.position() - starting_pos < patterns_size {
            let depth =
                unsigned_varint::io::read_u64(&mut cursor).expect("failed to decode pattern depth");
            assert!(depth <= 2048, "dictionary is invalid: depth={}", depth);

            if depth > pattern_max_depth {
                pattern_max_depth = depth;
            }

            let pattern_size =
                unsigned_varint::io::read_u64(&mut cursor).expect("failed to decode pattern size");
            let mut pattern = Vec::with_capacity(pattern_size as usize);
            (&mut cursor)
                .take(pattern_size)
                .read_to_end(&mut pattern)
                .expect("failed to read pattern");

            // trace!(depth = depth, pattern_size = pattern_size, pattern = ?pattern, "read pattern");
            patterns.push((depth, pattern));
        }

        assert_eq!(cursor.position() - starting_pos, patterns_size);

        let pattern_table = if patterns_size > 0 {
            let bitlen = u8::min(9, pattern_max_depth as u8);
            let table = PatternTable::from_patterns(&patterns, bitlen, pattern_max_depth);
            Some(table)
        } else {
            None
        };

        // read positions
        let positions_size = cursor
            .read_u64::<BigEndian>()
            .expect("failed to read positions size");
        let starting_pos = cursor.position();
        let mut positions = Vec::with_capacity(positions_size as usize);
        let mut position_max_depth = 0;

        trace!(positions_size = positions_size, "read positions");

        while cursor.position() - starting_pos < positions_size {
            let depth =
                unsigned_varint::io::read_u64(&mut cursor).expect("failed to decode pattern depth");
            assert!(depth <= 2048, "dictionary is invalid: depth={}", depth);

            if depth > position_max_depth {
                position_max_depth = depth;
            }

            let position =
                unsigned_varint::io::read_u64(&mut cursor).expect("failed to decode position");
            // trace!(depth = depth, position = position, "read position");
            positions.push((depth, position));
        }

        assert_eq!(cursor.position() - starting_pos, positions_size);

        let position_table = if positions_size > 0 {
            let bitlen = u8::min(9, position_max_depth as u8);
            let table = PositionTable::from_positions(&positions, bitlen, position_max_depth);
            Some(table)
        } else {
            None
        };
        let words_start = cursor.position() as usize;

        let decompressor = Self {
            file_name,
            mod_time: stat
                .modified()
                .expect("failed to get file modification time"),
            size: stat.len(),
            mmap,
            words_count,
            empty_words_count,
            words_start,
            position_table,
            pattern_table,
        };

        Ok(decompressor)
    }

    fn pattern_root(&self) -> &Pattern {
        self.pattern_at_index(0)
    }

    fn pattern_at_index(&self, index: usize) -> &Pattern {
        if let Some(table) = &self.pattern_table {
            &table.children[index]
        } else {
            panic!("expected pattern table")
        }
    }

    fn codeword_at_index(&self, index: usize) -> &Codeword {
        if let Some(table) = &self.pattern_table {
            &table.codewords[index]
        } else {
            panic!("expected pattern table")
        }
    }

    fn condensed_table_search(&self, pat: &Pattern, code: u16) -> usize {
        if pat.bitlen <= CONDENSED_PATTERN_TABLE_THRESHOLD {
            return pat.patterns[code as usize].expect("missing pattern codeword");
        }
        todo!()
    }

    pub fn getter(&self) -> Getter {
        Getter {
            decompressor: self,
            data: &self.mmap[self.words_start..],
            pos: 0,
            bit: 0,
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn count(&self) -> u64 {
        self.words_count
    }

    pub fn empty_words_count(&self) -> u64 {
        self.empty_words_count
    }
}

impl<'a> Getter<'a> {
    pub fn reset(&mut self, pos: usize) {
        self.pos = pos;
        self.bit = 0;
    }

    pub fn has_next_word(&self) -> bool {
        self.pos < self.data.len()
    }

    pub fn skip_word(&mut self) -> Option<usize> {
        // -1 to drop 0 terminator
        let word_len = self.next_pos(true) - 1;
        trace!(
            word_len = word_len,
            pos = self.pos,
            bit = self.bit,
            "skip word start"
        );
        if word_len == 0 {
            todo!()
        }

        let mut add = 0;
        let mut buf_pos = 0;
        let mut last_uncovered = 0;
        let mut pos = self.next_pos(false);
        let mut scratch = [0u8; 1024];
        while pos != 0 {
            buf_pos += pos - 1;
            if word_len < buf_pos {
                panic!("likely .idx is invalid");
            }
            if buf_pos > last_uncovered {
                add += buf_pos - last_uncovered;
            }
            last_uncovered = buf_pos + self.next_pattern(&mut scratch);
            pos = self.next_pos(false);
        }

        if self.bit > 0 {
            self.pos += 1;
            self.bit = 0;
        }

        if word_len > last_uncovered {
            add += word_len - last_uncovered;
        }

        self.pos += add as usize;
        Some(add)
    }

    pub fn next_word(&mut self, buf: &mut [u8]) -> Option<usize> {
        if !self.has_next_word() {
            return None;
        }
        let save_pos = self.pos;
        // -1 to drop 0 terminator
        let word_len = self.next_pos(true) - 1;
        trace!(
            save_pos = save_pos,
            word_len = word_len,
            bit = self.bit,
            "next word start"
        );

        if word_len == 0 {
            if self.bit > 0 {
                self.pos += 1;
                self.bit = 0;
            }
            return Some(0);
        }

        // fill in the pattern
        let mut pos = self.next_pos(false);
        let mut buf_pos = 0;
        while pos != 0 {
            // trace!(buf_pos = buf_pos, pos = pos, buf = ?buf, "fill in patterns");
            buf_pos += pos - 1;
            self.next_pattern(&mut buf[buf_pos..]);
            pos = self.next_pos(false);
        }

        if self.bit > 0 {
            self.pos += 1;
            self.bit = 0;
        }

        let mut post_loop_pos = self.pos;
        self.pos = save_pos;
        self.bit = 0;
        trace!(pos = self.pos, bit = self.bit, "  after fill");

        // reset state of the huffman reader
        self.next_pos(true);
        trace!(pos = self.pos, bit = self.bit, "  after next pos");

        // fill data not in patterns
        let mut pos = self.next_pos(false);
        let mut buf_pos = 0;
        let mut last_uncovered = 0;
        let mut scratch = [0u8; 1024];
        trace!("fill in not patterns");
        while pos != 0 {
            buf_pos += pos - 1;
            if buf_pos > last_uncovered {
                let diff = buf_pos - last_uncovered;
                buf[last_uncovered..buf_pos]
                    .copy_from_slice(&self.data[post_loop_pos..post_loop_pos + diff]);
                post_loop_pos += diff;
            }
            last_uncovered = buf_pos + self.next_pattern(&mut scratch);
            pos = self.next_pos(false);
        }

        if word_len > last_uncovered {
            let diff = word_len - last_uncovered;
            buf[last_uncovered..word_len]
                .copy_from_slice(&self.data[post_loop_pos..post_loop_pos + diff]);
            post_loop_pos += diff;
        }

        self.pos = post_loop_pos;
        self.bit = 0;

        trace!(pos = self.pos, bit = self.bit, "  before return");
        Some(word_len)
    }

    fn next_pos(&mut self, clean: bool) -> usize {
        if clean {
            if self.bit > 0 {
                self.pos += 1;
                self.bit = 0;
            }
        }

        let pos_table = self
            .decompressor
            .position_table
            .as_ref()
            .expect("position table is missing");
        let mut root_pos = pos_table.root();
        if root_pos.bitlen == 0 {
            return root_pos.positions[0].0 as usize;
        }

        let mut pos_len = 0;
        let mut pos = 0;
        while pos_len == 0 {
            let mut code = (self.data[self.pos] as u16) >> self.bit;
            if 8 - self.bit < root_pos.bitlen && self.pos + 1 < self.data.len() {
                code |= (self.data[self.pos + 1] as u16) << (8 - self.bit);
            }
            code &= (1u16 << root_pos.bitlen) - 1;
            pos_len = root_pos.positions[code as usize].1;
            if pos_len == 0 {
                let child_idx = root_pos.children[code as usize].expect("position children");
                root_pos = pos_table.position_at_index(child_idx);
                self.bit += 9;
            } else {
                self.bit += pos_len;
                pos = root_pos.positions[code as usize].0 as usize;
            }
            self.pos += (self.bit / 8) as usize;
            self.bit %= 8;
        }

        pos
    }

    fn next_pattern(&mut self, buf: &mut [u8]) -> usize {
        let mut table = self.decompressor.pattern_root();
        trace!(bitlen = table.bitlen, "start pattern");

        if table.bitlen == 0 {
            let codeword_idx = table.patterns[0].expect("pattern table is bad (1)");
            let cw = self.decompressor.codeword_at_index(codeword_idx);
            let src_pat = &cw.pattern;
            buf[..src_pat.len()].copy_from_slice(&src_pat);

            return src_pat.len();
        }

        let mut pat_len = 0;
        let mut wrote_bytes = 0;
        while pat_len == 0 {
            let mut code = (self.data[self.pos] as u16) >> self.bit;
            if 8 - self.bit < table.bitlen && self.pos + 1 < self.data.len() {
                code |= (self.data[self.pos + 1] as u16) << (8 - self.bit);
            }
            code &= (1u16 << table.bitlen) - 1;

            let codeword_idx = self.decompressor.condensed_table_search(table, code);
            let codeword = self.decompressor.codeword_at_index(codeword_idx);
            pat_len = codeword.size;
            trace!(code = code, pat_len = pat_len, "    pattern loop");
            if pat_len == 0 {
                let pat_idx = codeword.table.expect("must have table");
                table = self.decompressor.pattern_at_index(pat_idx);
                self.bit += 9;
            } else {
                self.bit += pat_len;
                let src_pat = &codeword.pattern;
                // trace!(src_pat = ?src_pat, buf = ?buf, "write pat");
                buf[..src_pat.len()].copy_from_slice(src_pat);
                wrote_bytes = src_pat.len();
            }
            self.pos += (self.bit / 8) as usize;
            self.bit %= 8;
        }

        return wrote_bytes;
    }
}

/// Pattern table.
#[derive(Debug)]
struct PatternTable {
    children: Vec<Pattern>,
    codewords: Vec<Codeword>,
}

#[derive(Debug)]
struct Pattern {
    index: usize,
    bitlen: u8,
    // Codeword index.
    patterns: Vec<Option<usize>>,
}

#[derive(Debug)]
struct Codeword {
    index: usize,
    /// pattern corresponding to entries.
    pattern: Vec<u8>,
    /// Code associated with the pattern.
    code: u16,
    /// Number of bits in the code.
    size: u8,
    /// Pointer to pattern table.
    table: Option<usize>,
}

impl PatternTable {
    fn from_patterns(patterns: &[(u64, Vec<u8>)], bitlen: u8, max_depth: u64) -> Self {
        let root_pattern = Pattern::with_bitlen(0, bitlen);
        let children = vec![root_pattern];
        let codewords = Vec::default();

        let mut table = PatternTable {
            children,
            codewords,
        };
        table.build_table(0, patterns, 0, 0, 0, max_depth);

        table
    }

    fn build_table(
        &mut self,
        pat_idx: usize,
        patterns: &[(u64, Vec<u8>)],
        code: u16,
        bits: u8,
        depth: u64,
        max_depth: u64,
    ) -> usize {
        /*
        trace!(
            depth = depth,
            max_depth = max_depth,
            code = format!("{:b}", code),
            bits = bits,
            patterns = ?patterns,
            "build pattern table"
        );
        */

        if patterns.is_empty() {
            return 0;
        }

        let (curr_depth, curr_pat) = &patterns[0];
        if depth == *curr_depth {
            // TODO: is it possible to avoid a copy here?
            // trace!("insert codeword 1");
            let codeword_idx = self.new_codeword(curr_pat.clone(), code, bits, None).index;
            self.insert_codeword_in_table(pat_idx, codeword_idx);
            return 1;
        }

        if bits == 9 {
            let bitlen = u64::min(9, max_depth);
            let new_pat_idx = self.new_pattern(bitlen as u8).index;
            let codeword_idx = self
                .new_codeword(Vec::default(), code, 0, Some(new_pat_idx))
                .index;
            self.insert_codeword_in_table(pat_idx, codeword_idx);
            return self.build_table(new_pat_idx, patterns, 0, 0, depth, max_depth);
        }

        let b0 = self.build_table(pat_idx, patterns, code, bits + 1, depth + 1, max_depth - 1);
        let b1 = self.build_table(
            pat_idx,
            &patterns[b0..],
            (1u16 << bits) | code,
            bits + 1,
            depth + 1,
            max_depth - 1,
        );
        return b0 + b1;
    }

    fn new_codeword(
        &mut self,
        pattern: Vec<u8>,
        code: u16,
        size: u8,
        table: Option<usize>,
    ) -> &Codeword {
        let index = self.codewords.len();
        let cw = Codeword {
            index,
            pattern,
            code,
            size,
            table,
        };

        self.codewords.push(cw);
        &self.codewords[index]
    }

    fn new_pattern(&mut self, bitlen: u8) -> &Pattern {
        let pattern_idx = self.children.len();
        let pattern = Pattern::with_bitlen(pattern_idx, bitlen);
        self.children.push(pattern);
        &self.children[pattern_idx]
    }

    fn insert_codeword_in_table(&mut self, pat_idx: usize, codeword_idx: usize) {
        let codeword = &self.codewords[codeword_idx];
        let table = &mut self.children[pat_idx];

        if table.bitlen <= CONDENSED_PATTERN_TABLE_THRESHOLD {
            let code_step = 1u16 << codeword.size;
            let code_from = codeword.code;
            let mut code_to = codeword.code + code_step;
            if table.bitlen != codeword.size && codeword.size > 0 {
                code_to = code_from | (1u16 << table.bitlen);
            }

            for c in (code_from..code_to).step_by(code_step as usize) {
                table.patterns[c as usize] = Some(codeword.index);
            }

            // trace!(table = ?table, "insert codeword in table");
            return;
        }

        todo!()
    }
}

impl Pattern {
    fn with_bitlen(index: usize, bitlen: u8) -> Self {
        let patterns = if bitlen <= CONDENSED_PATTERN_TABLE_THRESHOLD {
            let size = (1u16 << bitlen) as usize;
            let mut patterns = Vec::with_capacity(size);
            patterns.resize_with(size, Default::default);
            patterns
        } else {
            todo!()
        };

        Pattern {
            index,
            bitlen,
            patterns,
        }
    }
}

#[derive(Debug)]
struct PositionTable {
    children: Vec<Position>,
}

#[derive(Debug)]
struct Position {
    bitlen: u8,
    positions: Vec<(u64, u8)>,
    children: Vec<Option<usize>>,
}

impl PositionTable {
    fn from_positions(positions: &[(u64, u64)], bitlen: u8, max_depth: u64) -> Self {
        let root_position = Position::with_bitlen(bitlen);
        let children = vec![root_position];

        let mut table = PositionTable { children };
        table.build_table(0, positions, 0, 0, 0, max_depth);

        table
    }

    fn root(&self) -> &Position {
        self.position_at_index(0)
    }

    fn position_at_index(&self, idx: usize) -> &Position {
        &self.children[idx]
    }

    fn build_table(
        &mut self,
        pos_idx: usize,
        positions: &[(u64, u64)],
        code: u16,
        bits: u8,
        depth: u64,
        max_depth: u64,
    ) -> usize {
        /*
        trace!(
            depth = depth,
            max_depth = max_depth,
            code = format!("{:b}", code),
            bits = bits,
            positions = ?positions,
            "build position table"
        );
        */

        if positions.is_empty() {
            return 0;
        }

        let (curr_depth, curr_pos) = positions[0];
        if depth == curr_depth {
            let table = &mut self.children[pos_idx];
            if table.bitlen == bits {
                table.positions[code as usize] = (curr_pos, bits);
            } else {
                let code_step = 1u16 << bits;
                let code_from = code;
                let code_to = code | (1u16 << table.bitlen);
                for c in (code_from..code_to).step_by(code_step as usize) {
                    table.positions[c as usize] = (curr_pos, bits);
                }
            }
            return 1;
        }

        if bits == 9 {
            let bitlen = u64::min(9, max_depth);
            let new_pos_idx = {
                let position = Position::with_bitlen(bitlen as u8);
                let position_idx = self.children.len();
                self.children.push(position);
                position_idx
            };
            let table = &mut self.children[pos_idx];
            table.positions[code as usize] = (0, 0);
            table.children[code as usize] = Some(new_pos_idx);

            return self.build_table(new_pos_idx, positions, 0, 0, depth, max_depth);
        }

        let b0 = self.build_table(pos_idx, positions, code, bits + 1, depth + 1, max_depth - 1);
        let b1 = self.build_table(
            pos_idx,
            &positions[b0..],
            (1u16 << bits) | code,
            bits + 1,
            depth + 1,
            max_depth - 1,
        );
        return b0 + b1;
    }
}

impl Position {
    fn with_bitlen(bitlen: u8) -> Self {
        let size = (1u16 << bitlen) as usize;
        let positions = vec![(0, 0); size];
        let mut children = Vec::with_capacity(size);
        children.resize_with(size, Default::default);
        Self {
            bitlen,
            positions,
            children,
        }
    }
}
