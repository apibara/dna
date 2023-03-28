//! Elias-Fano integer compression.
use std::io::Read;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub enum EliasFanoError {
    #[error("failed to read elias fano data from reader")]
    ReadError(#[from] std::io::Error),
}

/// Elias-Fano data structure.
///
/// Based on Erigon's `recsplit/eliasfano32/elias_fano.go`.
pub struct EliasFano {
    count: u64,
    universe: u64,
    lower_bits_mask: u64,
    lower_bits: Vec<u64>,
    upper_bits: Vec<u64>,
    jumps: Vec<u64>,
    l: u64,
}

const LOG2_Q: u64 = 8u64;
const Q: u64 = 1u64 << LOG2_Q;
const Q_MASK: u64 = Q - 1;
const SUPER_Q: u64 = 1u64 << 14;
const SUPER_Q_MASK: u64 = SUPER_Q - 1;
const Q_PER_SUPER_Q: u64 = SUPER_Q / Q;
const SUPER_Q_SIZE: u64 = 1u64 + Q_PER_SUPER_Q / 2;

impl EliasFano {
    pub fn from_reader<R: Read>(cursor: &mut R) -> Result<Self, EliasFanoError> {
        let count = cursor.read_u64::<BigEndian>()?;
        let universe = cursor.read_u64::<BigEndian>()?;

        let (l, lower_bits_mask, words_lower_bits, words_upper_bits, jump_words_size) =
            derive_bit_fields(universe, count);

        trace!(
            count = %count,
            universe = %universe,
            lower_bits_mask = %lower_bits_mask,
            words_lower_bits = %words_lower_bits,
            words_upper_bits = %words_upper_bits,
            jump_words_size = %jump_words_size,
            "read ef params"
        );

        // the original go code re-interprets the remaining data as u64, without
        // any care for endiannes. So when we read the 3 values back we do the same.
        let mut lower_bits = vec![0u64; words_lower_bits as usize];
        cursor.read_u64_into::<LittleEndian>(&mut lower_bits)?;
        let mut upper_bits = vec![0u64; words_upper_bits as usize];
        cursor.read_u64_into::<LittleEndian>(&mut upper_bits)?;
        let mut jumps = vec![0u64; jump_words_size as usize];
        cursor.read_u64_into::<LittleEndian>(&mut jumps)?;

        trace!(
            lower_bits = ?lower_bits,
            upper_bits = ?upper_bits,
            jumps = ?jumps,
            "read ef data"
        );

        let ef = EliasFano::from_raw(count, universe, lower_bits, upper_bits, jumps);

        Ok(ef)
    }

    /// Returns the E-F filter from its raw components.
    pub fn from_raw(
        count: u64,
        universe: u64,
        lower_bits: Vec<u64>,
        upper_bits: Vec<u64>,
        jumps: Vec<u64>,
    ) -> Self {
        let (l, lower_bits_mask, _, _, _) = derive_bit_fields(universe, count);
        Self {
            count,
            universe,
            lower_bits_mask,
            lower_bits,
            upper_bits,
            jumps,
            l,
        }
    }

    /// Returns the value of the `index`-th element.
    pub fn get(&self, index: u64) -> Option<u64> {
        if index > self.count {
            return None;
        }

        let lower = index * self.l;
        let bit_idx = lower / 64;
        let shift = lower % 64;

        let mut lower = self.lower_bits[bit_idx as usize] >> shift;
        if shift > 0 {
            lower |= self.lower_bits[(bit_idx + 1) as usize] << (64 - shift);
        }

        let jump_super_q = (index / SUPER_Q) * SUPER_Q_SIZE;
        let jump_inside_super_q = (index % SUPER_Q) / Q;

        let jump_idx = jump_super_q + 1 + (jump_inside_super_q >> 1);
        let jump_shift = 32 * (jump_inside_super_q % 2);
        let mask = 0xffffffffu64 << jump_shift;
        let jump = self.jumps[jump_super_q as usize] + (self.jumps[jump_idx as usize] & mask)
            >> jump_shift;

        let mut curr_word = jump / 64;
        let mut window = self.upper_bits[curr_word as usize] & (u64::MAX << (jump % 64));
        let mut d = (index & Q_MASK) as i64;

        let mut bit_count = window.count_ones() as i64;
        println!(
            "ef.get cw = {}, w = {} d = {}, bc = {}",
            curr_word, window, d, bit_count
        );
        while bit_count <= d {
            curr_word += 1;
            window = self.upper_bits[curr_word as usize];
            d -= bit_count;
            println!(
                "loop curr = {}, window = {}, d = {}, bc = {}",
                curr_word, window, d, bit_count
            );
            bit_count = window.count_ones() as i64;
        }

        let sel = broadword::select1_raw(d as usize, window) as u64;
        let val = (curr_word * 64 + sel - index) << self.l | (lower & self.lower_bits_mask);

        println!("sel = {}, val = {}", sel, val);
        Some(val)
    }
}

fn derive_bit_fields(universe: u64, count: u64) -> (u64, u64, u64, u64, u64) {
    let l = if (universe / (count + 1)) == 0 {
        0u64
    } else {
        63 ^ ((universe / (count + 1)).leading_zeros() as u64)
    };

    let lower_bits_mask = (1u64 << l) - 1;
    let words_lower_bits = ((count + 1) * l + 63) / 64 + 1;
    let words_upper_bits = (count + 1 + (universe >> l) + 63) / 64;
    let jump_words_size = {
        let size = ((count + 1) / SUPER_Q) * SUPER_Q_SIZE;

        if (count + 1) % SUPER_Q != 0 {
            size + 1 + (((count + 1) % SUPER_Q + Q - 1) / Q + 3) / 2
        } else {
            size
        }
    };

    (
        l,
        lower_bits_mask,
        words_lower_bits,
        words_upper_bits,
        jump_words_size,
    )
}

#[cfg(test)]
mod tests {
    use super::EliasFano;

    #[test]
    pub fn test_elias_fano() {
        let ef = EliasFano::from_raw(
            999,
            9158,
            vec![
                8849889209085810824,
                9870256778185256004,
                2253639274199579334,
                9012018795671148753,
                5094028216370179172,
                4934566538344650292,
                11690976991867900404,
                15068860532787543729,
                2588191152425018346,
                10352764609699806554,
                4517569937967918184,
                2258705804146160034,
                6399931011796261073,
                1883446724368322847,
                15435039551645837291,
                6399931011796256399,
                1883607566675136799,
                2588191152425018347,
                1293773958017444634,
                9705716747965953421,
                12795277828280689640,
                14287623862888051838,
                14481944951392113146,
                4935209927205527384,
                3767214506112782828,
                16952651881124218838,
                18070281165525209505,
                6397639001288394376,
                5092761578369800479,
                5182263415884207668,
                16773887314251513187,
                17693672454321019427,
                15429964048937356561,
                7717520404098500239,
                17693672493522182980,
                15435040808197074193,
                10517422993265870407,
                9870419824960882645,
                15429962931862314840,
                14307849849119729872,
                7071754337406097914,
                2258705803843496189,
                12987651866686122778,
                9117695416260397306,
                2217610477180615348,
                9035140582762380633,
                49999460953074500,
                0,
            ],
            vec![
                5956755242357527893,
                6136905000603527765,
                6145911924978264725,
                10760413058247076517,
                12273809451447046826,
                12296328136782861482,
                12296328136782861642,
                12296328136782861642,
                12297454079640687946,
                12296328136783910226,
                5956737650708337994,
                5380294490590979413,
                12297735556764964181,
                3074433889191236948,
                6148163999669855573,
                11913334558559611561,
                12297454071050491178,
                5380294490590975316,
                6148727018334366037,
                6148164085571955370,
                12201751857056787113,
                5379925009048622378,
                6136904725681580373,
                6148163999669856853,
                6148727035525376681,
                12201705677568420522,
                12297454071049442474,
                6136903625498776914,
                12273807252339894953,
                3074363509167183018,
                6145911924936316245,
                12201751857325222570,
                12273810001207055530,
                2862961994,
            ],
            vec![0, 2353642078208, 7065221203016, 0],
        );

        assert_eq!(ef.get(0), Some(0));
        assert_eq!(ef.get(50), Some(458));
        assert_eq!(ef.get(100), Some(913));
        assert_eq!(ef.get(150), Some(1372));
        assert_eq!(ef.get(200), Some(1828));
        assert_eq!(ef.get(700), Some(6402));
        assert_eq!(ef.get(800), Some(7320));
        assert_eq!(ef.get(999), Some(9157));
    }
}
