use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use super::store;

pub fn convert_bitmap_map<K, KS>(
    map: BTreeMap<K, RoaringBitmap>,
) -> Result<BTreeMap<KS, store::Bitmap>, std::io::Error>
where
    K: Into<KS>,
    KS: Ord,
{
    map.into_iter()
        .map(|(key, bitmap)| {
            let key: KS = key.into();
            let bitmap = store::Bitmap::try_from(bitmap)?;
            Ok((key, bitmap))
        })
        .collect()
}

impl TryFrom<RoaringBitmap> for store::Bitmap {
    type Error = std::io::Error;

    fn try_from(bitmap: RoaringBitmap) -> Result<store::Bitmap, std::io::Error> {
        let mut buf = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut buf)?;
        Ok(store::Bitmap(buf))
    }
}
