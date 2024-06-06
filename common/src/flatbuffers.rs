use flatbuffers::Follow;

pub trait VectorExt<'a, T>
where
    T: Follow<'a> + 'a,
{
    fn binary_search_by_key<K, F>(&self, key: &K, f: F) -> Option<<T as Follow<'a>>::Inner>
    where
        F: Fn(&<T as Follow<'a>>::Inner) -> Option<&'a K>,
        K: Ord + 'a;
}

impl<'a, T> VectorExt<'a, T> for flatbuffers::Vector<'a, T>
where
    T: Follow<'a> + 'a,
{
    fn binary_search_by_key<K, F>(&self, key: &K, f: F) -> Option<<T as Follow<'a>>::Inner>
    where
        F: Fn(&<T as Follow<'a>>::Inner) -> Option<&'a K>,
        K: Ord + 'a,
    {
        use std::cmp::Ordering::*;

        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let el = self.get(mid);
            let mid_key = f(&el).expect("key must be present");

            let cmp = mid_key.cmp(key);
            if cmp == Less {
                left = mid + 1;
            } else if cmp == Greater {
                right = mid;
            } else {
                return Some(el);
            }

            size = right - left;
        }

        None
    }
}
