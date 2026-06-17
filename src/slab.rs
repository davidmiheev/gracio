//! `Slab<T>` — a dense, vector-backed key/value store.
//!
//! A slab hands out a monotonically increasing `usize` "key" for each
//! inserted value. Internally the data lives in a `Vec<Option<T>>`, so
//! lookups are a single bounds-checked index — **O(1) with great cache
//! behavior** and no hashing.
//!
//! Removed slots become empty (`None`) but their position is preserved
//! (the vector never shrinks). Insertion always grows the vector, so the
//! slab key for a given entry never changes for the lifetime of the
//! process. This is the same shape as `tokio::task::JoinSet` and
//! `mio`'s token tables — just hand-rolled so the runtime has **zero
//! external dependencies**.
//!
//! ## Why a slab instead of a `HashMap`?
//!
//! Gracio's task ids are produced by a monotonic counter, so they are
//! dense and roughly sequential. For this access pattern a `HashMap`
//! wastes time hashing each lookup. A `Vec<Option<T>>` is:
//!
//! - **Faster** — single index + bounds check, no hash, no probe sequence.
//! - **Cache-friendly** — entries live next to each other in memory.
//! - **Cheaper to iterate** — `iter()` walks a contiguous buffer.
//! - **Predictable** — no rehash pauses, allocation pattern is `Vec::push`.
//!
//! ## Trade-off
//!
//! The vector grows monotonically: removed slots are tombstones. For a
//! long-running runtime that spawns millions of tasks, a real implementation
//! would add generation counters or periodic compaction. For Gracio's
//! teaching scope this is fine — the runtime is short-lived by design.
//!
//! ## Stale keys
//!
//! Because keys are never reused, a stale wake for a removed key simply
//! resolves to an empty slot and is silently ignored by the executor.

use std::ops;

// ---------------------------------------------------------------------------
// Slab
// ---------------------------------------------------------------------------

/// A dense, vector-backed key→value store.
#[derive(Debug)]
pub(crate) struct Slab<T> {
    entries: Vec<Entry<T>>,
}

/// One slot in the slab.  `None` means the slot is free.
#[derive(Debug)]
struct Entry<T> {
    value: Option<T>,
}

impl<T> Slab<T> {
    /// Construct an empty slab.
    pub const fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Number of live (non-empty) entries.
    #[allow(dead_code)] // exposed as part of the slab API
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|e| e.value.is_some()).count()
    }

    /// True if there are no live entries.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert `value`, returning a key that can be used to look it up.
    ///
    /// Always grows the vector by one and returns the new last index. Keys
    /// are monotonically increasing and **never reused**, even after removal.
    /// This guarantees that an old key cannot accidentally resolve to a
    /// different value than it did originally — see the module docs.
    #[allow(dead_code)] // exposed as part of the general slab API
    pub fn insert(&mut self, value: T) -> usize {
        let key = self.entries.len();
        self.entries.push(Entry { value: Some(value) });
        key
    }

    /// Insert `value` at the given `key`, overwriting any existing entry
    /// there and growing the vector with empty slots as needed.
    ///
    /// This lets callers preserve a stable key across remove/re-insert
    /// cycles — useful when an external counter hands out ids and the slab
    /// is just the backing store.
    pub fn insert_at(&mut self, key: usize, value: T) {
        while self.entries.len() <= key {
            self.entries.push(Entry { value: None });
        }
        self.entries[key].value = Some(value);
    }

    /// Remove the entry for `key`, returning the owned value.
    ///
    /// Returns `None` if the key is out of range or the slot is already
    /// empty (i.e. the entry was already removed).
    pub fn remove(&mut self, key: usize) -> Option<T> {
        self.entries.get_mut(key).and_then(|e| e.value.take())
    }

    /// Borrow the entry for `key`.
    pub fn get(&self, key: usize) -> Option<&T> {
        self.entries.get(key).and_then(|e| e.value.as_ref())
    }

    /// Mutably borrow the entry for `key`.
    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        self.entries.get_mut(key).and_then(|e| e.value.as_mut())
    }

    /// True if a live entry exists at `key`.
    #[allow(dead_code)]
    pub fn contains(&self, key: usize) -> bool {
        self.get(key).is_some()
    }

    /// Iterate over `(key, &value)` pairs for every live entry.
    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.value.as_ref().map(|v| (i, v)))
    }

    /// Iterate over `(key, &mut value)` pairs for every live entry.
    #[allow(dead_code)]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut T)> {
        self.entries
            .iter_mut()
            .enumerate()
            .filter_map(|(i, e)| e.value.as_mut().map(|v| (i, v)))
    }

    /// Drop every entry, leaving the slab empty.
    pub fn clear(&mut self) {
        // `clear()` keeps the allocation; we fully drop the values.
        for entry in &mut self.entries {
            entry.value = None;
        }
    }

    /// Total number of slots (live + free).
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// `Slab<T>` can be indexed by `usize` for read-only access.
///
/// Panics if the key is out of range or the slot is empty.
impl<T> ops::Index<usize> for Slab<T> {
    type Output = T;

    fn index(&self, key: usize) -> &Self::Output {
        self.get(key)
            .unwrap_or_else(|| panic!("slab: no live entry at key {key}"))
    }
}

/// `Slab<T>` can be indexed by `usize` for mutable access.
///
/// Panics if the key is out of range or the slot is empty.
impl<T> ops::IndexMut<usize> for Slab<T> {
    fn index_mut(&mut self, key: usize) -> &mut Self::Output {
        let len = self.entries.len();
        self.get_mut(key)
            .unwrap_or_else(|| panic!("slab: no live entry at key {key} (capacity {len})"))
    }
}

/// Drop the slab — `T::drop` runs for every live entry.
impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        // Explicit loop so values are dropped *before* the Vec's allocation
        // is freed — important for `T: !Copy` types that hold `Rc`s.
        for entry in &mut self.entries {
            entry.value = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut s: Slab<&'static str> = Slab::new();
        let k = s.insert("hello");
        assert_eq!(s[k], "hello");
        assert_eq!(s.get(k), Some(&"hello"));
    }

    #[test]
    fn multiple_inserts_get_distinct_keys() {
        let mut s: Slab<i32> = Slab::new();
        let a = s.insert(1);
        let b = s.insert(2);
        let c = s.insert(3);
        assert_eq!(s[a], 1);
        assert_eq!(s[b], 2);
        assert_eq!(s[c], 3);
    }

    #[test]
    fn remove_returns_value_and_frees_slot() {
        let mut s: Slab<i32> = Slab::new();
        let a = s.insert(10);
        let b = s.insert(20);
        assert_eq!(s.remove(a), Some(10));
        assert!(s.get(a).is_none());
        assert_eq!(s.get(b), Some(&20));
    }

    #[test]
    fn keys_are_monotonic_and_never_reused() {
        let mut s: Slab<i32> = Slab::new();
        let a = s.insert(1);
        let b = s.insert(2);
        s.remove(a);
        let c = s.insert(3); // must NOT reuse slot `a`
        assert_eq!(a, 0);
        assert_eq!(b, 1);
        assert_eq!(c, 2);
        assert_eq!(s[c], 3);
        assert_eq!(s[b], 2);
    }

    #[test]
    fn removing_does_not_shift_other_keys() {
        let mut s: Slab<i32> = Slab::new();
        let a = s.insert(10);
        let b = s.insert(20);
        let c = s.insert(30);
        s.remove(b);
        // `a` and `c` must still be reachable at their original keys.
        assert_eq!(s[a], 10);
        assert_eq!(s[c], 30);
    }

    #[test]
    fn insert_at_preserves_key_across_remove_reinsert() {
        let mut s: Slab<&'static str> = Slab::new();
        s.insert_at(7, "alpha");
        s.insert_at(3, "beta");
        // Slots 0..3 are tombstones; slot 7 has "alpha"; slot 3 has "beta".
        assert_eq!(s.get(7), Some(&"alpha"));
        assert_eq!(s.get(3), Some(&"beta"));
        assert!(s.get(0).is_none());

        // Remove and re-insert at the same key.
        assert_eq!(s.remove(7), Some("alpha"));
        assert!(s.get(7).is_none());
        s.insert_at(7, "alpha-2");
        assert_eq!(s.get(7), Some(&"alpha-2"));
        assert_eq!(s[7], "alpha-2");
    }

    #[test]
    fn insert_at_grows_with_tombstones() {
        let mut s: Slab<u32> = Slab::new();
        assert_eq!(s.capacity(), 0);
        s.insert_at(5, 42);
        assert_eq!(s.capacity(), 6);
        assert_eq!(s[5], 42);
        // Intermediate slots are tombstones.
        assert!(s.get(0).is_none());
        assert!(s.get(4).is_none());
    }

    #[test]
    fn remove_missing_returns_none() {
        let mut s: Slab<i32> = Slab::new();
        assert_eq!(s.remove(0), None);
        let k = s.insert(7);
        s.remove(k);
        assert_eq!(s.remove(k), None);
    }

    #[test]
    fn len_and_is_empty() {
        let mut s: Slab<i32> = Slab::new();
        assert!(s.is_empty());
        let a = s.insert(1);
        let b = s.insert(2);
        assert_eq!(s.len(), 2);
        s.remove(a);
        assert_eq!(s.len(), 1);
        s.remove(b);
        assert!(s.is_empty());
    }

    #[test]
    fn iter_yields_live_entries() {
        let mut s: Slab<i32> = Slab::new();
        let a = s.insert(10);
        let _b = s.insert(20);
        let c = s.insert(30);
        s.remove(a);

        // `b` and `c` are live; `a` is a tombstone.
        let collected: Vec<_> = s.iter().collect();
        assert_eq!(collected, vec![(1, &20), (2, &30)]);
        // `c` should still be reachable through the index returned at insert.
        assert_eq!(s[c], 30);
    }

    #[test]
    fn clear_drops_values() {
        use std::cell::Cell;
        use std::rc::Rc;

        let drops = Rc::new(Cell::new(0));
        struct CountsOnDrop(Rc<Cell<u32>>);
        impl Drop for CountsOnDrop {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let mut s: Slab<CountsOnDrop> = Slab::new();
        s.insert(CountsOnDrop(Rc::clone(&drops)));
        s.insert(CountsOnDrop(Rc::clone(&drops)));
        s.insert(CountsOnDrop(Rc::clone(&drops)));
        assert_eq!(drops.get(), 0);
        s.clear();
        assert_eq!(drops.get(), 3);
    }

    #[test]
    fn drop_drops_values() {
        use std::cell::Cell;
        use std::rc::Rc;

        let drops = Rc::new(Cell::new(0));
        struct CountsOnDrop(Rc<Cell<u32>>);
        impl Drop for CountsOnDrop {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        {
            let mut s: Slab<CountsOnDrop> = Slab::new();
            s.insert(CountsOnDrop(Rc::clone(&drops)));
            s.insert(CountsOnDrop(Rc::clone(&drops)));
            assert_eq!(drops.get(), 0);
        }
        assert_eq!(drops.get(), 2);
    }
}
