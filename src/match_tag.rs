use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) struct MatchTagPool {
    avail_tags: BTreeSet<u32>,
    max_tag: u32,
}

impl MatchTagPool {
    pub fn new(size: u32) -> Self {
        MatchTagPool {
            avail_tags: BTreeSet::from_iter(1..=size),
            max_tag: size,
        }
    }
    fn resize_pool(&mut self, new_size: u32) {
        if new_size <= self.max_tag {
            return; // We cannot make the pool any smaller, sorry
        }

        self.avail_tags.extend(self.max_tag + 1..=new_size);
        self.max_tag = new_size;
    }
    pub fn alloc_tag(&mut self) -> u32 {
        if self.avail_tags.is_empty() {
            let new_size = self.max_tag.saturating_mul(2);
            self.resize_pool(new_size);
        }

        self.avail_tags
            .take(&self.avail_tags.first().copied().unwrap())
            .unwrap()
    }
    pub fn free_tag(&mut self, tag: u32) {
        if tag == 0 || tag > self.max_tag {
            panic!("Trying to free invalid match tag");
        }

        if !self.avail_tags.insert(tag) {
            panic!("Double free of match tag {}", tag);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_in_order() {
        let mut pool = MatchTagPool::new(3);

        assert_eq!(pool.alloc_tag(), 1);
        assert_eq!(pool.alloc_tag(), 2);
        assert_eq!(pool.alloc_tag(), 3);
    }

    #[test]
    fn reuses_freed_tag() {
        let mut pool = MatchTagPool::new(3);

        let t1 = pool.alloc_tag();
        let t2 = pool.alloc_tag();

        pool.free_tag(t1);
        pool.free_tag(t2);

        // Since this is a BTreeSet, it should return the smallest available
        assert_eq!(pool.alloc_tag(), t1);
        assert_eq!(pool.alloc_tag(), t2);
    }

    #[test]
    fn grows_when_exhausted() {
        let mut pool = MatchTagPool::new(2);

        assert_eq!(pool.alloc_tag(), 1);
        assert_eq!(pool.alloc_tag(), 2);

        // Should trigger resize to 4
        let t3 = pool.alloc_tag();
        let t4 = pool.alloc_tag();

        assert_eq!(t3, 3);
        assert_eq!(t4, 4);
    }

    #[test]
    fn grows_multiple_times() {
        let mut pool = MatchTagPool::new(1);

        let mut tags = Vec::new();
        for _ in 0..10 {
            tags.push(pool.alloc_tag());
        }

        // Should allocate sequentially
        assert_eq!(tags, (1..=10).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic(expected = "Double free")]
    fn double_free_panics() {
        let mut pool = MatchTagPool::new(2);

        let t = pool.alloc_tag();
        pool.free_tag(t);
        pool.free_tag(t); // should panic
    }

    #[test]
    #[should_panic(expected = "invalid match tag")]
    fn freeing_zero_panics() {
        let mut pool = MatchTagPool::new(2);
        pool.free_tag(0);
    }

    #[test]
    #[should_panic(expected = "invalid match tag")]
    fn freeing_out_of_range_panics() {
        let mut pool = MatchTagPool::new(2);
        pool.free_tag(999);
    }

    #[test]
    fn free_and_allocate_many_times() {
        let mut pool = MatchTagPool::new(5);

        let mut allocated = Vec::new();
        for _ in 0..5 {
            allocated.push(pool.alloc_tag());
        }

        // Free all
        for tag in &allocated {
            pool.free_tag(*tag);
        }

        // Should reallocate in sorted order
        for expected in 1..=5 {
            assert_eq!(pool.alloc_tag(), expected);
        }
    }
}
