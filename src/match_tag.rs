pub(crate) struct MatchTagPool {
    bitset: Vec<u32>,
}

impl MatchTagPool {
    pub fn new(size: u32) -> MatchTagPool {
        MatchTagPool {
            bitset: vec![u32::MAX; size as usize],
        }
    }
    pub fn alloc_tag(&mut self) -> u32 {
        let mut vidx = 0;
        loop {
            if self.bitset.len() <= vidx {
                self.bitset.push(u32::MAX);
            }
            if let Some(new_tag) = self.bitset[vidx].lowest_one() {
                // set to zero -> tag used
                self.bitset[vidx] &= !(1 << new_tag);
                return (vidx as u32 * 32 + new_tag) + 1;
            }
            vidx += 1;
        }
    }
    pub fn free_tag(&mut self, tag: u32) {
        let tag = tag.checked_sub(1).expect("invalid match tag");
        let vidx = tag.div_euclid(32);
        let sub_tag = tag.rem_euclid(32);
        let elem = self
            .bitset
            .get_mut(vidx as usize)
            .expect("invalid match tag");
        let bit = 1 << sub_tag;
        if *elem & bit != 0 {
            // the tag is already freed, panic
            panic!("double free");
        }
        *elem |= bit;
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

        assert_eq!(pool.alloc_tag(), t1);
        assert_eq!(pool.alloc_tag(), t2);
    }

    #[test]
    fn grows_when_exhausted() {
        let mut pool = MatchTagPool::new(1);

        for t1 in 1..=32 {
            assert_eq!(pool.alloc_tag(), t1);
        }

        // Should trigger resize
        let t2 = pool.alloc_tag();
        let t3 = pool.alloc_tag();

        assert_eq!(t2, 33);
        assert_eq!(t3, 34);
    }

    #[test]
    fn grows_multiple_times() {
        let mut pool = MatchTagPool::new(1);

        let mut tags = Vec::new();
        for _ in 1..=128 {
            tags.push(pool.alloc_tag());
        }

        // Should allocate sequentially
        assert_eq!(tags, (1..=128).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic(expected = "double free")]
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
