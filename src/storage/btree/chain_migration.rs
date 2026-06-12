//! Atomic migration of per-key MVCC delta chains across a structural mutation.
//!
//! ## Why chains must migrate with their cells
//!
//! A leaf frame owns the live MVCC version chain for each key it holds, and a
//! reader locates a key's chain by descending the tree on the parent-separator
//! routing — i.e. it reaches the chain only via the leaf page the separators
//! say owns that key. When a structural mutation moves cells between pages (a
//! leaf split partitions cells into new siblings; a merge folds one leaf's
//! cells into another; a redistribute repartitions a sibling pair across the
//! separator), the separator routing for some keys now points at a DIFFERENT
//! page. If the delta chains did not move with the cells, a reader would route
//! to the new owning page and find no chain — silently losing every version
//! that was only in the chain (uncheckpointed history), or a writer's
//! `chains_empty` guard in the merge path would refuse to free a leaf whose
//! chains were stranded. So every helper here drains the source frame's chains
//! and re-homes each one onto whichever destination page the post-mutation
//! separators route its key to, in the same structural step that moves the
//! cells.
//!
//! ## Exclusivity precondition (hard)
//!
//! These helpers take `&mut self` and mutate pages other than the caller's
//! target leaf. Per the structural-mutation exclusivity contract documented on
//! [`super::BTree`], they are only safe under engine-level exclusivity over the
//! entire affected root-to-leaf path (the SMO-latch set), NOT under a single
//! per-page latch. Calling them while only a per-leaf latch is held races
//! readers the page latch was never designed to exclude.

use crate::error::Result;
use crate::storage::buffer_pool::LatchMode;

use super::store::BTreePageStore;
use super::BTree;

impl<S: BTreePageStore> BTree<S> {
    /// Route every resident delta chain on `left_page` onto the post-split
    /// destination pages.
    ///
    /// `pages[0]` is the original (left) leaf and `pages[1..]` are the newly
    /// allocated right siblings; `separators[i]` is the first key of
    /// `pages[i + 1]`. Each chain goes to the last page whose separator is
    /// `<= key` (a key below the first separator stays on `pages[0]`). The
    /// drain + re-home pair must run atomically with the leaf-page split that
    /// produced `pages`/`separators`.
    pub(super) fn partition_chains_for_split(
        &mut self,
        left_page: u32,
        pages: &[u32],
        separators: &[Vec<u8>],
    ) -> Result<()> {
        let all_chains: Vec<_> =
            self.store
                .with_all_chains_under_latch(left_page, LatchMode::Exclusive, |chains| {
                    std::mem::take(chains).into_iter().collect()
                })?;
        #[cfg(any(test, feature = "test-hooks"))]
        {
            crate::storage::close_quadratic_probe::record_chain_drain_calls(1);
            crate::storage::close_quadratic_probe::record_chain_drain_entries(
                all_chains.len() as u64,
            );
            crate::storage::close_quadratic_probe::record_chain_rehome_ops(all_chains.len() as u64);
        }
        for (key, chain) in all_chains {
            let gi = separators.partition_point(|sep| sep.as_slice() <= key.as_slice());
            self.store
                .with_chain_under_latch(pages[gi], &key, LatchMode::Exclusive, |slot| {
                    *slot = Some(chain);
                })?;
        }
        Ok(())
    }

    /// Drain every chain from `from_page` and re-home it onto `to_page`.
    ///
    /// Used by the leaf-merge path: merge folds all of `from_page`'s cells into
    /// `to_page`, so all of its chains (including delta-only chains with no base
    /// cell) must follow before `from_page` is freed.
    pub(super) fn move_all_leaf_chains(&mut self, from_page: u32, to_page: u32) -> Result<()> {
        let drained: Vec<_> =
            self.store
                .with_all_chains_under_latch(from_page, LatchMode::Exclusive, |c| {
                    std::mem::take(c).into_iter().collect()
                })?;
        #[cfg(any(test, feature = "test-hooks"))]
        {
            crate::storage::close_quadratic_probe::record_chain_drain_calls(1);
            crate::storage::close_quadratic_probe::record_chain_drain_entries(drained.len() as u64);
            crate::storage::close_quadratic_probe::record_chain_rehome_ops(drained.len() as u64);
        }
        for (key, chain) in drained {
            self.store
                .with_chain_under_latch(to_page, &key, LatchMode::Exclusive, |slot| {
                    *slot = Some(chain);
                })?;
        }
        Ok(())
    }

    /// Re-partition the chains of a sibling pair across `separator_key`.
    ///
    /// Used by the leaf-redistribute path: after cells are repartitioned so the
    /// boundary moves to `separator_key`, every chain (drained from both pages)
    /// routes by the same raw key bytes — `< separator_key` to `left_page`,
    /// otherwise to `right_page` — so delta-only chains track the same boundary
    /// as base-backed chains.
    pub(super) fn redistribute_leaf_chains(
        &mut self,
        left_page: u32,
        right_page: u32,
        separator_key: &[u8],
    ) -> Result<()> {
        let mut chains: Vec<_> =
            self.store
                .with_all_chains_under_latch(left_page, LatchMode::Exclusive, |c| {
                    std::mem::take(c).into_iter().collect()
                })?;
        chains.extend(self.store.with_all_chains_under_latch(
            right_page,
            LatchMode::Exclusive,
            |c| std::mem::take(c).into_iter().collect::<Vec<_>>(),
        )?);

        #[cfg(any(test, feature = "test-hooks"))]
        {
            // Two drain calls (left + right), entries summed.
            crate::storage::close_quadratic_probe::record_chain_drain_calls(2);
            crate::storage::close_quadratic_probe::record_chain_drain_entries(chains.len() as u64);
            crate::storage::close_quadratic_probe::record_chain_rehome_ops(chains.len() as u64);
        }

        for (key, chain) in chains {
            if key.as_slice() < separator_key {
                self.store.with_chain_under_latch(
                    left_page,
                    &key,
                    LatchMode::Exclusive,
                    |slot| {
                        *slot = Some(chain);
                    },
                )?;
            } else {
                self.store.with_chain_under_latch(
                    right_page,
                    &key,
                    LatchMode::Exclusive,
                    |slot| {
                        *slot = Some(chain);
                    },
                )?;
            }
        }

        Ok(())
    }
}
