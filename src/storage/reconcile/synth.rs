//! Reconcile page synthesis.
//!
//! This module folds checkpoint-visible version-chain entries into a new leaf
//! base image and returns the side effects a later checkpoint driver must
//! install atomically.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::storage::btree::reconcile::{
    decode_folded_leaf, encode_folded_leaf, predict_encoded_leaf_size, FoldedLeafCell,
    RetainedChains,
};
use crate::storage::btree::OVERFLOW_THRESHOLD;
use crate::storage::page::PAGE_SIZE_LEAF;

use super::plan::TreeIdent;

const LEAF_PAGE_BUDGET: usize = PAGE_SIZE_LEAF as usize;

/// Result of synthesizing one replacement folded leaf image.
#[derive(Debug)]
pub(crate) struct PageSynthesisResult {
    /// Replacement folded base page bytes.
    pub(crate) new_base: Vec<u8>,
    /// Committed historical versions that must be durable before install.
    pub(crate) history_spill: Vec<HistorySpillEntry>,
    /// Version-chain entries that remain attached to the frame after install.
    pub(crate) retained_chains: RetainedChains,
}

/// One committed version that synthesis must spill to the history store.
#[derive(Clone, Debug)]
pub(crate) struct HistorySpillEntry {
    /// Tree that owns the version.
    pub(crate) ident: TreeIdent,
    /// User key for the spilled version.
    pub(crate) key: Vec<u8>,
    /// Version entry to write into the history store.
    pub(crate) entry: VersionEntry,
    /// Stable per-key ordinal for duplicate `(key, start_ts)` spill retries.
    pub(crate) counter: u32,
}

/// Reason a synthesized page cannot be installed in place.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotInstallable {
    /// Checkpoint-visible base cells alone do not fit in one leaf page.
    VisibleWinnerExceedsPageBudget,
    /// The final folded page plus retained sidecar budget does not fit.
    FoldedLeafExceedsPageByteBudget,
}

/// Synthesize a folded leaf image using the full-timestamp rules.
///
/// # Errors
///
/// Returns [`NotInstallable::VisibleWinnerExceedsPageBudget`] when the
/// checkpoint-visible base alone exceeds the leaf budget. Returns
/// [`NotInstallable::FoldedLeafExceedsPageByteBudget`] when retained sidecar
/// payloads or final encoding make the folded page non-installable.
pub(crate) fn synthesize_page(
    base_image: &[u8],
    chains: &BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
    checkpoint_ts: Ts,
    oldest_required_ts: Ts,
    ident: TreeIdent,
) -> Result<PageSynthesisResult, NotInstallable> {
    let decoded = decode_folded_leaf(base_image)
        .map_err(|_| NotInstallable::FoldedLeafExceedsPageByteBudget)?;
    let links = decoded.links;
    let mut base_cells: BTreeMap<Vec<u8>, FoldedLeafCell> = decoded
        .cells
        .into_iter()
        .map(|cell| (cell.key.clone(), cell))
        .collect();
    let mut retained_chains = RetainedChains::new();
    let mut history_spill = Vec::new();

    for (key, chain) in chains {
        let winner_index = checkpoint_winner_index(chain, checkpoint_ts);
        if let Some(index) = winner_index {
            base_cells.remove(key);
            if !chain[index].is_tombstone {
                let cell = folded_cell_from_entry(key, &chain[index])?;
                base_cells.insert(key.clone(), cell);
            }
        }

        let mut retained = VecDeque::new();
        for (index, entry) in chain.iter().enumerate() {
            if Some(index) == winner_index {
                continue;
            }

            match entry.state {
                VersionState::Committed if entry.start_ts > checkpoint_ts => {
                    retained.push_back(entry.clone());
                }
                VersionState::Committed if entry.stop_ts <= oldest_required_ts => {}
                VersionState::Committed => {
                    history_spill.push(HistorySpillEntry {
                        ident: ident.clone(),
                        key: key.clone(),
                        entry: entry.clone(),
                        counter: stable_history_counter(chain, index, entry.start_ts)?,
                    });
                }
                VersionState::Pending { .. } => retained.push_back(entry.clone()),
                VersionState::Aborted => {}
            }
        }

        if !retained.is_empty() {
            retained_chains.insert(key.clone(), Arc::new(retained));
        }
    }

    let folded_cells: Vec<FoldedLeafCell> = base_cells.into_values().collect();
    if predict_encoded_leaf_size(&folded_cells, &RetainedChains::new()) > LEAF_PAGE_BUDGET {
        return Err(NotInstallable::VisibleWinnerExceedsPageBudget);
    }
    if predict_encoded_leaf_size(&folded_cells, &retained_chains) > LEAF_PAGE_BUDGET {
        return Err(NotInstallable::FoldedLeafExceedsPageByteBudget);
    }

    let new_base = encode_folded_leaf(&folded_cells, links)
        .map_err(|_| NotInstallable::FoldedLeafExceedsPageByteBudget)?;

    Ok(PageSynthesisResult {
        new_base,
        history_spill,
        retained_chains,
    })
}

/// Return true when checkpoint-visible winners can each fit after B-tree
/// materialization is allowed to split leaf contents and spill large values.
///
/// # Errors
///
/// Returns [`NotInstallable`] when a visible winner cannot be converted into a
/// folded cell for budget checking.
pub(crate) fn visible_winners_fit_individual_leaf_pages(
    chains: &BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
    checkpoint_ts: Ts,
) -> Result<bool, NotInstallable> {
    let mut visible_winners = 0usize;
    for (key, chain) in chains {
        let Some(index) = checkpoint_winner_index(chain, checkpoint_ts) else {
            return Ok(false);
        };
        if chain.len() != 1 {
            return Ok(false);
        }
        let entry = &chain[index];
        if entry.is_tombstone {
            continue;
        }
        visible_winners += 1;
        let cell = match &entry.data {
            VersionData::Inline(bytes) if bytes.len() > OVERFLOW_THRESHOLD => {
                let total_length = u32::try_from(bytes.len())
                    .map_err(|_| NotInstallable::FoldedLeafExceedsPageByteBudget)?;
                FoldedLeafCell::overflow(key.to_vec(), 0, total_length)
            }
            _ => folded_cell_from_entry(key, entry)?,
        };
        if predict_encoded_leaf_size(&[cell], &RetainedChains::new()) > LEAF_PAGE_BUDGET {
            return Ok(false);
        }
    }
    Ok(visible_winners > 1)
}

fn checkpoint_winner_index(chain: &VecDeque<VersionEntry>, checkpoint_ts: Ts) -> Option<usize> {
    chain
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            matches!(entry.state, VersionState::Committed) && entry.start_ts <= checkpoint_ts
        })
        .max_by_key(|(_, entry)| entry.start_ts)
        .map(|(index, _)| index)
}

fn stable_history_counter(
    chain: &VecDeque<VersionEntry>,
    index: usize,
    start_ts: Ts,
) -> Result<u32, NotInstallable> {
    let prior_same_start = chain
        .iter()
        .take(index)
        .filter(|entry| {
            matches!(entry.state, VersionState::Committed) && entry.start_ts == start_ts
        })
        .count();
    u32::try_from(prior_same_start).map_err(|_| NotInstallable::FoldedLeafExceedsPageByteBudget)
}

fn folded_cell_from_entry(
    key: &[u8],
    entry: &VersionEntry,
) -> Result<FoldedLeafCell, NotInstallable> {
    match &entry.data {
        VersionData::Inline(bytes) => Ok(FoldedLeafCell::inline(key.to_vec(), bytes.clone())),
        VersionData::Overflow(overflow) => {
            let total_length = u32::try_from(overflow.total_length())
                .map_err(|_| NotInstallable::FoldedLeafExceedsPageByteBudget)?;
            Ok(FoldedLeafCell::overflow(
                key.to_vec(),
                overflow.first_page(),
                total_length,
            ))
        }
    }
}
