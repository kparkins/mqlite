# macOS sample — 204104 leaf self-time samples

## Top 30 self-time frames (overall)

   pct     count   sync?  symbol
----------------------------------------------------------------------------------------------------
47.27%     96478          mqlite::client::collection::Collection$LT$T$GT$::insert_one::hbe8147645a875ed5
13.51%     27571          _pthread_cond_wait
 7.76%     15834  [SYNC]  parking_lot::raw_rwlock::RawRwLock::lock_exclusive_slow::h721072fa4b777956
 7.29%     14872  [SYNC]  __psynch_cvwait
 7.23%     14751          mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::get_mvcc::h186c204df3a260d8
 6.49%     13244          alloc::sync::Arc$LT$T$C$A$GT$::make_mut::h63fb80134f1dc83b
 5.43%     11092          mqlite::storage::buffer_pool::BufferPool::pin_then_latch::h5763fd41eec2d8f8
 2.46%      5018          mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::read_leaf_for_point_key::hfa851b671c9e9464
 1.62%      3311  [SYNC]  parking_lot::raw_rwlock::RawRwLock::lock_shared_slow::hfd5b8cc16367bcba
 0.23%       477  [SYNC]  parking_lot::raw_mutex::RawMutex::unlock_slow::h3745a9740d132785
 0.11%       216          _$LT$std..fs..File$u20$as$u20$mqlite..journal..log_file..PositionedLogIo$GT$::sync_data::ha5ac80fb67c9519a
 0.11%       216          fcntl
 0.11%       216          __fcntl
 0.04%        78          _$LT$mqlite..storage..paged_engine..snapshot_ops..PrimaryHistoryProbe$LT$S$GT$$u20$as$u20$mqlite..storage..btree..HistoryProbe$GT$::probe_visible_version::h5192f2d5885cfb92
 0.02%        44          cthread_yield
 0.02%        44          pthread_cond_signal
 0.02%        42          bson::ser::serde::_$LT$impl$u20$serde_core..ser..Serialize$u20$for$u20$bson..document..Document$GT$::serialize::h9b76644fc3fbdb82
 0.02%        39          core::ptr::drop_in_place$LT$core..option..Option$LT$mqlite..storage..buffer_pool..LatchHoldRecorder$GT$$GT$::hd04e62225c15f127
 0.02%        31          swtch_pri
 0.01%        25          mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::read_leaf_for_key::h2451b65961ea4fbf
 0.01%        22  [SYNC]  parking_lot::raw_rwlock::RawRwLock::unlock_exclusive_slow::h6d1589c815d622c4
 0.01%        20          _xzm_xzone_malloc_tiny
 0.01%        19          mqlite::validation::validate_document::hc5323dfe1bb8e66d
 0.01%        16          core::ops::function::FnOnce::call_once$u7b$$u7b$vtable.shim$u7d$$u7d$::h0293fa72079f1e8e
 0.01%        16  [SYNC]  __psynch_cvsignal
 0.01%        15          alloc::collections::btree::map::BTreeMap$LT$K$C$V$C$A$GT$::insert::h155f3764acb3b1a8
 0.01%        14          mqlite::storage::buffer_pool::BufferPool::detect_page_size::hece9ec718f4a323b
 0.01%        13          mqlite::storage::buffer_pool::BufferPool::unpin_internal::h18e68c12838d94e5
 0.01%        12          _realloc
 0.01%        12          _xzm_free

## Sync primitives: 34621 samples (16.96% of leaf samples)
## mqlite-prefixed: 127796 samples (62.61% of leaf samples)

## Top 25 mqlite leaf frames

   pct     count  symbol
----------------------------------------------------------------------------------------------------
47.27%     96478  mqlite::client::collection::Collection$LT$T$GT$::insert_one::hbe8147645a875ed5
 7.23%     14751  mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::get_mvcc::h186c204df3a260d8
 5.43%     11092  mqlite::storage::buffer_pool::BufferPool::pin_then_latch::h5763fd41eec2d8f8
 2.46%      5018  mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::read_leaf_for_point_key::hfa851b671c9e9464
 0.11%       216  _$LT$std..fs..File$u20$as$u20$mqlite..journal..log_file..PositionedLogIo$GT$::sync_data::ha5ac80fb67c9519a
 0.04%        78  _$LT$mqlite..storage..paged_engine..snapshot_ops..PrimaryHistoryProbe$LT$S$GT$$u20$as$u20$mqlite..storage..btree..HistoryProbe$GT$::probe_visible_version::h5192f2d5885cfb92
 0.02%        39  core::ptr::drop_in_place$LT$core..option..Option$LT$mqlite..storage..buffer_pool..LatchHoldRecorder$GT$$GT$::hd04e62225c15f127
 0.01%        25  mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::read_leaf_for_key::h2451b65961ea4fbf
 0.01%        19  mqlite::validation::validate_document::hc5323dfe1bb8e66d
 0.01%        14  mqlite::storage::buffer_pool::BufferPool::detect_page_size::hece9ec718f4a323b
 0.01%        13  mqlite::storage::buffer_pool::BufferPool::unpin_internal::h18e68c12838d94e5
 0.01%        12  mqlite::storage::paged_engine::publish::rebuild_and_publish::h5a09a7173f7510bd
 0.00%         8  _$LT$mqlite..storage..btree_store..BufferPoolPageStore$u20$as$u20$mqlite..storage..btree..BTreePageStore$GT$::read_leaf_guarded::h3e3a6c7f57d00636
 0.00%         7  mqlite::storage::buffer_pool::partition::Partition::pin_page::he4888bb7a3f8d953
 0.00%         6  mqlite::storage::paged_engine::smo_latch::required_pages_for_shapes::h71813a432bdf6e12
 0.00%         5  mqlite::storage::btree::scan::_$LT$impl$u20$mqlite..storage..btree..BTree$LT$S$GT$$GT$::path_to_leaf::h192d6912b78a16d1
 0.00%         4  mqlite::validation::validate_doc_recursive::h739b54d0f44401a9
 0.00%         3  mqlite::storage::buffer_pool::BufferPool::pin::h81f811026cc03ae6
 0.00%         3  mqlite::storage::btree::node::LeafNode::parse::h0d428415f5b229a1
 0.00%         3  mqlite::storage::paged_engine::doc_helpers::ensure_id::haf244dfc95e79484
 0.00%         2  mqlite::storage::paged_engine::smo_latch::classify_leaf_bytes::h3cee54988fbc8932
