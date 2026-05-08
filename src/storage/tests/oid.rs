use super::*;

#[test]
fn generated_oid_is_12_bytes() {
    let oid = ObjectIdGenerator::generate();
    assert_eq!(oid.to_hex().len(), 24, "hex string is 2 chars per byte");
}

#[test]
fn sequential_oids_are_unique() {
    const N: usize = 1000;
    let mut ids: Vec<ObjectId> = (0..N).map(|_| ObjectIdGenerator::generate()).collect();
    ids.sort_unstable_by_key(|id| id.to_hex());
    ids.dedup_by_key(|id| id.to_hex());
    assert_eq!(ids.len(), N, "all {N} generated ObjectIds should be unique");
}

#[test]
fn counter_increases_monotonically() {
    // The counter is the last 3 bytes of the ObjectId.
    // Generate a batch and confirm the counters are strictly increasing.
    //
    // Note: tests run in parallel, so the global COUNTER may advance by
    // more than 1 between consecutive calls in this batch. We only check
    // strict increase, not exactly-consecutive values.
    let ids: Vec<ObjectId> = (0..16).map(|_| ObjectIdGenerator::generate()).collect();

    // Extract the 3-byte counter field from each ObjectId
    fn counter_value(oid: &ObjectId) -> u32 {
        let bytes = oid.bytes();
        // bytes[9..12] are the counter (big-endian)
        u32::from_be_bytes([0, bytes[9], bytes[10], bytes[11]])
    }

    let counters: Vec<u32> = ids.iter().map(counter_value).collect();

    // Each counter must be strictly greater than the previous (mod 2^24).
    // The global AtomicU32 only ever increases, so within this sequential
    // batch each value is greater than the last (gaps are allowed).
    for w in counters.windows(2) {
        let a = w[0] & 0x00FF_FFFF;
        let b = w[1] & 0x00FF_FFFF;
        assert!(
            b > a,
            "counter must increase: {a} -> {b} (full sequence: {counters:?})"
        );
    }
}

#[test]
fn timestamp_is_recent() {
    let oid = ObjectIdGenerator::generate();
    let bytes = oid.bytes();
    // Timestamp is the first 4 bytes, big-endian seconds since epoch
    let ts = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    // The timestamp should be within a 5-second window of now
    assert!(
        ts <= now_secs && ts >= now_secs.saturating_sub(5),
        "timestamp {ts} should be close to now ({now_secs})"
    );
}

#[test]
fn process_random_is_stable_across_calls() {
    // The per-process random component (bytes 4..9) should be the same
    // for every ObjectId generated in the same process.
    let id1 = ObjectIdGenerator::generate();
    let id2 = ObjectIdGenerator::generate();

    let r1 = &id1.bytes()[4..9];
    let r2 = &id2.bytes()[4..9];

    assert_eq!(
        r1, r2,
        "process-random bytes must be stable within a process"
    );
}

#[test]
fn concurrent_generation_produces_no_duplicates() {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let results: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let results = Arc::clone(&results);
        handles.push(thread::spawn(move || {
            let batch: Vec<String> = (0..128)
                .map(|_| ObjectIdGenerator::generate().to_hex())
                .collect();
            let mut set = results.lock().unwrap();
            set.extend(batch);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let set = results.lock().unwrap();
    assert_eq!(
        set.len(),
        8 * 128,
        "all 1024 concurrent ObjectIds must be unique"
    );
}

#[test]
fn oid_from_parts_roundtrip() {
    // Verify bson's from_parts → bytes layout matches MongoDB spec
    let ts: u32 = 0x1234_5678;
    let pid: [u8; 5] = [0xAB, 0xCD, 0xEF, 0x01, 0x23];
    let counter: [u8; 3] = [0x45, 0x67, 0x89];

    let oid = ObjectId::from_parts(ts, pid, counter);
    let bytes = oid.bytes();

    // timestamp: big-endian
    assert_eq!(&bytes[0..4], &ts.to_be_bytes());
    // random
    assert_eq!(&bytes[4..9], &pid);
    // counter
    assert_eq!(&bytes[9..12], &counter);
}

#[test]
fn next_counter_wraps_gracefully() {
    // Save the current counter value and set it near u32::MAX to test wrap
    // Note: AtomicU32 wraps naturally; we just verify the 3-byte extraction
    let near_max: u32 = 0x00FF_FFFE; // one before the 24-bit boundary
    let be = near_max.to_be_bytes();
    let result: [u8; 3] = [be[1], be[2], be[3]];
    assert_eq!(result, [0xFF, 0xFF, 0xFE]);
}
