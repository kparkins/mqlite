use super::*;

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use bson::doc;

use crate::error::{EngineFatalReason, Error, Result};
use crate::options::FindOptions;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS_A: &str = "phase7.us002.a";
const NS_B: &str = "phase7.us002.b";
const NS_C: &str = "phase7.us002.c";
const GATE_TIMEOUT: Duration = Duration::from_secs(5);
const SHORT_WAIT: Duration = Duration::from_millis(100);

fn buffered_engine() -> Result<PagedEngine> {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, 0, 0)
}

fn create_namespaces(engine: &PagedEngine, namespaces: &[&str]) -> Result<()> {
    for ns in namespaces {
        engine.create_namespace(ns)?;
    }
    Ok(())
}

#[test]
fn test_checkpoint_gate_blocks_new_writers_and_drains_all_namespaces() -> Result<()> {
    let engine = Arc::new(buffered_engine()?);
    create_namespaces(&engine, &[NS_A, NS_B, NS_C])?;

    let mut hook_a = engine.install_write_body_entry_hook(NS_A, None);
    let mut hook_b = engine.install_write_body_entry_hook(NS_B, None);
    let writer_a_engine = Arc::clone(&engine);
    let writer_b_engine = Arc::clone(&engine);
    let writer_a = thread::spawn(move || {
        writer_a_engine
            .insert(NS_A, doc! { "_id": 1i32 })
            .expect("writer A insert")
    });
    let writer_b = thread::spawn(move || {
        writer_b_engine
            .insert(NS_B, doc! { "_id": 2i32 })
            .expect("writer B insert")
    });
    hook_a
        .wait_until_entered_timeout(GATE_TIMEOUT)
        .expect("writer A enters body");
    hook_b
        .wait_until_entered_timeout(GATE_TIMEOUT)
        .expect("writer B enters body");

    let (drained_tx, drained_rx) = mpsc::channel();
    let gate = Arc::clone(&engine.shared.checkpoint_admission);
    let drainer = thread::spawn(move || {
        let guard = gate.close_and_drain_all(GATE_TIMEOUT);
        drained_tx.send(guard).expect("send drain result");
    });
    assert!(
        drained_rx.recv_timeout(SHORT_WAIT).is_err(),
        "drain must wait for writers admitted on every namespace"
    );

    hook_a.release().expect("release writer A");
    hook_b.release().expect("release writer B");
    writer_a.join().expect("writer A joined");
    writer_b.join().expect("writer B joined");

    let guard = drained_rx
        .recv_timeout(GATE_TIMEOUT)
        .expect("drain completes after admitted writers exit")
        .expect("checkpoint admission guard");
    drainer.join().expect("drainer joined");

    let mut hook_c = engine.install_write_body_entry_hook(NS_C, None);
    let writer_c_engine = Arc::clone(&engine);
    let writer_c = thread::spawn(move || {
        writer_c_engine
            .insert(NS_C, doc! { "_id": 3i32 })
            .expect("writer C insert")
    });
    assert!(
        hook_c.wait_until_entered_timeout(SHORT_WAIT).is_err(),
        "new writers must stop before entering the write body while gate is closed"
    );

    drop(guard);
    hook_c
        .wait_until_entered_timeout(GATE_TIMEOUT)
        .expect("writer C enters after gate reopens");
    hook_c.release().expect("release writer C");
    writer_c.join().expect("writer C joined");
    Ok(())
}

#[test]
fn test_checkpoint_gate_released_on_planning_error() -> Result<()> {
    let engine = Arc::new(buffered_engine()?);
    create_namespaces(&engine, &[NS_A])?;

    let guard = engine
        .shared
        .checkpoint_admission
        .close_and_drain_all(GATE_TIMEOUT)?;
    drop(guard);

    let mut hook = engine.install_write_body_entry_hook(NS_A, None);
    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        writer_engine
            .insert(NS_A, doc! { "_id": 10i32 })
            .expect("writer insert after planning error")
    });
    hook.wait_until_entered_timeout(GATE_TIMEOUT)
        .expect("planning-error drop reopens checkpoint admission");
    hook.release().expect("release writer");
    writer.join().expect("writer joined");
    Ok(())
}

#[test]
fn test_checkpoint_gate_poison_path_rejects_operations() -> Result<()> {
    let engine = buffered_engine()?;
    create_namespaces(&engine, &[NS_A])?;
    let guard = engine
        .shared
        .checkpoint_admission
        .close_and_drain_all(GATE_TIMEOUT)?;

    let reason = EngineFatalReason::PostDurablePublishFailure;
    engine.shared.poison_engine(reason.clone());
    drop(guard);

    let write_err = engine
        .insert(NS_A, doc! { "_id": 20i32 })
        .expect_err("poisoned engine rejects writes");
    assert!(matches!(
        write_err,
        Error::EngineFatal { reason: observed } if observed == reason
    ));

    let gate_err = match engine.shared.checkpoint_admission.admit_writer() {
        Ok(_) => panic!("poisoned gate admitted a writer"),
        Err(err) => err,
    };
    assert!(matches!(
        gate_err,
        Error::EngineFatal { reason: observed } if observed == reason
    ));
    Ok(())
}

#[test]
fn test_checkpoint_gate_does_not_block_readers() -> Result<()> {
    let engine = Arc::new(buffered_engine()?);
    create_namespaces(&engine, &[NS_A])?;
    engine.insert(NS_A, doc! { "_id": 30i32, "value": "visible" })?;

    let guard = engine
        .shared
        .checkpoint_admission
        .close_and_drain_all(GATE_TIMEOUT)?;
    let reader_engine = Arc::clone(&engine);
    let (reader_tx, reader_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let result = reader_engine.find(NS_A, &doc! { "_id": 30i32 }, &FindOptions::default());
        reader_tx.send(result).expect("send reader result");
    });

    let (docs, _) = reader_rx
        .recv_timeout(SHORT_WAIT)
        .expect("reader must not wait for checkpoint gate")?;
    assert_eq!(docs.len(), 1);

    drop(guard);
    reader.join().expect("reader joined");
    Ok(())
}
