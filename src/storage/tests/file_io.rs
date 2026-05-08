use super::*;
use std::sync::Mutex;

/// In-memory `FileLock` implementation for testing `FilePageSource`.
struct MemFileLock {
    data: Mutex<Vec<u8>>,
}

impl MemFileLock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(Vec::new()),
        })
    }

    fn snapshot(&self) -> Vec<u8> {
        self.data.lock().unwrap().clone()
    }
}

impl FileLock for MemFileLock {
    fn acquire_exclusive(&self, _: std::time::Duration) -> Result<bool> {
        Ok(false)
    }
    fn acquire_shared(&self, _: std::time::Duration) -> Result<bool> {
        Ok(false)
    }
    fn release(&self) -> Result<()> {
        Ok(())
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut buf = self.data.lock().unwrap();
        let end = offset as usize + data.len();
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset as usize..end].copy_from_slice(data);
        Ok(())
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let data = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + buf.len();
        if end > data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read beyond end of mock file",
            )));
        }
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }
}

fn make_io() -> (Arc<MemFileLock>, FilePageSource) {
    let lock = MemFileLock::new();
    let io = FilePageSource::new(Arc::clone(&lock) as Arc<dyn FileLock>);
    (lock, io)
}

#[test]
fn file_offset_page_0_is_zero() {
    assert_eq!(FilePageSource::file_offset(0), 0);
}

#[test]
fn file_offset_page_1_is_32768() {
    assert_eq!(FilePageSource::file_offset(1), PAGE_SIZE_LEAF as u64);
}

#[test]
fn file_offset_is_uniform_32k_stride() {
    for n in 0u32..10 {
        assert_eq!(
            FilePageSource::file_offset(n),
            n as u64 * PAGE_SIZE_LEAF as u64
        );
    }
}

#[test]
fn write_and_read_32k_page_roundtrip() {
    let (_, io) = make_io();

    let mut data = vec![0u8; PageSize::Large32k.bytes()];
    data[0] = 0xAB;
    data[1000] = 0xCD;

    io.write_page(1, PageSize::Large32k, &data).unwrap();

    let mut buf = vec![0u8; PageSize::Large32k.bytes()];
    io.read_page(1, PageSize::Large32k, &mut buf).unwrap();

    assert_eq!(buf[0], 0xAB);
    assert_eq!(buf[1000], 0xCD);
}

#[test]
fn write_and_read_4k_page_roundtrip() {
    let (_, io) = make_io();

    let mut data = vec![0u8; PageSize::Small4k.bytes()];
    data[42] = 0xFF;

    io.write_page(0, PageSize::Small4k, &data).unwrap();

    let mut buf = vec![0u8; PageSize::Small4k.bytes()];
    io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

    assert_eq!(buf[42], 0xFF);
}

#[test]
fn pages_at_different_numbers_do_not_overlap() {
    let (_, io) = make_io();

    let mut p1_data = vec![0u8; PageSize::Large32k.bytes()];
    p1_data[0] = 0x11;

    let mut p2_data = vec![0u8; PageSize::Large32k.bytes()];
    p2_data[0] = 0x22;

    io.write_page(1, PageSize::Large32k, &p1_data).unwrap();
    io.write_page(2, PageSize::Large32k, &p2_data).unwrap();

    let mut buf = vec![0u8; PageSize::Large32k.bytes()];
    io.read_page(1, PageSize::Large32k, &mut buf).unwrap();
    assert_eq!(buf[0], 0x11, "page 1 data corrupted by page 2 write");

    io.read_page(2, PageSize::Large32k, &mut buf).unwrap();
    assert_eq!(buf[0], 0x22, "page 2 data corrupted by page 1 write");
}

#[test]
fn header_4k_and_page_1_32k_do_not_overlap() {
    let (mem, io) = make_io();

    // Write 4K header at page 0
    let mut header = vec![0u8; PageSize::Small4k.bytes()];
    header[0] = 0xAA;
    io.write_page(0, PageSize::Small4k, &header).unwrap();

    // Write 32K leaf at page 1 (should be at offset 32768)
    let mut leaf = vec![0u8; PageSize::Large32k.bytes()];
    leaf[0] = 0xBB;
    io.write_page(1, PageSize::Large32k, &leaf).unwrap();

    // Verify the file layout: header at 0, leaf at 32768
    let snap = mem.snapshot();
    assert_eq!(snap[0], 0xAA, "header first byte corrupted");
    assert_eq!(snap[32768], 0xBB, "leaf page at wrong offset");

    // Bytes 4096..32767 (between header and leaf slot) should be zero
    assert!(
        snap[4096..32768].iter().all(|&b| b == 0),
        "gap between header and page 1 must be zero"
    );
}

#[test]
fn read_beyond_eof_returns_zeroes_32k() {
    let (_, io) = make_io(); // empty file

    let mut buf = vec![0xFFu8; PageSize::Large32k.bytes()];
    io.read_page(3, PageSize::Large32k, &mut buf).unwrap();

    assert!(buf.iter().all(|&b| b == 0), "EOF read must return zeroes");
}

#[test]
fn read_beyond_eof_returns_zeroes_4k() {
    let (_, io) = make_io();

    let mut buf = vec![0xFFu8; PageSize::Small4k.bytes()];
    io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

    assert!(
        buf.iter().all(|&b| b == 0),
        "EOF read of page 0 must return zeroes"
    );
}

#[test]
fn second_write_overwrites_first() {
    let (_, io) = make_io();

    let first = vec![0x11u8; PageSize::Small4k.bytes()];
    let second = vec![0x22u8; PageSize::Small4k.bytes()];

    io.write_page(0, PageSize::Small4k, &first).unwrap();
    io.write_page(0, PageSize::Small4k, &second).unwrap();

    let mut buf = vec![0u8; PageSize::Small4k.bytes()];
    io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

    assert!(
        buf.iter().all(|&b| b == 0x22),
        "second write must overwrite first"
    );
}
