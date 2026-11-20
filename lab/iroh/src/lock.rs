//! Section 10: the data-dir lock (as written in the snippets).
use std::fs::{File, OpenOptions, TryLockError};
use std::{io, path::Path};

/// Held for the lifetime of the node; dropping the file releases the lock.
#[derive(Debug)]
pub struct InstanceLock(#[allow(dead_code)] File);

pub fn lock_data_dir(dir: &Path) -> io::Result<InstanceLock> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(InstanceLock(file)),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another station is already running with this data dir (one station per identity)",
        )),
        Err(TryLockError::Error(e)) => Err(e),
    }
}
