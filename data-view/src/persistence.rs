use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::DataViewContractError;

pub trait DataViewPersistence: Send + Sync {
    fn load(&self) -> Result<Option<Vec<u8>>, DataViewContractError>;
    fn persist(&self, state: &[u8]) -> Result<(), DataViewContractError>;
}

#[derive(Debug, Default)]
pub struct VolatileDataViewPersistence;

impl DataViewPersistence for VolatileDataViewPersistence {
    fn load(&self) -> Result<Option<Vec<u8>>, DataViewContractError> {
        Ok(None)
    }

    fn persist(&self, _state: &[u8]) -> Result<(), DataViewContractError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryDataViewPersistence {
    state: Mutex<Option<Vec<u8>>>,
}

impl MemoryDataViewPersistence {
    pub fn snapshot(&self) -> Option<Vec<u8>> {
        self.state.lock().ok().and_then(|state| state.clone())
    }
}

impl DataViewPersistence for MemoryDataViewPersistence {
    fn load(&self) -> Result<Option<Vec<u8>>, DataViewContractError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| DataViewContractError::PersistenceFailed)
    }

    fn persist(&self, state: &[u8]) -> Result<(), DataViewContractError> {
        let mut current = self
            .state
            .lock()
            .map_err(|_| DataViewContractError::PersistenceFailed)?;
        *current = Some(state.to_vec());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileDataViewPersistence {
    path: PathBuf,
    lock_file: Option<Arc<File>>,
    read_only: bool,
}

impl FileDataViewPersistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock_file: None,
            read_only: false,
        }
    }

    /// Observe an existing atomic snapshot without a writer lock or initialization.
    /// All attempted persistence fails closed, including genesis creation.
    pub fn new_read_only(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), lock_file: None, read_only: true }
    }

    pub fn new_exclusive(path: impl Into<PathBuf>) -> Result<Self, DataViewContractError> {
        let path = path.into();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|_| DataViewContractError::PersistenceFailed)?;
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(PathBuf::from(lock_name))
            .map_err(|_| DataViewContractError::PersistenceFailed)?;
        lock_file
            .try_lock()
            .map_err(|_| DataViewContractError::PersistenceFailed)?;
        Ok(Self {
            path,
            lock_file: Some(Arc::new(lock_file)),
            read_only: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn holds_exclusive_lock(&self) -> bool {
        self.lock_file.is_some()
    }
}

impl DataViewPersistence for FileDataViewPersistence {
    fn load(&self) -> Result<Option<Vec<u8>>, DataViewContractError> {
        match fs::read(&self.path) {
            Ok(state) => Ok(Some(state)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !self.read_only => Ok(None),
            Err(_) => Err(DataViewContractError::PersistenceFailed),
        }
    }

    fn persist(&self, state: &[u8]) -> Result<(), DataViewContractError> {
        if self.read_only {
            return Err(DataViewContractError::PersistenceFailed);
        }
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|_| DataViewContractError::PersistenceFailed)?;
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(DataViewContractError::PersistenceFailed)?;
        let temporary = parent.join(format!(".{file_name}.tmp"));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)
                .map_err(|_| DataViewContractError::PersistenceFailed)?;
            file.write_all(state)
                .map_err(|_| DataViewContractError::PersistenceFailed)?;
            file.sync_all()
                .map_err(|_| DataViewContractError::PersistenceFailed)?;
            fs::rename(&temporary, &self.path)
                .map_err(|_| DataViewContractError::PersistenceFailed)?;
            if let Ok(directory) = fs::File::open(parent) {
                directory
                    .sync_all()
                    .map_err(|_| DataViewContractError::PersistenceFailed)?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}
