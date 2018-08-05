use std::path::{Path, PathBuf};
use std::{io, fs, mem};
use std::ffi::CString;
use std::io::{Read, Write};
use std::rc::Rc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use libc;


#[derive(Debug)]
enum StorageError {
    IOError(io::Error),
    PathError(String),
    PermissionError(String),
}


impl From<io::Error> for StorageError {
    fn from(io_err: io::Error) -> StorageError {
        StorageError::IOError(io_err)
    }
}


struct StorageHandler {
    path: PathBuf,
    pieces_path: PathBuf,
    cache: Cache<Rc<Vec<u8>>>,
}

impl StorageHandler {
    pub fn create(base_path: &str, cache_size: usize) -> Result<Self, StorageError> {
        let path = Path::new(base_path).to_owned();
        let mut pieces_path = path.clone();
        pieces_path.push(".pieces");

        if path.exists() {
            if !path.is_dir() {
                return Err(StorageError::PathError(String::from("Provided path exists and is not directory")));
            }

            let is_owner = _check_if_owner(base_path)?;
            if !is_owner {
                return Err(StorageError::PermissionError(String::from("Current user not owner of directory at given path")));
            }

            if pieces_path.exists() {
                if !pieces_path.is_dir() {
                    return Err(StorageError::PathError(String::from("Invalid pieces path inside of provided path")));
                }
            } else {
                fs::DirBuilder::new().create(&pieces_path)?;
            }
        } else {
            fs::DirBuilder::new()
                .recursive(true)
                .create(&pieces_path)?;
        }

        let storage_handler = StorageHandler {
            path: path,
            pieces_path: pieces_path,
            cache: Cache::new(cache_size),
        };
        Ok(storage_handler)
    }

    pub fn store_piece(&self, piece_index: u32, piece_data: &[u8]) -> Result<(), StorageError> {
        let piece_path = self._get_piece_path(piece_index);
        let mut file = fs::File::create(piece_path)?;
        file.write_all(piece_data)?;
        Ok(())
    }

    // NOTE - has to be mutable reference because of the cache. Consider using RefCell
    pub fn retrieve_piece(&mut self, piece_index: u32) -> Result<Rc<Vec<u8>>, StorageError> {
        let piece_path = self._get_piece_path(piece_index);
        let mut file = fs::File::open(piece_path)?;
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content)?;
        let file_content = Rc::new(file_content);
        self.cache.put(piece_index, Rc::clone(&file_content));
        Ok(file_content)
    }

    fn _get_piece_path(&self, piece_index: u32) -> PathBuf {
        let mut piece_path = self.pieces_path.clone();
        piece_path.push(&format!("{}.piece", piece_index));
        piece_path
    }
}


fn _check_if_owner(path: &str) -> Result<bool, StorageError> {
    let c_path = CString::new(path).unwrap();

    unsafe {
        let mut stat: libc::stat = mem::zeroed();
        match libc::stat(c_path.as_ptr(), &mut stat) {
            0 => {
                let cuid = libc::getuid();
                if stat.st_uid == cuid {
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
            _ => {
                Err(StorageError::PathError(String::from("Failed to get stat for path")))
            }
        }
    }
}


struct CacheRecord<T> where T: Clone {
    key: u32,
    item: T,
    timestamp: u64,
}

impl<T> CacheRecord<T> where T: Clone {
    fn new(key: u32, item: T, timestamp: u64) -> Self {
        Self {
            key: key,
            item: item,
            timestamp: timestamp,
        }
    }
}


struct Cache<T> where T: Clone {
    max_size: usize,
    records: HashMap<u32, CacheRecord<T>>,
}

impl<T> Cache<T> where T: Clone {
    fn new(max_size: usize) -> Self {
        Self {
            max_size: max_size,
            records: HashMap::new(),
        }
    }

    fn put(&mut self, key: u32, data: T) {
        if self.records.len() >= self.max_size {
            self.purge();
        }

        let ts = _calculate_timestamp();
        let new_record = CacheRecord::new(key,data, ts);
        self.records.insert(key, new_record);
    }

    fn get(&mut self, key: u32) -> Option<T> {
        let record = match self.records.get_mut(&key) {
            None => return None,
            Some(record) => record,
        };

        let ts = _calculate_timestamp();
        record.timestamp = ts;
        Some(record.item.clone())
    }

    fn purge(&mut self) {
        let to_remove_count = self.records.len() * 1 / 3;
        let keys_to_remove = {
            let mut record_references = Vec::new();
            for record in self.records.values() {
                record_references.push(record);
            }
            record_references.sort_by_key(|record| record.timestamp);

            let mut keys_to_remove = Vec::with_capacity(to_remove_count);
            for i in 0 .. to_remove_count {
                let record = record_references[i];
                keys_to_remove.push(record.key);
            };
            keys_to_remove
        };

        for key_to_remove in keys_to_remove {
            self.records.remove(&key_to_remove);
        }
    }
}


fn _calculate_timestamp() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000
}


#[cfg(test)]
mod tests {
    use std::{fs, thread, time};
    use std::path::{Path, PathBuf};
    use std::io::{Read, Write};


    use super::{Cache, StorageHandler};

    fn _get_base_path() -> PathBuf {
        PathBuf::from("/tmp/beetle/tests/storage")
    }

    fn _get_handler_path() -> PathBuf {
        let mut path = _get_base_path();
        path.push("handler");
        path
    }

    fn _get_pieces_path() -> PathBuf {
        let mut path = _get_handler_path();
        path.push(".pieces");
        path
    }

    fn _prepare_base_path() {
        let base_path = _get_base_path();
        if !base_path.exists() {
            fs::DirBuilder::new()
                .recursive(true)
                .create(base_path).expect("Failed to create base path for tests");
        }
    }

    fn _prepare_handler_path() -> String {
        _prepare_base_path();
        let mut handler_path = _get_handler_path();
        handler_path.to_str().unwrap().to_owned()
    }

    fn _clear_path() {
        let mut handler_path = _get_handler_path();
        if handler_path.exists() {
            fs::remove_dir_all(&handler_path).expect("Failed to remove handler path");
        }
    }

    fn _create_pieces_path() {
        let pieces_path = _get_pieces_path();
        fs::DirBuilder::new()
            .recursive(true)
            .create(&pieces_path)
            .expect("Failed to create pieces path");
    }

    #[test]
    fn test_create_if_no_such_path() {
        _clear_path();
        _prepare_handler_path();
        let handler_path = _get_handler_path();

        StorageHandler::create(handler_path.to_str().unwrap(), 5)
            .expect("Failed to create storage handler");

        if !handler_path.exists() && !handler_path.is_dir() {
            panic!("Handler path does not exist");
        }
        let pieces_path = _get_pieces_path();
        if !pieces_path.exists() && !pieces_path.is_dir() {
            panic!("Pieces path does not exist");
        }

        _clear_path();
    }

    #[test]
    fn test_create_if_path_exists() {
        _clear_path();
        _create_pieces_path();
        let mut piece_path = _get_pieces_path();
        piece_path.push("1.piece");
        let file = fs::File::create(&piece_path).
            expect("Failed to create piece file");

        let handler_path = _get_handler_path();

        StorageHandler::create(handler_path.to_str().unwrap(), 5)
            .expect("Failed to create storage handler");

        if !handler_path.exists() && !handler_path.is_dir() {
            panic!("Handler path does not exist");
        }
        if !piece_path.exists() && !piece_path.is_file() {
            panic!("Pieces path does not exist");
        }

        _clear_path();
    }

    #[test]
    fn test_create_if_no_permissions() {
        let res = StorageHandler::create("/bin/handler", 5);
        assert!(res.is_err());
    }
    
    #[test]
    fn test_store_piece() {
        _clear_path();
        _prepare_handler_path();
        let piece_index = 3;
        let piece_content = vec![10, 20, 30, 40, 50];
        
        let storage_handler = StorageHandler::create(_get_handler_path().to_str().unwrap(), 5).
            expect("Failed to create storage handler");
        storage_handler.store_piece(piece_index, &piece_content).expect("Failed to store piece");

        let mut pieces_path = _get_pieces_path();
        pieces_path.push("3.piece");
        let mut piece_file = fs::File::open(&pieces_path).expect("Failed to open piece file");
        let mut retrieved_piece_content = Vec::new();
        piece_file.read_to_end(&mut retrieved_piece_content).expect("Failed to read piece file");

        assert_eq!(piece_content, retrieved_piece_content);
        
        _clear_path();
    }

    #[test]
    fn test_retrieve_piece() {
        _clear_path();
        _prepare_handler_path();
        let mut pieces_path = _get_pieces_path();
        pieces_path.push("3.piece");
        let mut storage_handler = StorageHandler::create(_get_handler_path().to_str().unwrap(), 5).
            expect("Failed to create storage handler");
        let mut piece_file = fs::File::create(&pieces_path).expect("Failed to create piece file");
        let piece_content = vec![10, 20, 30, 40, 50];
        piece_file.write_all(&piece_content).expect("Failed to write to piece file");

        let retrieved_piece_content = storage_handler.retrieve_piece(3).expect("Failed to retrieve piece");

        assert_eq!(&piece_content, &*retrieved_piece_content);
    }

    #[test]
    fn test_cache() {
        let sleep_time = time::Duration::from_millis(1);
        let max_size = 7;
        let mut cache: Cache<i64> = Cache::new(max_size);
        cache.put(1, 1);
        thread::sleep(sleep_time);
        cache.put(2, 4);
        thread::sleep(sleep_time);
        cache.put(3, 9);
        thread::sleep(sleep_time);
        cache.get(1).unwrap();
        cache.put(4, 16);
        thread::sleep(sleep_time);
        cache.put(5, 25);
        thread::sleep(sleep_time);
        cache.put(6, 36);
        thread::sleep(sleep_time);
        cache.put(7, 49);
        thread::sleep(sleep_time);

        assert_eq!(cache.records.len(), max_size);

        cache.put(8, 64);

        assert!(cache.records.len() < max_size);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_none());
        assert!(cache.get(4).is_some());
        assert!(cache.get(5).is_some());
        assert!(cache.get(6).is_some());
        assert!(cache.get(7).is_some());
        assert!(cache.get(8).is_some());
    }
}