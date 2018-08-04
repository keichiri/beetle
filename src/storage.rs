use std::path::{Path, PathBuf};
use std::{io, fs, mem};
use std::ffi::CString;

use libc;


struct DirectoryHandler {
    path: PathBuf,
    pieces_path: PathBuf,
}


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


impl DirectoryHandler {
    fn create(base_path: &str) -> Result<Self, StorageError> {
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

        let directory_handler = DirectoryHandler {
            path: path,
            pieces_path: pieces_path,
        };
        Ok(directory_handler)
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


#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::DirectoryHandler;

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

        DirectoryHandler::create(handler_path.to_str().unwrap())
            .expect("Failed to create directory handler");

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

        DirectoryHandler::create(handler_path.to_str().unwrap())
            .expect("Failed to create directory handler");

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
        let res = DirectoryHandler::create("/bin/handler");
        assert!(res.is_err());
    }
}