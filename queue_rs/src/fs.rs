use std::path::Path;
use std::{fs, io};
pub fn dir_size(path: &String) -> io::Result<usize> {
    fn dir_size(mut dir: fs::ReadDir) -> io::Result<usize> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let size = match file.metadata()? {
                data if data.is_dir() => dir_size(fs::read_dir(file.path())?)?,
                data => data.len() as usize,
            };
            Ok(acc + size)
        })
    }
    dir_size(fs::read_dir(Path::new(path))?)
}
