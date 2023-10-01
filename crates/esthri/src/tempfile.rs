use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use tokio::{
    fs::{File, OpenOptions},
    io::AsyncSeekExt,
    task,
};

use crate::Result;

pub struct TempFile {
    path: tempfile::TempPath,
    file: Option<File>,
}

pub const TEMP_FILE_PREFIX: &str = ".esthri_temp";

impl TempFile {
    pub async fn new(dir: PathBuf, suffix: Option<&str>) -> Result<Self> {
        let suffix = suffix.unwrap_or_default().to_owned();
        if !dir.exists() {
            tokio::fs::create_dir_all(&dir).await?;
        }
        let path = task::spawn_blocking(move || {
            let f = tempfile::Builder::new()
                .prefix(TEMP_FILE_PREFIX)
                .suffix(&suffix)
                .tempfile_in(dir)?;
            Result::Ok(f.into_temp_path())
        })
        .await??;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub async fn rewind(&mut self) -> Result<()> {
        self.file.as_mut().unwrap().seek(SeekFrom::Start(0)).await?;
        Ok(())
    }

    pub async fn persist(self, path: PathBuf) -> Result<()> {
        task::spawn_blocking(move || self.path.persist(path)).await??;
        Ok(())
    }

    pub fn file_mut(&mut self) -> &mut File {
        self.file.as_mut().unwrap()
    }

    pub fn take_file(&mut self) -> File {
        self.file.take().unwrap()
    }

    pub async fn take_std_file(&mut self) -> std::fs::File {
        self.file.take().unwrap().into_std().await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn into_path(self) -> Box<dyn AsRef<Path> + Send + Sync> {
        Box::new(self.path)
    }
}
