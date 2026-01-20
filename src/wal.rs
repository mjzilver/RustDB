use std::collections::BTreeMap;

use crate::app::SharedState;
use crate::db::{Command, KvState};
use crate::error::{DbError, DbResult};
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Receiver;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use crate::config::config_get;

pub static WAL_FILEPATH: &str = "wal.bin";
pub static SNAP_FILEPATH: &str = "snap.bin";

pub async fn try_read_snapshot() -> BTreeMap<String, String> {
    if let Ok(bytes) = fs::read(SNAP_FILEPATH).await {
        match KvState::deserialize(&bytes) {
            Ok(state) => return state.map,
            Err(_) => return BTreeMap::new(),
        }
    }
    return BTreeMap::new();
}

pub async fn check_snapshot_needed(app_state: &SharedState) -> DbResult<()> {
    let metadata = fs::metadata(WAL_FILEPATH).await.map_err(DbError::Io)?;
    let wal_size = metadata.len();

    if wal_size > config_get().max_wal_size {
        let bytes = app_state.kv.read().await.serialize()?;

        if cfg!(debug_assertions) {
            println!("WAL file size reached: {}, Writing {} bytes to snapfile", wal_size, bytes.len());
        }

        let tmp_path = "snap.bin.tmp";
        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)
            .await
            .map_err(DbError::Io)?;

        tmp.write_all(&bytes).await.map_err(DbError::Io)?;
        tmp.flush().await.map_err(DbError::Io)?;

        fs::rename(tmp_path, SNAP_FILEPATH)
            .await
            .map_err(DbError::Io)?;

        let mut wal_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(WAL_FILEPATH)
            .await
            .map_err(DbError::Io)?;

        wal_file.flush().await.map_err(DbError::Io)?;
    }

    Ok(())
}

pub async fn read_wal(app_state: &SharedState) {
    if let Ok(file) = File::open(WAL_FILEPATH).await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut kv = app_state.kv.write().await;

        while let Ok(Some(line)) = lines.next_line().await {
            match Command::deserialize(&line.into_bytes()) {
                Ok(cmd) => kv.apply(cmd),
                Err(e) => eprintln!("Skipping corrupt log entry: {e}"),
            }
        }
    }
}

pub async fn wal_task(mut rx: Receiver<Command>, state: SharedState) -> DbResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WAL_FILEPATH)
        .await
        .map_err(DbError::Io)?;

    while let Some(cmd) = rx.recv().await {
        let data = cmd.serialize()?;
        file.write_all(&data).await.map_err(DbError::Io)?;
        file.write_all(b"\n").await.map_err(DbError::Io)?;
        file.flush().await.map_err(DbError::Io)?;

        {
            let mut kv = state.kv.write().await;
            kv.apply(cmd);
        }

        check_snapshot_needed(&state).await?;
    }
    Ok(())
}
