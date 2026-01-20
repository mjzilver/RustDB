use crate::app::SharedState;
use crate::db::{Command, DbError, DbResult};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Receiver;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

pub static FILEPATH: &str = "wal.bin";

pub async fn read_wal(app_state: &SharedState) {
    if let Ok(file) = tokio::fs::File::open(FILEPATH).await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut kv = app_state.kv.write().await;

        while let Ok(Some(line)) = lines.next_line().await {
            match Command::deserialize(&line) {
                Ok(cmd) => kv.apply(cmd),
                Err(e) => eprintln!("Skipping corrupt log entry: {e}"),
            }
        }
    }
}

pub async fn wal_task(mut rx: Receiver<Command>, kv: SharedState) -> DbResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(FILEPATH)
        .await
        .map_err(DbError::Io)?;

    while let Some(cmd) = rx.recv().await {
        let data = cmd.serialize()?;
        file.write_all(&data).await.map_err(DbError::Io)?;
        file.write_all(b"\n").await.map_err(DbError::Io)?;
        file.flush().await.map_err(DbError::Io)?;

        let mut kv = kv.kv.write().await;
        kv.apply(cmd);
    }

    Ok(())
}
