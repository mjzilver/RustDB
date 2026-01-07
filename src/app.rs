use crate::db::Command;
use crate::db::KvState;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc::Sender};

pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub tx: Sender<Command>,
    pub kv: RwLock<KvState>,
}

impl AppState {
    pub async fn send(&self, cmd: Command) -> Result<(), ()> {
        self.tx.send(cmd).await.map_err(|_| ())
    }
}
