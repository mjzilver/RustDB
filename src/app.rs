use crate::db::Command;
use crate::db::KvState;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub tx: mpsc::Sender<Command>,
    pub kv: RwLock<KvState>,
    pub shutdown_tx: broadcast::Sender<()>,
}

impl AppState {
    pub async fn send(&self, cmd: Command) -> Result<(), ()> {
        self.tx.send(cmd).await.map_err(|_| ())
    }
}
