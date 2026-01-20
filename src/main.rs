mod app;
mod cli;
mod db;
mod wal;
mod binary;
mod error;

use app::AppState;
use cli::handle_input;
use db::KvState;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use wal::{read_wal, try_read_snapshot, wal_task};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(1024);

    let app_state = Arc::new(AppState {
        tx: tx.clone(),
        kv: tokio::sync::RwLock::new(KvState {
            map: try_read_snapshot().await,
        }),
    });

    read_wal(&app_state).await;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    tokio::spawn(wal_task(rx, app_state.clone()));

    let listener = TcpListener::bind("127.0.0.1:4000").await.unwrap();
    println!("Listening on 127.0.0.1:4000");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
        }

        _ = accept_loop(listener, app_state, shutdown_tx.clone()) => {}
    }

    let _ = shutdown_tx.send(());
    println!("Server stopped.");
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown_tx: broadcast::Sender<()>,
) {
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => break,
        };

        let shutdown_rx = shutdown_tx.subscribe();
        let state = state.clone();

        tokio::spawn(handle_connection(socket, state, shutdown_rx));
    }
}

async fn read_line_or_shutdown<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    buf: &mut String,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Option<usize> {
    tokio::select! {
        _ = shutdown_rx.recv() => None,

        result = async {
            buf.clear();
            reader.read_line(buf).await
        } => result.ok(),
    }
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    state: Arc<AppState>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        let bytes = match read_line_or_shutdown(&mut reader, &mut line, &mut shutdown_rx).await {
            Some(n) => n,
            None => break,
        };

        if bytes == 0 {
            break;
        }

        let cmd = line.trim();
        if cmd == "exit" {
            break;
        }

        match handle_input(cmd, state.clone()).await {
            Ok(resp) => {
                let _ = writer.write_all(resp.as_bytes()).await;
                let _ = writer.write_all(b"\n").await;
            }
            Err(err) => {
                let _ = writer.write_all(format!("ERR: {}\n", err).as_bytes()).await;
            }
        }
    }
}
