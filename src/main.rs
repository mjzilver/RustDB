mod app;
mod binary;
mod cli;
mod config;
mod db;
mod error;
mod wal;

use app::AppState;
use cli::handle_input;
use db::KvState;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, mpsc};
use wal::{read_wal, try_read_snapshot, wal_task};

use crate::config::config_get;

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(1024);
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let app_state = Arc::new(AppState {
        tx: tx.clone(),
        kv: RwLock::new(KvState {
            map: try_read_snapshot().await,
        }),
        shutdown_tx: shutdown_tx.clone(),
    });

    read_wal(&app_state).await;

    tokio::spawn(wal_task(rx, app_state.clone()));

    let port = config_get().port;
    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(addr.clone()).await.unwrap();
    println!("Listening on {addr}");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down...");
            let _ = app_state.shutdown_tx.send(());
        }

        _ = accept_loop(listener, app_state.clone()) => {}
    }

    println!("Server stopped.");
}

async fn accept_loop(listener: TcpListener, state: Arc<AppState>) {
    let mut shutdown_rx = state.shutdown_tx.subscribe();

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                break;
            }

            accept = listener.accept() => {
                let (socket, _) = match accept {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let shutdown_rx = state.shutdown_tx.subscribe();
                let state = state.clone();

                tokio::spawn(handle_connection(socket, state, shutdown_rx));
            }
        }
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
