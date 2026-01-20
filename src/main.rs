mod app;
mod cli;
mod db;
mod handlers;
mod wal;

use app::AppState;
use axum::{Router, routing::put};
use cli::handle_input;
use handlers::*;
use std::io::Write;
use std::io::stdin;
use std::io::stdout;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use wal::read_wal;
use wal::wal_task;

#[tokio::main]
async fn main() {
    let (tx, rx) = channel(1024);

    let app_state = Arc::new(AppState {
        tx: tx.clone(),
        kv: tokio::sync::RwLock::new(crate::db::KvState {
            map: std::collections::BTreeMap::new(),
        }),
    });

    read_wal(&app_state).await;

    tokio::spawn(wal_task(rx, app_state.clone()));

    let app = Router::new()
        .route(
            "/kv/{key}",
            put(put_key)
                .get(get_key)
                .delete(delete_key),
        )
        .with_state(app_state.clone());

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    let mut s = String::new();
    loop {
        print!("> ");
        stdout().flush().unwrap();

        s.clear();
        stdin().read_line(&mut s).unwrap();

        if s.trim() == "exit" {
            break;
        }

        match handle_input(&s.trim(), app_state.clone()).await {
            Ok(r) => println!("{}", r),
            Err(e) => eprintln!("{}", e),
        }
    }
}
