use crate::app::SharedState;
use crate::db::Command;
use axum::{
    extract::{Path, State},
    http::StatusCode,
};

pub async fn put_key(
    Path(key): Path<String>,
    State(state): State<SharedState>,
    body: String,
) -> Result<StatusCode, StatusCode> {
    let cmd = Command::Put { key, value: body };
    state
        .send(cmd)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn patch_key(
    Path(key): Path<String>,
    State(state): State<SharedState>,
    body: String,
) -> Result<StatusCode, StatusCode> {
    let cmd = Command::Patch { key, value: body };
    state
        .send(cmd)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_key(
    Path(key): Path<String>,
    State(state): State<SharedState>,
) -> Result<String, StatusCode> {
    let kv = state.kv.read().await;
    kv.map.get(&key).cloned().ok_or(StatusCode::NOT_FOUND)
}

pub async fn delete_key(
    Path(key): Path<String>,
    State(state): State<SharedState>,
) -> Result<StatusCode, StatusCode> {
    let cmd = Command::Delete { key };
    state
        .send(cmd)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}
