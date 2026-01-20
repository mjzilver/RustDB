use crate::app::SharedState;
use crate::db::Command;
use crate::error::{DbError, DbResult};
use std::fmt::Write;

pub fn parse_command(input: &str) -> Result<Command, String> {
    let args: Vec<&str> = input.split_whitespace().collect();
    match args.as_slice() {
        ["put", key, value] => Ok(Command::Put {
            key: key.to_string(),
            value: value.to_string(),
        }),
        ["delete", key] => Ok(Command::Delete {
            key: key.to_string(),
        }),
        _ => Err(format!("{args:?}").into()),
    }
}

pub async fn handle_input(input: &str, state: SharedState) -> DbResult<String> {
    if input.starts_with("get ") {
        let key = input.trim_start_matches("get ").trim();
        let kv = state.kv.read().await;
        let value = kv.map.get(key).cloned().ok_or(DbError::KeyNotFound)?;
        return Ok(value);
    } else if input.starts_with("dump_all") {
        let kv = state.kv.read().await;
        let mut s = String::new();
        for (k, v) in kv.map.iter() {
            writeln!(&mut s, "DEBUG: key: {k}, val: {v}").map_err(|err| DbError::Fmt(err))?;
        }
        return Ok(s);
    } else if input.starts_with("length") {
        return Ok(format!("Dict length: {}", state.kv.read().await.map.len()));
    }

    let cmd = parse_command(input).map_err(|input| DbError::InvalidCommand {
        input: input.to_string(),
    })?;
    state.send(cmd).await.map_err(|_| DbError::ChannelClosed)?;
    Ok("OK".to_string())
}
