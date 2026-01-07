use crate::app::SharedState;
use crate::db::{Command, DbError, DbResult};

pub fn parse_command(input: &str) -> Result<Command, String> {
    let args: Vec<&str> = input.split_whitespace().collect();
    match args.as_slice() {
        ["put", key, value] => Ok(Command::Put {
            key: key.to_string(),
            value: value.to_string(),
        }),
        ["patch", key, value] => Ok(Command::Patch {
            key: key.to_string(),
            value: value.to_string(),
        }),
        ["delete", key] => Ok(Command::Delete {
            key: key.to_string(),
        }),
        _ => Err("Invalid command".into()),
    }
}

pub async fn handle_input(input: &str, state: SharedState) -> DbResult<String> {
    if input.starts_with("get ") {
        let key = input.trim_start_matches("get ").trim();
        let kv = state.kv.read().await;
        let value = kv.map.get(key).cloned().ok_or(DbError::KeyNotFound)?;
        return Ok(value);
    }

    let cmd = parse_command(input).map_err(|input| DbError::InvalidCommand {
        input: input.to_string(),
    })?;
    state.send(cmd).await.map_err(|_| {
        DbError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send command to WAL",
        ))
    })?;
    Ok("OK".to_string())
}
