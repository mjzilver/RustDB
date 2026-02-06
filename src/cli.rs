use crate::app::SharedState;
use crate::db::Command;
use crate::error::{DbError, DbResult};
use std::fmt::Write;
use std::ops::Bound::Included;

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
        ["get", key] => Ok(Command::Get {
            key: key.to_string(),
        }),
        ["range", start, end] => Ok(Command::Range {
            start: start.to_string(),
            end: end.to_string(),
        }),
        ["keys", needle] => Ok(Command::Keys {
            needle: needle.to_string(),
        }),
        ["values", needle] => Ok(Command::Values {
            needle: needle.to_string(),
        }),
        ["amount"] => Ok(Command::Amount),
        ["dump_all"] => Ok(Command::DumpAll),
        ["shutdown"] => Ok(Command::Shutdown),
        _ => Err(format!("{args:?}")),
    }
}

pub async fn handle_input(input: &str, state: SharedState) -> DbResult<String> {
    let cmd = parse_command(input).map_err(|input| DbError::InvalidCommand {
        input: input.to_string(),
    })?;

    if cmd.is_mutation() {
        state.send(cmd).await.map_err(|_| DbError::ChannelClosed)?;
        return Ok("OK".to_string());
    }

    match cmd {
        Command::Range { start, end } => {
            let kv = state.kv.read().await;
            let mut s = String::new();
            for (k, v) in kv.map.range((Included(start), Included(end))) {
                writeln!(&mut s, "key: {k}, val: {v}").map_err(DbError::Fmt)?;
            }
            Ok(s)
        }
        Command::Keys { needle } => {
            let kv = state.kv.read().await;
            let mut s = String::new();
            for k in kv.map.keys() {
                if k.contains(&needle) {
                    writeln!(&mut s, "key: {k}").map_err(DbError::Fmt)?;
                }
            }
            Ok(s)
        }
        Command::Values { needle } => {
            let kv = state.kv.read().await;
            let mut s = String::new();
            for v in kv.map.values() {
                if v.contains(&needle) {
                    writeln!(&mut s, "val: {v}").map_err(DbError::Fmt)?;
                }
            }
            Ok(s)
        }
        Command::Get { key } => {
            let kv = state.kv.read().await;
            kv.map.get(&key).cloned().ok_or(DbError::KeyNotFound)
        }
        Command::Amount => Ok(format!(
            "Amount of keys: {}",
            state.kv.read().await.map.len()
        )),
        Command::DumpAll => {
            let kv = state.kv.read().await;
            let mut s = String::new();
            for (k, v) in kv.map.iter() {
                writeln!(&mut s, "DEBUG: key: {k}, val: {v}").map_err(DbError::Fmt)?;
            }
            Ok(s)
        }
        Command::Shutdown => {
            let _ = state.shutdown_tx.send(());
            Ok("OK".to_string())
        }
        _ => unreachable!(),
    }
}
