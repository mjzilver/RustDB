use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    CorruptLog { line: String },
    InvalidCommand { input: String },
    ChannelClosed,
    KeyNotFound,
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "disk error: {}", e),
            DbError::CorruptLog { line } => write!(f, "corrupt log entry: {}", line),
            DbError::InvalidCommand { input } => write!(f, "invalid command: {}", input),
            DbError::KeyNotFound => write!(f, "key not found"),
            DbError::ChannelClosed => write!(f, "Task channel was closed"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Command {
    Put { key: String, value: String },
    Delete { key: String },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::Put { key, value } => write!(f, "PUT key={} value={}", key, value),
            Command::Delete { key } => write!(f, "DELETE key={}", key),
        }
    }
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = s.len() as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> DbResult<String> {
    let len = u32::from_be_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap()) as usize;
    *cursor += 4;

    let s = std::str::from_utf8(&bytes[*cursor..*cursor + len])
        .map_err(|_| DbError::CorruptLog {
            line: "<utf8>".into(),
        })?
        .to_owned();

    *cursor += len;
    Ok(s)
}

impl Command {
    pub fn to_number(&self) -> u8 {
        match self {
            Command::Put { .. } => 0,
            Command::Delete { .. } => 1,
        }
    }

    pub fn serialize(&self) -> DbResult<Vec<u8>> {
        let mut buf = Vec::new();

        match self {
            Command::Put { key, value } => {
                buf.push(self.to_number());
                write_string(&mut buf, key);
                write_string(&mut buf, value);
            }
            Command::Delete { key } => {
                buf.push(self.to_number());
                write_string(&mut buf, key);
            }
        }
        Ok(buf)
    }

    pub fn deserialize(str: &String) -> DbResult<Command> {
        let bytes = str.clone().into_bytes();
        let mut cursor = 0;

        let cmd = bytes.get(cursor).ok_or(DbError::CorruptLog {
            line: "<empty>".into(),
        })?;
        cursor += 1;

        let key = read_string(&bytes, &mut cursor)?;

        match cmd {
            0 => {
                let value = read_string(&bytes, &mut cursor)?;
                Ok(Command::Put { key, value })
            }
            1 => Ok(Command::Delete { key }),
            _ => Err(DbError::InvalidCommand {
                input: format!("unknown opcode {}", cmd),
            }),
        }
    }
}

pub struct KvState {
    pub map: BTreeMap<String, String>,
}

impl KvState {
    pub fn apply(&mut self, cmd: Command) {
        match cmd {
            Command::Put { key, value } => {
                self.map.insert(key, value);
            }
            Command::Delete { key } => {
                self.map.remove(&key);
            }
        }
    }
}
