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
    Serialization { input: String },
    InvalidCommand { input: String },
    KeyNotFound,
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "disk error: {}", e),
            DbError::CorruptLog { line } => write!(f, "corrupt log entry: {}", line),
            DbError::InvalidCommand { input } => write!(f, "invalid command: {}", input),
            DbError::KeyNotFound => write!(f, "key not found"),
            DbError::Serialization { input } => write!(f, "Unable to serialize command {}", input),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Command {
    Put { key: String, value: String },
    Patch { key: String, value: String },
    Delete { key: String },
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::Put { key, value } => write!(f, "PUT key={} value={}", key, value),
            Command::Patch { key, value } => write!(f, "PATCH key={} value={}", key, value),
            Command::Delete { key } => write!(f, "DELETE key={}", key),
        }
    }
}

impl Command {
    pub fn serialize(&self) -> DbResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| DbError::Serialization {
            input: self.to_string(),
        })
    }

    pub fn deserialize(str: &String) -> DbResult<Command> {
        serde_json::from_str::<Command>(str).map_err(|_| DbError::CorruptLog {
            line: str.to_owned(),
        })
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
            Command::Patch { key, value } => {
                self.map.insert(key, value);
            }
            Command::Delete { key } => {
                self.map.remove(&key);
            }
        }
    }
}
