use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};

use crate::binary::{read_exact, read_string, read_u32, write_string};
use crate::error::{DbError, DbResult};

#[derive(Clone)]
pub enum Command {
    Put { key: String, value: String },
    Delete { key: String },
    Get { key: String },
    Range { start: String, end: String },
    Keys { needle: String },
    Values { needle: String },
    Amount,
    DumpAll,
    Shutdown,
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::Put { key, value } => write!(f, "PUT key={} value={}", key, value),
            Command::Delete { key } => write!(f, "DELETE key={}", key),
            Command::Get { key } => write!(f, "GET key={}", key),
            Command::Range { start, end } => write!(f, "RANGE start={} end={}", start, end),
            Command::Keys { needle } => write!(f, "KEYS needle={needle}"),
            Command::Values { needle } => write!(f, "VALUES needle={needle}"),
            Command::Amount => write!(f, "AMOUNT"),
            Command::DumpAll => write!(f, "DUMP_ALL"),
            Command::Shutdown => write!(f, "SHUTDOWN"),
        }
    }
}

impl Command {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Command::Put { .. } | Command::Delete { .. })
    }

    fn opcode(&self) -> u8 {
        match self {
            Command::Put { .. } => 0,
            Command::Delete { .. } => 1,
            Command::Get { .. } => 2,
            Command::Range { .. } => 3,
            Command::Amount => 4,
            Command::DumpAll => 5,
            Command::Shutdown => 6,
            Command::Keys { .. } => 7,
            Command::Values { .. } => 8,
        }
    }

    pub fn serialize(&self) -> DbResult<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(self.opcode());

        match self {
            Command::Put { key, value } => {
                write_string(&mut buf, key);
                write_string(&mut buf, value);
            }
            Command::Delete { key } => {
                write_string(&mut buf, key);
            }
            _ => unreachable!(),
        }

        Ok(buf)
    }

    pub fn deserialize(bytes: &[u8]) -> DbResult<Self> {
        let mut cursor = 0;

        let opcode = read_exact(bytes, &mut cursor, 1)?[0];
        let key = read_string(bytes, &mut cursor)?;

        match opcode {
            0 => {
                let value = read_string(bytes, &mut cursor)?;
                Ok(Command::Put { key, value })
            }
            1 => Ok(Command::Delete { key }),
            _ => Err(DbError::InvalidCommand {
                input: format!("unknown opcode {}", opcode),
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
            _ => unreachable!(),
        }
    }

    pub fn serialize(&self) -> DbResult<Vec<u8>> {
        let mut buf = Vec::new();

        let len = self.map.len() as u32;
        buf.extend_from_slice(&len.to_be_bytes());

        for (k, v) in &self.map {
            write_string(&mut buf, k);
            write_string(&mut buf, v);
        }

        Ok(buf)
    }

    pub fn deserialize(bytes: &[u8]) -> DbResult<Self> {
        let mut cursor = 0;
        let len = read_u32(bytes, &mut cursor)? as usize;

        let mut map = BTreeMap::new();
        for _ in 0..len {
            let key = read_string(bytes, &mut cursor)?;
            let value = read_string(bytes, &mut cursor)?;
            map.insert(key, value);
        }

        Ok(KvState { map })
    }
}
