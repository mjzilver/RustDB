use std::fmt::{self, Display, Formatter};

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    Fmt(std::fmt::Error),
    CorruptLog { line: String },
    InvalidCommand { input: String },
    ChannelClosed,
    KeyNotFound,
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "disk error: {}", e),
            DbError::Fmt(e) => write!(f, "formatting error: {}", e),
            DbError::CorruptLog { line } => write!(f, "corrupt log entry: {}", line),
            DbError::InvalidCommand { input } => write!(f, "invalid command: {}", input),
            DbError::KeyNotFound => write!(f, "key not found"),
            DbError::ChannelClosed => write!(f, "task channel was closed"),
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e)
    }
}

impl From<std::fmt::Error> for DbError {
    fn from(e: std::fmt::Error) -> Self {
        DbError::Fmt(e)
    }
}
