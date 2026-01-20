use crate::error::{DbError, DbResult};

pub fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = s.len() as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

pub fn read_u32(bytes: &[u8], cursor: &mut usize) -> DbResult<u32> {
    if *cursor + 4 > bytes.len() {
        return Err(DbError::CorruptLog {
            line: "<truncated>".into(),
        });
    }

    let slice: [u8; 4] =
        bytes[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| DbError::CorruptLog {
                line: "<truncated>".into(),
            })?;

    *cursor += 4;
    Ok(u32::from_be_bytes(slice))
}

pub fn read_exact<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> DbResult<&'a [u8]> {
    if *cursor + len > bytes.len() {
        return Err(DbError::CorruptLog {
            line: "<truncated>".into(),
        });
    }

    let slice = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(slice)
}

pub fn read_string(bytes: &[u8], cursor: &mut usize) -> DbResult<String> {
    let len = read_u32(bytes, cursor)? as usize;
    let slice = read_exact(bytes, cursor, len)?;

    std::str::from_utf8(slice)
        .map(|s| s.to_owned())
        .map_err(|_| DbError::CorruptLog {
            line: "<utf8>".into(),
        })
}
