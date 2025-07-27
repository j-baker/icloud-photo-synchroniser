use eyre::Result;
use std::{
    fs::File,
    io::{self, Write},
    path::Path,
};

use rusqlite::{ToSql, types::FromSql};
use sha2::{Digest, Sha256};

const SHA256_BYTES: usize = 32;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Sha256Hash([u8; SHA256_BYTES]);

impl Sha256Hash {
    #[cfg(test)]
    pub fn new_for_tests(id: u8) -> Self {
        Self([id; SHA256_BYTES])
    }
}

impl ToSql for Sha256Hash {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for Sha256Hash {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(Self(FromSql::column_result(value)?))
    }
}

pub struct DigestWriter<W: Write> {
    inner: W,
    sha256: Sha256,
}

impl<W: Write> DigestWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            sha256: Sha256::new(),
        }
    }

    pub fn finalise(mut self) -> Result<Sha256Hash> {
        self.inner.flush()?;
        Ok(Sha256Hash(self.sha256.finalize().into()))
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        if written > 0 {
            self.sha256.update(&buf[..written]);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn digest(path: &Path) -> Result<Sha256Hash> {
    let mut hasher = Sha256::new();
    let mut file = File::open(path)?;
    io::copy(&mut file, &mut hasher)?;
    Ok(Sha256Hash(hasher.finalize().into()))
}
