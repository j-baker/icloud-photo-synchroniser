use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use eyre::{ContextCompat, Result, eyre};
use rusqlite::{Connection, OptionalExtension, params};

use crate::digest::Sha256Hash;

pub struct PhotoSyncStore(Connection);

impl PhotoSyncStore {
    #[cfg(test)]
    pub fn new_for_tests() -> Result<Self> {
        let mut store = Self(Connection::open_in_memory()?);
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn new(path: PathBuf) -> Result<Self> {
        let mut store = Self(Connection::open(path)?);
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn ensure_schema(&mut self) -> Result<()> {
        self.0.execute_batch(
            r#"
        CREATE TABLE IF NOT EXISTS old_target_files (
            path    TEXT    NOT NULL,
            mtime   INTEGER NOT NULL,
            size    INTEGER NOT NULL,
            digest  BLOB    NOT NULL,
            PRIMARY KEY (path)
        );

        CREATE TABLE IF NOT EXISTS source_files (
            path    TEXT    NOT NULL,
            mtime   INTEGER NOT NULL,
            size    INTEGER NOT NULL,
            digest  BLOB    NOT NULL,
            PRIMARY KEY (path)
        );

        DROP VIEW IF EXISTS all_target_digests;
        CREATE VIEW all_target_digests AS
              SELECT digest FROM old_target_files
        UNION ALL
              SELECT digest FROM source_files;
    "#,
        )?;
        Ok(())
    }

    pub fn exists_in_old_target(
        &mut self,
        path: &Path,
        last_modified: SystemTime,
        size: u64,
    ) -> Result<bool> {
        let mut stmt = self.0.prepare_cached(
            "SELECT 1 FROM old_target_files \
             WHERE path=?1 AND mtime=?2 AND size=?3 LIMIT 1",
        )?;
        let exists = stmt
            .query_row(
                params![
                    path_to_text(path)?,
                    system_time_as_i64(last_modified)?,
                    size as i64
                ],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn mark_exists_in_old_target(
        &mut self,
        path: &Path,
        last_modified: SystemTime,
        size: u64,
        digest: &Sha256Hash,
    ) -> Result<()> {
        self.0.execute(
            "INSERT INTO old_target_files (path, mtime, size, digest)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                path_to_text(path)?,
                system_time_as_i64(last_modified)?,
                size as i64,
                digest
            ],
        )?;
        Ok(())
    }

    pub fn exists_in_target(&mut self, digest: &Sha256Hash) -> Result<bool> {
        let mut stmt = self
            .0
            .prepare_cached("SELECT 1 FROM all_target_digests WHERE digest=?1 LIMIT 1")?;
        let exists = stmt
            .query_row(params![digest], |_| Ok(()))
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn was_transferred_from_source(
        &mut self,
        path: &Path,
        last_modified: SystemTime,
        size: u64,
    ) -> Result<bool> {
        let mut stmt = self.0.prepare_cached(
            "SELECT 1 FROM source_files \
             WHERE path=?1 AND mtime=?2 AND size=?3 LIMIT 1",
        )?;
        let exists = stmt
            .query_row(
                params![
                    path_to_text(path)?,
                    system_time_as_i64(last_modified)?,
                    size as i64
                ],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn mark_transferred_from_source(
        &mut self,
        path: &Path,
        digest: &Sha256Hash,
        last_modified: SystemTime,
        size: u64,
    ) -> Result<()> {
        self.0.execute(
            "INSERT INTO source_files (path, mtime, size, digest)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                path_to_text(path)?,
                system_time_as_i64(last_modified)?,
                size as i64,
                digest,
            ],
        )?;
        Ok(())
    }
}

fn system_time_as_i64(t: SystemTime) -> Result<i64> {
    Ok(t.duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| eyre!("system time before UNIX_EPOCH: {}", e))?
        .as_secs() as i64)
}

fn path_to_text(p: &Path) -> Result<String> {
    p.to_str()
        .map(|s| s.to_string())
        .wrap_err("could not convert to bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn dummy_digest(n: u8) -> Sha256Hash {
        Sha256Hash::new_for_tests(n)
    }

    #[test]
    fn full_roundtrip() {
        let mut store = PhotoSyncStore::new_for_tests().unwrap();

        let path = Path::new("/tmp/foo.jpg");
        let size = 1234u64;
        let now = SystemTime::now();
        let digest_a = dummy_digest(1);

        // Initially nothing exists.
        assert!(!store.exists_in_old_target(path, now, size).unwrap());
        assert!(!store.was_transferred_from_source(path, now, size).unwrap());
        assert!(!store.exists_in_target(&digest_a).unwrap());

        // Mark as already present in old target.
        store
            .mark_exists_in_old_target(path, now, size, &digest_a)
            .unwrap();
        assert!(store.exists_in_old_target(path, now, size).unwrap());
        assert!(store.exists_in_target(&digest_a).unwrap());

        // Different digest not yet present
        let digest_b = dummy_digest(2);
        assert!(!store.exists_in_target(&digest_b).unwrap());

        // Mark transfer from source
        let later = now + Duration::from_secs(10);
        let size2 = 5678u64;
        store
            .mark_transferred_from_source(path, &digest_b, later, size2)
            .unwrap();
        assert!(
            store
                .was_transferred_from_source(path, later, size2)
                .unwrap()
        );
        assert!(store.exists_in_target(&digest_b).unwrap());
    }
}
