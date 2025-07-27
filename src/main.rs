use std::{
    fs::{self, File},
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use clap::Parser;
use eyre::{Result, ensure};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::os::unix::fs::PermissionsExt;
use tempfile::NamedTempFile;
use walkdir::WalkDir;

use crate::{
    digest::{DigestWriter, digest},
    sau64::SimpleAtomicU64,
    store::{PhotoSyncStore, WasTransferredFromSourceResult},
};

mod digest;
mod sau64;
mod store;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long)]
    in_dir: PathBuf,
    #[clap(long)]
    out_dir: PathBuf,
    #[clap(long)]
    old_out_dir: PathBuf,
    #[clap(long)]
    database_file: PathBuf,
    #[clap(long)]
    temp_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("starting syncing with configuration: {args:?}");

    let mut store = PhotoSyncStore::new(args.database_file)?;

    store.ensure_schema()?;

    let store = store;

    println!("store successfully created");

    // first, we make sure that the old out directory has been properly indexed,
    // so all of its files have been hashed and recorded.
    ensure_old_out_dir_properly_indexed(&store, &args.old_out_dir)?;

    let new_files = detect_new_files(&store, &args.in_dir)?;

    transfer_new_files(
        &store,
        &args.in_dir,
        &args.out_dir,
        &new_files,
        &args.temp_dir,
    )?;

    Ok(())
}

fn ensure_old_out_dir_properly_indexed(store: &PhotoSyncStore, old_out_dir: &Path) -> Result<()> {
    println!("starting phase 1: ensuring old data hashed");
    let store = Mutex::new(store);
    let mut paths = Vec::new();
    for path in WalkDir::new(old_out_dir) {
        let path = path?;
        let file_type = path.file_type();
        if !file_type.is_file() {
            continue;
        }
        let path = path.path();
        let path = path.strip_prefix(old_out_dir)?;
        paths.push(path.to_path_buf());
    }
    let total_files = paths.len();
    let bytes_processed = AtomicU64::new(0);
    let files_processed = AtomicUsize::new(0);
    paths.into_par_iter().try_for_each(|path| {
        let processed = files_processed.fetch_add(1, Ordering::SeqCst);
        if processed % 100 == 0 {
            println!(
                "processed {processed} of {total_files} files, have hashed {}MB",
                bytes_processed.load(Ordering::SeqCst) / 1_000_000
            );
        }
        let full_path = old_out_dir.join(&path);

        let metadata = fs::metadata(&full_path)?;
        let last_modified = metadata.modified()?;
        let size = metadata.size();

        let exists_in_old_target = store.lock().unwrap().exists_in_old_target(
            &path,
            metadata.modified()?,
            metadata.size(),
        )?;
        match exists_in_old_target {
            WasTransferredFromSourceResult::New => {
                let digest = digest(&full_path)?;
                bytes_processed.fetch_add(size, Ordering::SeqCst);
                store.lock().unwrap().mark_exists_in_old_target(
                    &path,
                    last_modified,
                    size,
                    &digest,
                )?;
            }
            WasTransferredFromSourceResult::Transferred => {}
            WasTransferredFromSourceResult::NewMetadata {
                last_modified,
                size,
                digest: old_digest,
            } => {
                let new_digest = digest(&full_path)?;
                ensure!(
                    old_digest == new_digest,
                    "unexpected rewrite of file {full_path:?}, digest changed"
                );
                bytes_processed.fetch_add(size, Ordering::SeqCst);
                store.lock().unwrap().mark_exists_in_old_target(
                    &path,
                    last_modified,
                    size,
                    &new_digest,
                )?;
            }
        }

        Ok::<_, eyre::Error>(())
    })?;

    println!("finished phase 1: ensuring old data hashed");
    Ok(())
}

fn detect_new_files(store: &PhotoSyncStore, in_dir: &Path) -> Result<Vec<PathBuf>> {
    println!("starting phase 2: detecting new files");
    let mut result = Vec::new();
    let mut failures = Vec::new();
    let mut total_processed = 0;
    for path in WalkDir::new(in_dir) {
        let path = path?;
        if path.file_type().is_dir() {
            continue;
        }
        let metadata = path.metadata()?;
        let last_modified = metadata.modified()?;
        let size = metadata.len();
        let path = path.path().strip_prefix(in_dir)?.to_path_buf();
        match store.was_transferred_from_source(&path, last_modified, size)? {
            WasTransferredFromSourceResult::New => result.push(path),
            WasTransferredFromSourceResult::Transferred => {}
            WasTransferredFromSourceResult::NewMetadata {
                last_modified: old_last_modified,
                size: old_size,
                ..
            } => {
                println!(
                    "file {path:?} was already transferred but with a different size ({old_size} vs {size}) or last modified ({old_last_modified:?} vs {last_modified:?}). skipping for manual intervention."
                );
                failures.push(path);
            }
        }
        total_processed += 1;
        if total_processed % 100 == 0 {
            println!(
                "processed {total_processed} files from source, of which {} will be transferred",
                result.len()
            );
        }
    }
    println!("files for which metadata has changed between old and new:");
    for path in failures {
        println!("    {path:?}");
    }
    println!("finished phase 2: detecting new files");
    Ok(result)
}

enum FileOutcome {
    Success,
    FailedToOpen(PathBuf),
    FailedToCopy(PathBuf),
}

fn transfer_new_files(
    store: &PhotoSyncStore,
    in_dir: &Path,
    out_dir: &Path,
    files: &[PathBuf],
    temp_dir: &Path,
) -> Result<()> {
    println!("starting phase 3: transferring new files");
    let file_count = files.len();
    let files_considered = SimpleAtomicU64::default();
    let bytes_stored = SimpleAtomicU64::default();
    let bytes_considered = SimpleAtomicU64::default();

    let results: Result<Vec<_>> = files.into_par_iter().map(|path| {
        let in_path = in_dir.join(path);
        let in_data = File::open(&in_path);

        // errors on first open are tolerated - the file is just skipped.
        let mut in_data = match in_data {
            Ok(f) => f,
            Err(e) => {
                println!("error when opening {in_path:?}. Skipping and moving on. {e}");
                return Ok(FileOutcome::FailedToOpen(in_path));
            }
        };

        let file_metadata = fs::metadata(&in_path)?;
        let size = file_metadata.len();
        bytes_considered.fetch_add(size);

        let mut temp_path = NamedTempFile::new_in(temp_dir)?;
        let out_path = out_dir.join(path);

        let mut writer = DigestWriter::new(temp_path.as_file_mut());
        let maybe_err = io::copy(&mut in_data, &mut writer);
        if let Err(e) = maybe_err {
            println!("failed to copy bytes of file {in_path:?}: {e}");
            return Ok(FileOutcome::FailedToCopy(in_path));
        }

        let digest = writer.finalise()?;

        let already_exists = store.exists_in_target(&digest)?;

        if !already_exists {
            temp_path.persist_noclobber(&out_path)?;
            bytes_stored.fetch_add(size);
            fs::set_permissions(out_path, fs::Permissions::from_mode(0o644))?;
        }

        store.mark_transferred_from_source(path, &digest, file_metadata.modified()?, size)?;

        let files_considered = files_considered.fetch_add(1);

        if files_considered % 10 == 0 {
            println!(
                "processed {files_considered} files overall of {file_count}, added {}MB of {}MB considered",
                bytes_stored.as_u64() / 1_000_000,
                bytes_considered.as_u64() / 1_000_000
            );
        }
        Ok(FileOutcome::Success)
    }).collect();
    let results = results?;

    println!("could not transfer the following files:");
    results
        .into_iter()
        .filter_map(|x| match x {
            FileOutcome::Success => None,
            FileOutcome::FailedToOpen(path_buf) => Some(path_buf),
            FileOutcome::FailedToCopy(path_buf) => Some(path_buf),
        })
        .for_each(|path| println!("    {path:?}"));

    println!("finished phase 3: transferring new files");

    Ok(())
}
