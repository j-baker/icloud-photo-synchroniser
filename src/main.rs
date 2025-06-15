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
use eyre::Result;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tempfile::NamedTempFile;
use walkdir::WalkDir;

use crate::{
    digest::{DigestWriter, digest},
    store::PhotoSyncStore,
};

mod digest;
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

    println!("store successfully created");

    // first, we make sure that the old out directory has been properly indexed,
    // so all of its files have been hashed and recorded.
    ensure_old_out_dir_properly_indexed(&mut store, &args.old_out_dir)?;

    let new_files = detect_new_files(&mut store, &args.in_dir)?;

    transfer_new_files(
        &mut store,
        &args.in_dir,
        &args.out_dir,
        &new_files,
        &args.temp_dir,
    )?;

    Ok(())
}

fn ensure_old_out_dir_properly_indexed(
    store: &mut PhotoSyncStore,
    old_out_dir: &Path,
) -> Result<()> {
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
        if !store.lock().unwrap().exists_in_old_target(
            &path,
            metadata.modified()?,
            metadata.size(),
        )? {
            let digest = digest(&full_path)?;
            bytes_processed.fetch_add(size, Ordering::SeqCst);
            store
                .lock()
                .unwrap()
                .mark_exists_in_old_target(&path, last_modified, size, &digest)?;
        }

        Ok::<_, eyre::Error>(())
    })?;

    println!("finished phase 1: ensuring old data hashed");
    Ok(())
}

fn detect_new_files(store: &mut PhotoSyncStore, in_dir: &Path) -> Result<Vec<PathBuf>> {
    println!("starting phase 2: detecting new files");
    let mut result = Vec::new();
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
        if !store.was_transferred_from_source(&path, last_modified, size)? {
            result.push(path);
        }
        total_processed += 1;
        if total_processed % 100 == 0 {
            println!(
                "processed {total_processed} files from source, of which {} will be transferred",
                result.len()
            );
        }
    }
    println!("finished phase 2: detecting new files");
    Ok(result)
}

fn transfer_new_files(
    store: &mut PhotoSyncStore,
    in_dir: &Path,
    out_dir: &Path,
    files: &[PathBuf],
    temp_dir: &Path,
) -> Result<()> {
    println!("starting phase 3: transferring new files");
    let file_count = files.len();
    let mut bytes_stored = 0;
    let mut bytes_considered = 0;
    for (idx, path) in files.iter().enumerate() {
        let in_path = in_dir.join(path);
        let mut in_data = File::open(&in_path)?;

        let file_metadata = fs::metadata(in_path)?;
        let size = file_metadata.len();
        bytes_considered += size;

        let mut temp_path = NamedTempFile::new_in(temp_dir)?;
        let out_path = out_dir.join(path);

        let mut writer = DigestWriter::new(temp_path.as_file_mut());
        let maybe_err = io::copy(&mut in_data, &mut writer);
        if maybe_err.is_err() {
            continue;
        }

        let digest = writer.finalise()?;

        let already_exists = store.exists_in_target(&digest)?;

        if !already_exists {
            temp_path.persist_noclobber(out_path)?;
            bytes_stored += size;
        }
        store.mark_transferred_from_source(path, &digest, file_metadata.modified()?, size)?;

        if idx % 10 == 0 {
            println!(
                "processed {idx} files overall of {file_count}, added {}MB of {}MB considered",
                bytes_stored / 1_000_000,
                bytes_considered / 1_000_000
            );
        }
    }

    println!("finished phase 3: transferring new files");

    Ok(())
}
