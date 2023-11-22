// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{BackupRestoreMetadata, BackupRestoreOperator, METADATA_FILE_NAME};
use anyhow::{Context, Error};
use aptos_logger::{error, info};
use cloud_storage::{Bucket, Object};
use std::{
    env,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    time::sleep,
};
use tokio_tar::Builder;

const JSON_FILE_TYPE: &str = "application/json";
const TAR_FILE_TYPE: &str = "application/tar";
const SERVICE_ACCOUNT_ENV_VAR: &str = "SERVICE_ACCOUNT";

pub struct GcsBackupRestoreOperator {
    bucket_name: String,
    metadata_epoch: AtomicU64,
}

impl GcsBackupRestoreOperator {
    // pub fn new(bucket_name: String, service_account_path: String) -> Self {
    pub fn new(bucket_name: String) -> Self {
    // pub fn new(bucket_name: String, service_account_path: String) -> Self {
        // env::set_var(SERVICE_ACCOUNT_ENV_VAR, service_account_path);
        Self {
            bucket_name,
            metadata_epoch: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl BackupRestoreOperator for GcsBackupRestoreOperator {
    async fn verify_storage_bucket_existence(&self) {
        tracing::info!(
            bucket_name = self.bucket_name,
            "Before gcs operator starts, verify the bucket exists."
        );
        Bucket::read(&self.bucket_name)
            .await
            .expect("Failed to read bucket.");
    }

    async fn get_metadata(&self) -> Option<BackupRestoreMetadata> {
        match Object::download(&self.bucket_name, METADATA_FILE_NAME).await {
            Ok(metadata) => {
                let metadata: BackupRestoreMetadata =
                    serde_json::from_slice(&metadata).expect("Expected metadata to be valid JSON.");
                Some(metadata)
            },
            Err(cloud_storage::Error::Other(err)) => {
                if err.contains("No such object: ") {
                    None
                } else {
                    panic!(
                        "[Indexer Async V2 Backup & Restore] Error happens when accessing metadata file. {}",
                        err
                    );
                }
            },
            Err(e) => {
                panic!(
                    "[Indexer Async V2 Backup & Restore] Error happens when accessing metadata file. {}",
                    e
                );
            },
        }
    }

    async fn create_default_metadata_if_absent(
        &self,
        expected_chain_id: u64,
    ) -> anyhow::Result<BackupRestoreMetadata> {
        match Object::download(&self.bucket_name, METADATA_FILE_NAME).await {
            Ok(metadata) => {
                let metadata: BackupRestoreMetadata =
                    serde_json::from_slice(&metadata).expect("Expected metadata to be valid JSON.");
                anyhow::ensure!(metadata.chain_id == expected_chain_id, "Chain ID mismatch.");
                self.metadata_epoch.store(metadata.epoch, Ordering::Relaxed);
                Ok(metadata)
            },
            Err(cloud_storage::Error::Other(err)) => {
                let is_file_missing = err.contains("No such object: ");
                if is_file_missing {
                    self.update_metadata(expected_chain_id, 0)
                        .await
                        .expect("[Indexer Async V2 Backup & Restore] Update metadata failed.");
                    self.metadata_epoch.store(0, Ordering::Relaxed);
                    Ok(BackupRestoreMetadata::new(expected_chain_id, 0))
                } else {
                    Err(anyhow::Error::msg(format!(
                        "Metadata not found or gcs operator is not in write mode. {}",
                        err
                    )))
                }
            },
            Err(err) => Err(anyhow::Error::from(err)),
        }
    }

    async fn update_metadata(&self, chain_id: u64, epoch: u64) -> anyhow::Result<()> {
        let metadata = BackupRestoreMetadata::new(chain_id, epoch);
        let mut attempts = 0;
        let max_attempts = 5;
        let retry_delay = Duration::from_secs(10);

        loop {
            match Object::create(
                self.bucket_name.as_str(),
                serde_json::to_vec(&metadata).unwrap(),
                METADATA_FILE_NAME,
                JSON_FILE_TYPE,
            )
            .await
            {
                Ok(_) => {
                    self.metadata_epoch.store(epoch, Ordering::Relaxed);
                    return Ok(());
                },
                Err(cloud_storage::Error::Other(err)) => {
                    if err.contains("rateLimitExceeded") {
                        attempts += 1;
                        if attempts >= max_attempts {
                            anyhow::bail!(
                                "Failed to update the metadata.json file due to gcs single object rate limit: {}",
                                err
                            );
                        }
                        sleep(retry_delay).await;
                    }
                },
                Err(err) => {
                    anyhow::bail!("Failed to update metadata: {}", err);
                },
            }
        }
    }

    async fn upload_snapshot(
        &self,
        chain_id: u64,
        epoch: u64,
        snapshot_path: PathBuf,
    ) -> anyhow::Result<()> {
        let bucket_name = self.bucket_name.clone();

        let (tar_file, tar_file_name) = create_tar(snapshot_path, &epoch.to_string()).await?;
        let mut file = File::open(tar_file.clone())
            .await
            .context("Failed to open tar file")?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .context("Failed to read tar file")?;

        match Object::create(bucket_name.as_str(), buffer, &tar_file_name, TAR_FILE_TYPE).await {
            Ok(_) => {
                fs::remove_file(&tar_file.clone())
                    .await
                    .context("Failed to remove local tar file")?;
                self.update_metadata(chain_id, epoch).await?;
            },
            Err(err) => {
                anyhow::bail!("Failed to upload snapshot: {}", err);
            },
        }
        Ok(())
    }

    async fn restore_snapshot(&self, chain_id: u64, db_path: PathBuf) -> anyhow::Result<()> {
        let metadata = self.get_metadata().await;
        if metadata.is_none() {
            info!("trying to restore from gcs backup but metadata.json file does not exist");
            return Ok(());
        }
        let metadata = metadata.unwrap();
        anyhow::ensure!(metadata.chain_id == chain_id, "Chain ID mismatch.");
        let epoch = metadata.epoch;
        self.metadata_epoch.store(epoch, Ordering::Relaxed);
        let epoch_file_name = format!("{}.tar", epoch);

        match Object::download(&self.bucket_name, &epoch_file_name).await {
            Ok(snapshot) => {
                // Check if db_path exists, remove it if it does
                if db_path.exists() {
                    fs::remove_dir_all(&db_path).await?;
                }

                // Recreate db_path
                fs::create_dir_all(&db_path).await?;

                // Create a temporary file to write the snapshot data
                let temp_file_path = db_path.join("snapshot.tar");
                let mut temp_file = File::create(&temp_file_path).await?;
                temp_file.write_all(&snapshot).await?;
                temp_file.flush().await?; // Ensure all data is written
                drop(temp_file); // Close the file

                // Open the temporary file as a tar archive
                let file = File::open(&temp_file_path).await?;
                let mut archive = tokio_tar::Archive::new(file);

                // Unpack the archive into db_path
                match archive.unpack(&db_path).await {
                    Ok(_) => {},
                    Err(e) => {
                        error!("Failed to unpack archive: {:?}", e);
                    },
                }
                Ok(())
            },
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    fn get_metadata_epoch(&self) -> u64 {
        self.metadata_epoch.load(Ordering::Relaxed)
    }
}

async fn create_tar(dir_path: PathBuf, backup_file_name: &str) -> Result<(PathBuf, String), Error> {
    let tar_file_name = format!("{}.tar", backup_file_name);
    let mut tar_file_path = dir_path.clone();
    tar_file_path.set_file_name(&tar_file_name);
    let tar_file = File::create(&tar_file_path).await?;
    let tar_data = BufWriter::new(tar_file);
    let mut tar_builder = Builder::new(tar_data);
    tar_builder.append_dir_all(".", &dir_path).await?;
    drop(tar_builder.into_inner().await?);
    Ok((tar_file_path, tar_file_name))
}
