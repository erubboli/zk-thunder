use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use uuid::Uuid;
use zksync_types::{
    l2_to_l1_log::L2ToL1Log,
    pubdata_da::{DataAvailabilityBlob, DataAvailabilityDetails},
    Address, L1BatchNumber,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperationStatus {
    Pending,
    InProgress,
    Completed,
    Failed(String),
}

impl Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            OperationStatus::Pending => "pending",
            OperationStatus::InProgress => "in_progress",
            OperationStatus::Completed => "completed",
            OperationStatus::Failed(_) => "failed",
        };

        write!(f, "{}", status)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OperationType {
    Commit,
    Proof,
    Execute,
}

impl OperationType {
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "commit" => Ok(Self::Commit),
            "proof" => Ok(Self::Proof),
            "execute" => Ok(Self::Execute),
            default => Err(format!("Unrecognized operation type: {}", default)),
        }
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Commit => write!(f, "commit"),
            Self::Proof => write!(f, "proof"),
            Self::Execute => write!(f, "execute"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingIpfsOperation {
    pub id: Uuid,
    pub operation_type: OperationType,
    pub data: Vec<u8>,
    pub attempts: u32,
    pub last_attempt: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub status: OperationStatus,
    pub ipfs_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMintlayerBatch {
    pub id: Uuid,
    pub ipfs_hashes: Vec<String>,
    pub attempts: u32,
    pub last_attempt: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub status: OperationStatus,
    pub tx_hash: Option<String>,
    pub group_ipfs_hash: Option<String>,
}

/// Represents a blob in the data availability layer.
#[derive(Debug, Clone)]
pub(crate) struct StorageDABlob {
    pub l1_batch_number: i64,
    pub dispatch_request_id: String,
    pub blob_id: Option<String>,
    pub inclusion_data: Option<Vec<u8>>,
    pub sent_at: NaiveDateTime,
}

impl From<StorageDABlob> for DataAvailabilityBlob {
    fn from(blob: StorageDABlob) -> DataAvailabilityBlob {
        DataAvailabilityBlob {
            l1_batch_number: L1BatchNumber(blob.l1_batch_number as u32),
            dispatch_request_id: blob.dispatch_request_id,
            blob_id: blob.blob_id,
            inclusion_data: blob.inclusion_data,
            sent_at: blob.sent_at.and_utc(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageDADetails {
    pub blob_id: String,
    pub client_type: Option<String>,
    pub inclusion_data: Option<Vec<u8>>,
    pub sent_at: NaiveDateTime,
    pub l2_da_validator_address: Option<Vec<u8>>,
}

impl From<StorageDADetails> for DataAvailabilityDetails {
    fn from(row: StorageDADetails) -> DataAvailabilityDetails {
        DataAvailabilityDetails {
            // safe to unwrap because query is guaranteed to return a non-null blob_id
            blob_id: row.blob_id,
            // safe to unwrap because the value in the database is assumed to be always correct
            pubdata_type: row.client_type.map(|t| t.parse().unwrap()),
            inclusion_data: row.inclusion_data,
            sent_at: row.sent_at.and_utc(),
            l2_da_validator: row
                .l2_da_validator_address
                .map(|addr| Address::from_slice(addr.as_slice())),
        }
    }
}

/// A small struct used to store a batch and its data availability, which are retrieved from the database.
#[derive(Debug)]
pub struct L1BatchDA {
    pub pubdata: Vec<u8>,
    pub l1_batch_number: L1BatchNumber,
    pub system_logs: Vec<L2ToL1Log>,
    pub sealed_at: DateTime<Utc>,
}
