use zksync_db_connection::{
    connection::Connection,
    error::DalResult,
    instrument::{InstrumentExt, Instrumented},
};
use zksync_types::{
    commitment::PubdataType,
    l2_to_l1_log::L2ToL1Log,
    pubdata_da::{DataAvailabilityBlob, DataAvailabilityDetails},
    Address, L1BatchNumber,
};

use crate::{
    models::storage_data_availability::{L1BatchDA, StorageDABlob, StorageDADetails},
    Core,
};

pub use crate::models::storage_data_availability::{
    OperationStatus, OperationType, PendingIpfsOperation, PendingMintlayerBatch,
};

const MAX_RETRY_ATTEMPTS: i32 = 10;
const MAX_BATCH_SIZE: i64 = 100;

#[derive(Debug)]
pub struct DataAvailabilityDal<'a, 'c> {
    pub(crate) storage: &'a mut Connection<'c, Core>,
}

impl DataAvailabilityDal<'_, '_> {
    /// Inserts the blob_id for the given L1 batch.
    pub async fn insert_l1_batch_da(
        &mut self,
        number: L1BatchNumber,
        blob_id: &str,
        sent_at: chrono::NaiveDateTime,
        pubdata_type: PubdataType,
        da_inclusion_data: Option<&[u8]>,
        l2_validator_address: Option<Address>,
    ) -> DalResult<()> {
        let instrumentation = Instrumented::new("insert_l1_batch_da")
            .with_arg("number", &number)
            .with_arg("blob_id", &blob_id)
            .with_arg("sent_at", &sent_at)
            .with_arg("pubdata_type", &pubdata_type)
            .with_arg("da_inclusion_data", &da_inclusion_data)
            .with_arg("l2_validator_address", &l2_validator_address);

        let query = sqlx::query!(
            r#"
            INSERT INTO
            data_availability (
                l1_batch_number,
                blob_id,
                inclusion_data,
                client_type,
                l2_da_validator_address,
                sent_at,
                created_at,
                updated_at
            )
            VALUES
            ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT DO NOTHING
            "#,
            i64::from(number.0),
            blob_id,
            da_inclusion_data,
            pubdata_type.to_string(),
            l2_validator_address.map(|addr| addr.as_bytes().to_vec()),
            sent_at,
        );

        let update_result = instrumentation
            .clone()
            .with(query)
            .report_latency()
            .execute(self.storage)
            .await?;

        if update_result.rows_affected() == 0 {
            let err = instrumentation.constraint_error(anyhow::anyhow!(
                "L1 batch #{number}: batch DA was attempted to be inserted twice"
            ));
            return Err(err);
        }

        Ok(())
    }

    /// Inserts the dispatch request id and basic fields for the given L1 batch
    pub async fn insert_l1_batch_da_request_id(
        &mut self,
        number: L1BatchNumber,
        dispatch_request_id: &str,
        sent_at: chrono::NaiveDateTime,
        pubdata_type: PubdataType,
        l2_validator_address: Option<Address>,
    ) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            INSERT INTO
            data_availability (
                l1_batch_number,
                dispatch_request_id,
                client_type,
                l2_da_validator_address,
                sent_at,
                created_at,
                updated_at
            )
            VALUES
            ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT DO NOTHING
            "#,
            i64::from(number.0),
            dispatch_request_id,
            pubdata_type.to_string(),
            l2_validator_address.map(|addr| addr.as_bytes().to_vec()),
            sent_at,
        )
        .instrument("insert_l1_batch_da_request_id")
        .with_arg("number", &number)
        .with_arg("dispatch_request_id", &dispatch_request_id)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::error!(
                "L1 batch #{number}: batch DA with request_id was attempted to be inserted twice"
            );
        }

        Ok(())
    }

    /// Saves the inclusion data for the given L1 batch. If the inclusion data is already present,
    /// verifies that it matches the one provided in the function arguments
    /// (meaning that the inclusion data corresponds to the same DA blob)
    pub async fn save_l1_batch_inclusion_data(
        &mut self,
        number: L1BatchNumber,
        da_inclusion_data: &[u8],
    ) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            UPDATE data_availability
            SET
                inclusion_data = $1,
                updated_at = NOW()
            WHERE
                l1_batch_number = $2
                AND inclusion_data IS NULL
            "#,
            da_inclusion_data,
            i64::from(number.0),
        )
        .instrument("save_l1_batch_da_data")
        .with_arg("number", &number)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::debug!("L1 batch #{number}: DA data wasn't updated as it's already present");

            let instrumentation =
                Instrumented::new("get_matching_batch_da_data").with_arg("number", &number);

            // Batch was already processed. Verify that existing DA data matches
            let query = sqlx::query!(
                r#"
                SELECT
                    inclusion_data
                FROM
                    data_availability
                WHERE
                    l1_batch_number = $1
                "#,
                i64::from(number.0),
            );

            let matched: Option<Vec<u8>> = instrumentation
                .clone()
                .with(query)
                .report_latency()
                .fetch_one(self.storage)
                .await?
                .inclusion_data;

            if matched.unwrap_or_default() != da_inclusion_data.to_vec() {
                let err = instrumentation.constraint_error(anyhow::anyhow!(
                    "Error storing DA inclusion data. DA data for L1 batch #{number} does not match the one provided before"
                ));
                return Err(err);
            }
        }
        Ok(())
    }

    pub async fn get_first_da_blob_awaiting_finality(
        &mut self,
    ) -> DalResult<Option<DataAvailabilityBlob>> {
        Ok(sqlx::query_as!(
            StorageDABlob,
            r#"
            SELECT
                l1_batch_number,
                dispatch_request_id,
                blob_id,
                inclusion_data,
                sent_at
            FROM
                data_availability
            WHERE
                blob_id IS NULL
            ORDER BY
                l1_batch_number
            LIMIT
                1
            "#,
        )
        .instrument("get_first_da_blob_awaiting_finality")
        .fetch_optional(self.storage)
        .await?
        .map(DataAvailabilityBlob::from))
    }

    pub async fn set_blob_id(&mut self, number: L1BatchNumber, blob_id: &str) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            UPDATE data_availability
            SET
                blob_id = $1,
                updated_at = NOW()
            WHERE
                l1_batch_number = $2
            "#,
            blob_id,
            i64::from(number.0),
        )
        .instrument("set_blob_id")
        .with_arg("number", &number)
        .with_arg("blob_id", &blob_id)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::error!("L1 batch #{number}: blob_id wasn't updated");
        }
        Ok(())
    }

    pub async fn remove_data_availability_entry(&mut self, number: L1BatchNumber) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            DELETE FROM data_availability
            WHERE
                l1_batch_number = $1
            "#,
            i64::from(number.0),
        )
        .instrument("remove_data_availability_entry")
        .with_arg("number", &number)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::error!("L1 batch #{number}: data_availability entry wasn't removed");
        }
        Ok(())
    }

    /// Assumes that the L1 batches are sorted by number, and returns the first one that is ready for DA dispatch.
    pub async fn get_first_da_blob_awaiting_inclusion(
        &mut self,
    ) -> DalResult<Option<DataAvailabilityBlob>> {
        Ok(sqlx::query_as!(
            StorageDABlob,
            r#"
            SELECT
                l1_batch_number,
                blob_id,
                dispatch_request_id,
                inclusion_data,
                sent_at
            FROM
                data_availability
            WHERE
                inclusion_data IS NULL
                AND blob_id IS NOT NULL
            ORDER BY
                l1_batch_number
            LIMIT
                1
            "#,
        )
        .instrument("get_first_da_blob_awaiting_inclusion")
        .fetch_optional(self.storage)
        .await?
        .map(DataAvailabilityBlob::from))
    }

    /// Fetches the pubdata and `l1_batch_number` for the L1 batches that are ready for DA dispatch.
    pub async fn get_ready_for_da_dispatch_l1_batches(
        &mut self,
        limit: usize,
    ) -> DalResult<Vec<L1BatchDA>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                number,
                pubdata_input,
                system_logs,
                sealed_at
            FROM
                l1_batches
            LEFT JOIN
                data_availability
                ON data_availability.l1_batch_number = l1_batches.number
            WHERE
                eth_commit_tx_id IS NULL
                AND number != 0
                AND data_availability.dispatch_request_id IS NULL
                AND pubdata_input IS NOT NULL
                AND sealed_at IS NOT NULL
            ORDER BY
                number
            LIMIT
                $1
            "#,
            limit as i64,
        )
        .instrument("get_ready_for_da_dispatch_l1_batches")
        .with_arg("limit", &limit)
        .fetch_all(self.storage)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| L1BatchDA {
                // `unwrap` is safe here because we have a `WHERE` clause that filters out `NULL` values
                pubdata: row.pubdata_input.unwrap(),
                l1_batch_number: L1BatchNumber(row.number as u32),
                sealed_at: row.sealed_at.unwrap().and_utc(),
                system_logs: row
                    .system_logs
                    .into_iter()
                    .map(|raw_log| L2ToL1Log::from_slice(&raw_log))
                    .collect(),
            })
            .collect())
    }

    pub async fn get_da_details_by_batch_number(
        &mut self,
        number: L1BatchNumber,
    ) -> DalResult<Option<DataAvailabilityDetails>> {
        Ok(sqlx::query_as!(
            StorageDADetails,
            r#"
            SELECT
                blob_id AS "blob_id!",
                client_type,
                inclusion_data,
                sent_at,
                l2_da_validator_address
            FROM
                data_availability
            WHERE
                l1_batch_number = $1
                AND blob_id IS NOT NULL
            "#,
            i64::from(number.0),
        )
        .instrument("get_da_details_by_batch_number")
        .with_arg("number", &number)
        .fetch_optional(self.storage)
        .await?
        .map(DataAvailabilityDetails::from))
    }

    pub async fn get_latest_batch_with_inclusion_data(
        &mut self,
        last_processed_batch: L1BatchNumber,
    ) -> DalResult<Option<L1BatchNumber>> {
        let row = sqlx::query!(
            r#"
            SELECT
                l1_batch_number
            FROM
                data_availability
            WHERE
                inclusion_data IS NOT NULL
                AND l1_batch_number >= $1
            ORDER BY
                l1_batch_number DESC
            LIMIT
                1
            "#,
            i64::from(last_processed_batch.0),
        )
        .instrument("get_latest_batch_with_inclusion_data")
        .report_latency()
        .fetch_optional(self.storage)
        .await?;

        Ok(row.map(|row| L1BatchNumber(row.l1_batch_number as u32)))
    }

    pub async fn set_dummy_inclusion_data_for_old_batches(
        &mut self,
        current_l2_da_validator: Address,
    ) -> DalResult<()> {
        sqlx::query!(
            r#"
            UPDATE data_availability
            SET
                inclusion_data = $1,
                updated_at = NOW()
            WHERE
                inclusion_data IS NULL
                AND (l2_da_validator_address IS NULL OR l2_da_validator_address != $2)
            "#,
            vec![],
            current_l2_da_validator.as_bytes(),
        )
        .instrument("set_dummy_inclusion_data_for_old_batches")
        .report_latency()
        .execute(self.storage)
        .await?;

        Ok(())
    }

    pub async fn l1_batch_missing_data_availability(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> DalResult<bool> {
        let row = sqlx::query!(
            r#"
            SELECT (
                COUNT(*) = 0 AND
                (
                    SELECT (miniblocks.pubdata_type != 'Rollup')
                    FROM miniblocks
                    WHERE miniblocks.l1_batch_number = $1
                    ORDER BY miniblocks.number
                    LIMIT 1
                )
            ) AS "da_is_missing"
            FROM data_availability
            WHERE l1_batch_number = $1
            "#,
            i64::from(l1_batch_number.0),
        )
        .instrument("l1_batch_missing_data_availability")
        .fetch_optional(self.storage)
        .await?;

        let Some(row) = row else {
            return Ok(false);
        };

        Ok(row.da_is_missing.unwrap_or(false))
    }

    pub async fn remove_batches_without_inclusion_data(&mut self) -> DalResult<()> {
        sqlx::query!(
            r#"
            DELETE FROM data_availability
            WHERE inclusion_data IS NULL
            "#,
        )
        .instrument("remove_batches_without_inclusion_data")
        .report_latency()
        .execute(self.storage)
        .await?;

        Ok(())
    }

    pub async fn get_pending_ipfs_operations(self) -> DalResult<Vec<PendingIpfsOperation>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id, operation_type, data, attempts,
                last_attempt as "last_attempt", created_at,
                status::text as "status!", ipfs_hash
            FROM pending_ipfs_operations
            WHERE status::text = 'pending'
            OR (status::text = 'failed' AND attempts < $1)
            ORDER BY created_at ASC
            LIMIT $2
            "#,
            MAX_RETRY_ATTEMPTS,
            MAX_BATCH_SIZE
        )
        .instrument("get_pending_ipfs_operations")
        .with_arg("MAX_RETRY_ATTEMPTS", &MAX_RETRY_ATTEMPTS)
        .with_arg("MAX_BATCH_SIZE", &MAX_BATCH_SIZE)
        .fetch_all(self.storage)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PendingIpfsOperation {
                    id: row.id,
                    operation_type: OperationType::from_str(&row.operation_type)
                        .expect("Invalid operation_type"),
                    data: row.data,
                    attempts: row.attempts as u32,
                    last_attempt: row.last_attempt,
                    created_at: row.created_at,
                    status: match row.status.as_str() {
                        "pending" => OperationStatus::Pending,
                        "in_progress" => OperationStatus::InProgress,
                        "completed" => OperationStatus::Completed,
                        "failed" => OperationStatus::Failed("".to_string()),
                        _ => panic!("Invalid operation_type"),
                    },
                    ipfs_hash: row.ipfs_hash,
                })
            })
            .collect()
    }

    pub async fn get_pending_mintlayer_batches(self) -> DalResult<Vec<PendingMintlayerBatch>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id, ipfs_hashes, attempts, last_attempt,
                created_at, status::text as "status!", group_ipfs_hash, tx_hash
            FROM pending_mintlayer_batches
            WHERE status::text = 'pending'
            OR (status::text = 'failed' AND attempts < $1)
            ORDER BY created_at ASC
            LIMIT $2
            "#,
            MAX_RETRY_ATTEMPTS,
            MAX_BATCH_SIZE
        )
        .instrument("get_pending_mintlayer_batches")
        .with_arg("MAX_RETRY_ATTEMPTS", &MAX_RETRY_ATTEMPTS)
        .with_arg("MAX_BATCH_SIZE", &MAX_BATCH_SIZE)
        .fetch_all(self.storage)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PendingMintlayerBatch {
                    id: row.id,
                    ipfs_hashes: row.ipfs_hashes,
                    attempts: row.attempts as u32,
                    last_attempt: row.last_attempt,
                    created_at: row.created_at,
                    status: match row.status.as_str() {
                        "pending" => OperationStatus::Pending,
                        "in_progress" => OperationStatus::InProgress,
                        "completed" => OperationStatus::Completed,
                        "failed" => OperationStatus::Failed("".to_string()),
                        _ => panic!("Invalid operation_status"),
                    },
                    group_ipfs_hash: row.group_ipfs_hash,
                    tx_hash: row.tx_hash,
                })
            })
            .collect()
    }

    pub async fn update_ipfs_operations<'a>(self, op: &PendingIpfsOperation) -> DalResult<()> {
        sqlx::query!(
            r#"
            UPDATE pending_ipfs_operations
            SET status = $1::text::operation_status, attempts = $2, last_attempt = $3, ipfs_hash = $4
            WHERE id = $5
            "#,
            op.status.to_string(),
            op.attempts as i32,
            op.last_attempt,
            op.ipfs_hash,
            op.id
        ).instrument("update_ipfs_operations")
        .with_arg("status", &op.status.to_string())
        .with_arg("attempts", &op.attempts)
        .with_arg("last_attempt", &op.last_attempt)
        .with_arg("ipfs_hash", &op.ipfs_hash)
        .with_arg("id", &op.id)
        .execute(self.storage)
        .await?;
        Ok(())
    }

    pub async fn update_mintlayer_batch<'a>(self, batch: &PendingMintlayerBatch) -> DalResult<()> {
        sqlx::query!(
            r#"
            INSERT INTO pending_mintlayer_batches (
                id, ipfs_hashes, status, attempts, last_attempt, created_at, group_ipfs_hash
            ) VALUES ($1, $2, $3::text::operation_status, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                ipfs_hashes = $2,
                status = $3::text::operation_status,
                group_ipfs_hash = $7
            "#,
            batch.id,
            &batch.ipfs_hashes,
            batch.status.to_string(),
            batch.attempts as i32,
            batch.last_attempt,
            batch.created_at,
            batch.group_ipfs_hash,
        )
        .instrument("update_mintlayer_batch")
        .with_arg("id", &batch.id)
        .with_arg("ipfs_hashes", &batch.ipfs_hashes)
        .with_arg("status", &batch.status)
        .with_arg("attempts", &batch.attempts)
        .with_arg("last_attempt", &batch.last_attempt)
        .with_arg("created_at", &batch.created_at)
        .with_arg("group_ipfs_hash", &batch.group_ipfs_hash)
        .execute(self.storage)
        .await?;
        Ok(())
    }

    pub async fn cleanup_old_operations(self, days_old: i32) -> DalResult<()> {
        let mut tx = self.storage.start_transaction().await?;

        sqlx::query!(
            r#"
        DELETE FROM pending_ipfs_operations
        WHERE created_at < NOW() - make_interval(days => $1)
        AND (status = 'completed' OR attempts >= $2)
        "#,
            days_old,
            MAX_RETRY_ATTEMPTS
        )
        .instrument("cleanup_old_operations")
        .with_arg("days old", &days_old)
        .with_arg("MAX_RETRY_ATTEMPTS", &MAX_RETRY_ATTEMPTS)
        .execute(&mut tx)
        .await?;

        sqlx::query!(
            r#"
        DELETE FROM pending_mintlayer_batches
        WHERE created_at < NOW() - make_interval(days => $1)
        AND (status = 'completed' OR attempts >= $2)
        "#,
            days_old,
            MAX_RETRY_ATTEMPTS
        )
        .instrument("cleanup_old_operations")
        .with_arg("days old", &days_old)
        .with_arg("MAX_RETRY_ATTEMPTS", &MAX_RETRY_ATTEMPTS)
        .execute(&mut tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn save_pending_operation(self, op: &PendingIpfsOperation) -> DalResult<()> {
        sqlx::query!(
            r#"
            INSERT INTO pending_ipfs_operations (
                id, operation_type, data, attempts,
                last_attempt, created_at, status, ipfs_hash
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::text::operation_status, $8)
            "#,
            op.id,
            op.operation_type.to_string(),
            op.data,
            op.attempts as i32,
            op.last_attempt,
            op.created_at,
            op.status.to_string(),
            op.ipfs_hash
        )
        .instrument("save_pending_operation")
        .with_arg("id", &op.id)
        .with_arg("operation type", &op.operation_type)
        .with_arg("data", &op.data)
        .with_arg("attempts", &op.attempts)
        .with_arg("last attempt", &op.last_attempt)
        .with_arg("created at", &op.created_at)
        .with_arg("status", &op.status)
        .with_arg("ipfs hash", &op.ipfs_hash)
        .execute(self.storage)
        .await?;
        Ok(())
    }
}
