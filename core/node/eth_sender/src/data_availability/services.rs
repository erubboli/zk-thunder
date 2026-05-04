use async_trait::async_trait;
use base64::Engine;
use std::fmt::Debug;
use std::time::Duration;

use super::{config::{IPFSConfig, MintlayerConfig}, error::DataAvailabilityError};

#[async_trait]
pub trait IPFSService: Send + Sync + Debug {
    async fn upload(&self, data: &[u8]) -> Result<String, DataAvailabilityError>;
}

#[async_trait]
pub trait MintlayerService: Send + Sync + Debug {
    async fn initialize_wallet(&self) -> Result<(), DataAvailabilityError>;
    async fn submit_hashes(&self, ipfs_hashes: &[String]) -> Result<String, DataAvailabilityError>;
}

#[derive(Debug)]
pub struct KuboIPFSService {
    api_url: String,
    client: reqwest::Client,
}

impl KuboIPFSService {
    pub fn new(config: IPFSConfig) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to build IPFS HTTP client");
        Self { api_url: config.api_url, client }
    }
}

#[async_trait]
impl IPFSService for KuboIPFSService {
    async fn upload(&self, data: &[u8]) -> Result<String, DataAvailabilityError> {
        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name(format!("op_{}", uuid::Uuid::new_v4()))
            .mime_str("application/octet-stream")
            .map_err(|e| DataAvailabilityError::IPFSError(e.to_string()))?;
        let form = reqwest::multipart::Form::new().part("file", part);

        let response = self.client
            .post(format!("{}/api/v0/add", self.api_url))
            .multipart(form)
            .send()
            .await
            .map_err(|e| DataAvailabilityError::IPFSError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(DataAvailabilityError::IPFSError(
                format!("Upload failed: {}", response.status())
            ));
        }

        let json: serde_json::Value = response.json().await
            .map_err(|e| DataAvailabilityError::IPFSError(e.to_string()))?;

        let cid = json["Hash"].as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| DataAvailabilityError::IPFSError("Missing Hash in response".into()))?;

        if !cid.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.')) {
            return Err(DataAvailabilityError::IPFSError(format!("Invalid CID returned by IPFS: {}", cid)));
        }

        // Verify: download back and compare to detect any storage corruption or CID mismatch
        let verify = self.client
            .post(format!("{}/api/v0/cat?arg={}", self.api_url, cid))
            .send()
            .await
            .map_err(|e| DataAvailabilityError::IPFSError(format!("CID verification request failed: {}", e)))?;

        if !verify.status().is_success() {
            return Err(DataAvailabilityError::IPFSError(
                format!("CID verification failed with status: {}", verify.status())
            ));
        }

        let downloaded = verify.bytes().await
            .map_err(|e| DataAvailabilityError::IPFSError(format!("CID verification read failed: {}", e)))?;

        if downloaded.as_ref() != data {
            return Err(DataAvailabilityError::IPFSError(
                "CID verification failed: downloaded content does not match uploaded data".into()
            ));
        }

        Ok(cid)
    }
}

#[derive(Debug)]
pub struct MintlayerRpcService {
    config: MintlayerConfig,
    client: reqwest::Client,
}

impl MintlayerRpcService {
    pub fn new(config: MintlayerConfig) -> Self {
        if config.mnemonic.is_some() && !config.rpc_url.starts_with("https://") {
            tracing::warn!(
                "ML_RPC_URL uses plaintext HTTP while a mnemonic is configured; \
                 use https:// to protect credentials in transit"
            );
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build Mintlayer HTTP client");
        Self { config, client }
    }

    fn get_auth_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        
        if let (Some(username), Some(password)) = (&self.config.rpc_username, &self.config.rpc_password) {
            let creds = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", username, password));
            headers.insert(
                "Authorization",
                format!("Basic {}", creds).parse().unwrap(),
            );
        }
        
        headers
    }
}

#[async_trait]
impl MintlayerService for MintlayerRpcService {
    async fn initialize_wallet(&self) -> Result<(), DataAvailabilityError> {
        let headers = self.get_auth_headers();
        
        // Create wallet
        let payload = match &self.config.mnemonic {
            Some(mnemonic) => serde_json::json!({
                "method": "wallet_create",
                "params": {
                    "path": self.config.wallet_path,
                    "store_seed_phrase": true,
                    "mnemonic": mnemonic
                },
                "jsonrpc": "2.0",
                "id": 1,
            }),
            None => serde_json::json!({
                "method": "wallet_create",
                "params": {
                    "path": self.config.wallet_path,
                    "store_seed_phrase": true
                },
                "jsonrpc": "2.0",
                "id": 1,
            }),
        };

        match self.client
            .post(&self.config.rpc_url)
            .headers(headers.clone())
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) if !resp.status().is_success() => {
                tracing::debug!("wallet_create returned {} (wallet may already exist)", resp.status());
            }
            Err(e) => {
                tracing::debug!("wallet_create request failed (wallet may already exist): {}", e);
            }
            Ok(_) => {}
        }
        
        // Open wallet
        let payload = serde_json::json!({
            "method": "wallet_open",
            "params": {
                "path": self.config.wallet_path,
            },
            "jsonrpc": "2.0",
            "id": 1,
        });

        let response = self.client
            .post(&self.config.rpc_url)
            .headers(headers.clone())
            .json(&payload)
            .send()
            .await
            .map_err(|e| DataAvailabilityError::MintlayerError(format!("Failed to open wallet: {}", e)))?;
            
        if !response.status().is_success() {
            return Err(DataAvailabilityError::MintlayerError(
                format!("Failed to open wallet: {}", response.status())
            ));
        }

        // Create address
        let payload = serde_json::json!({
            "method": "address_new",
            "params": {
                "account": 0,
            },
            "jsonrpc": "2.0",
            "id": 1,
        });

        let response = self.client
            .post(&self.config.rpc_url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| DataAvailabilityError::MintlayerError(format!("Failed to create address: {}", e)))?;
            
        if !response.status().is_success() {
            return Err(DataAvailabilityError::MintlayerError(
                format!("Failed to create address: {}", response.status())
            ));
        }

        Ok(())
    }

    async fn submit_hashes(&self, ipfs_hashes: &[String]) -> Result<String, DataAvailabilityError> {
        let headers = self.get_auth_headers();
        
        let payload = serde_json::json!({
            "method": "address_deposit_data",
            "params": {
                "data": hex::encode(ipfs_hashes.join(",")),
                "account": 0,
                "options": {},
            },
            "jsonrpc": "2.0",
            "id": 1,
        });

        let response = self.client
            .post(&self.config.rpc_url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| DataAvailabilityError::MintlayerError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(DataAvailabilityError::MintlayerError(format!(
                "Request failed with status: {}",
                response.status()
            )));
        }

        let response_text = response
            .text()
            .await
            .map_err(|e| DataAvailabilityError::MintlayerError(e.to_string()))?;
        let response_json: serde_json::Value = serde_json::from_str(&response_text)
            .map_err(|e| DataAvailabilityError::MintlayerError(e.to_string()))?;

        tracing::info!(
            "add root digest to mintlayer with L1 tx_info: {}",
            serde_json::to_string(&response_json).unwrap()
        );
        
        match response_json.get("result").and_then(|v| v.as_str()) {
            Some(tx_hash) if !tx_hash.is_empty() => Ok(tx_hash.to_string()),
            Some(_) => Err(DataAvailabilityError::MintlayerError(
                "Empty tx_hash in response".into(),
            )),
            None => Err(DataAvailabilityError::MintlayerError(
                "No tx_hash in response".into(),
            )),
        }
    }
}

#[async_trait]
impl IPFSService for Box<dyn IPFSService + 'static> {
    async fn upload(&self, data: &[u8]) -> Result<String, DataAvailabilityError> {
        (**self).upload(data).await
    }
}

#[async_trait]
impl MintlayerService for Box<dyn MintlayerService + 'static> {
    async fn initialize_wallet(&self) -> Result<(), DataAvailabilityError> {
        (**self).initialize_wallet().await
    }
    
    async fn submit_hashes(&self, ipfs_hashes: &[String]) -> Result<String, DataAvailabilityError> {
        (**self).submit_hashes(ipfs_hashes).await
    }
} 