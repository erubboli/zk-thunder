use std::fmt;

#[derive(Debug)]
pub enum DataAvailabilityError {
    MaxRetriesExceededError(String),
    IPFSError(String),
    MintlayerError(String),
    DatabaseError(String),
    CircuitBreakerOpenError(String),
}

impl fmt::Display for DataAvailabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxRetriesExceededError(op) => {
                write!(f, "Max retries exceeded for operation: {}", op)
            }
            Self::IPFSError(msg) => write!(f, "IPFS error: {}", msg),
            Self::MintlayerError(msg) => write!(f, "Mintlayer error: {}", msg),
            Self::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            Self::CircuitBreakerOpenError(service) => {
                write!(f, "Circuit breaker is open for service: {}", service)
            }
        }
    }
}

impl std::error::Error for DataAvailabilityError {}
