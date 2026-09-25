use ring::rand::{SecureRandom, SystemRandom};

use crate::DataViewContractError;

pub trait IdGenerator: Send + Sync {
    fn next_id(&self, prefix: &str) -> Result<String, DataViewContractError>;
}

#[derive(Debug, Default)]
pub struct SystemIdGenerator;

impl IdGenerator for SystemIdGenerator {
    fn next_id(&self, prefix: &str) -> Result<String, DataViewContractError> {
        let mut random = [0_u8; 24];
        SystemRandom::new()
            .fill(&mut random)
            .map_err(|_| DataViewContractError::IdGenerationFailed)?;
        let mut encoded = String::with_capacity(prefix.len() + 1 + random.len() * 2);
        encoded.push_str(prefix);
        encoded.push(':');
        for byte in random {
            use std::fmt::Write;
            write!(&mut encoded, "{byte:02x}")
                .map_err(|_| DataViewContractError::IdGenerationFailed)?;
        }
        Ok(encoded)
    }
}
