#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    pub circuit_failure_threshold: u32,
    pub circuit_cooldown_base_ms: u64,
    pub circuit_cooldown_factor: f64,
    pub circuit_cooldown_max_ms: u64,
    pub max_attempts: u32,
}

impl DispatcherConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(value) = std::env::var("RECEIVER_CIRCUIT_FAILURE_THRESHOLD")
            && let Ok(parsed) = value.parse::<u32>()
        {
            config.circuit_failure_threshold = parsed.max(1);
        }
        if let Ok(value) = std::env::var("RECEIVER_CIRCUIT_COOLDOWN_BASE_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            config.circuit_cooldown_base_ms = parsed;
        }
        if let Ok(value) = std::env::var("RECEIVER_CIRCUIT_COOLDOWN_FACTOR")
            && let Ok(parsed) = value.parse::<f64>()
        {
            config.circuit_cooldown_factor = parsed;
        }
        if let Ok(value) = std::env::var("RECEIVER_CIRCUIT_COOLDOWN_MAX_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            config.circuit_cooldown_max_ms = parsed;
        }
        if let Ok(value) = std::env::var("RECEIVER_MAX_ATTEMPTS")
            && let Ok(parsed) = value.parse::<u32>()
        {
            config.max_attempts = parsed.max(1);
        }

        config
    }
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            circuit_failure_threshold: 3,
            circuit_cooldown_base_ms: 30_000,
            circuit_cooldown_factor: 2.0,
            circuit_cooldown_max_ms: 600_000,
            max_attempts: 5,
        }
    }
}
