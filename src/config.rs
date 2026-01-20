use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub port: u16,
    pub max_wal_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 4210,
            max_wal_size:  100 * 1024, // 100kb,
        }
    }
}

pub fn config_get() -> &'static Config {
    CONFIG.get_or_init(|| -> Config {
        let path = "Config.toml";

        if !std::path::Path::new(path).exists() {
            let default = Config::default();
            let toml_str = toml::to_string(&default).unwrap();
            fs::write(path, toml_str).expect("Failed to create default config");
        }

        let toml_string = fs::read_to_string(path).expect("Failed to read config file");

        toml::from_str(&toml_string).expect("Failed to parse config")
    })
}
