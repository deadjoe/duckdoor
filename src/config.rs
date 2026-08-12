use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use base64::Engine;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:9494";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Backend {
    pub name: String,
    pub path: PathBuf,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

fn enabled_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "config_version")]
    pub version: u32,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_threads_per_worker")]
    pub threads_per_worker: usize,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default = "default_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default)]
    pub backends: Vec<Backend>,
}

fn config_version() -> u32 {
    1
}
fn default_listen() -> String {
    DEFAULT_LISTEN.to_owned()
}
fn default_max_rows() -> usize {
    10_000
}
fn default_timeout_seconds() -> u64 {
    300
}
fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(4, usize::from)
        .clamp(1, 8)
}
fn default_threads_per_worker() -> usize {
    1
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: config_version(),
            listen: default_listen(),
            workers: default_workers(),
            threads_per_worker: default_threads_per_worker(),
            max_rows: default_max_rows(),
            request_timeout_seconds: default_timeout_seconds(),
            backends: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Paths {
    pub home: PathBuf,
    pub config: PathBuf,
    pub config_lock: PathBuf,
    pub pid: PathBuf,
    pub log: PathBuf,
    pub admin_token: PathBuf,
    pub init_sql: PathBuf,
}

impl Paths {
    pub fn resolve(override_home: Option<PathBuf>) -> Result<Self> {
        let home = match override_home {
            Some(path) => path,
            None => ProjectDirs::from("io", "duckdoor", "duckdoor")
                .context("could not determine the user configuration directory")?
                .config_dir()
                .to_path_buf(),
        };
        Ok(Self {
            config: home.join("config.toml"),
            config_lock: home.join("config.lock"),
            pid: home.join("duckdoor.pid"),
            log: home.join("duckdoor.log"),
            admin_token: home.join("admin.token"),
            init_sql: home.join("init.sql"),
            home,
        })
    }

    pub fn ensure(&self) -> Result<()> {
        fs::create_dir_all(&self.home)
            .with_context(|| format!("could not create {}", self.home.display()))?;
        if !self.config.exists() {
            save_config(self, &Config::default())?;
        }
        if !self.init_sql.exists() {
            atomic_write(
                &self.init_sql,
                b"-- Optional read-only views and macros loaded into every worker.\n",
            )?;
        }
        if !self.admin_token.exists() {
            let mut bytes = [0_u8; 32];
            fill_random(&mut bytes)?;
            let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
            atomic_write(&self.admin_token, token.as_bytes())?;
            restrict_permissions(&self.admin_token)?;
        }
        Ok(())
    }
}

pub fn load_config(paths: &Paths) -> Result<Config> {
    let raw = fs::read_to_string(&paths.config)
        .with_context(|| format!("could not read {}", paths.config.display()))?;
    let mut config: Config =
        toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", paths.config.display()))?;
    validate_config(&mut config)?;
    Ok(config)
}

pub fn save_config(paths: &Paths, config: &Config) -> Result<()> {
    let raw = toml::to_string_pretty(config).context("could not serialize configuration")?;
    atomic_write(&paths.config, raw.as_bytes())
}

pub fn validate_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name.bytes().enumerate().all(|(i, byte)| {
            byte == b'_' || byte.is_ascii_alphanumeric() && (i > 0 || !byte.is_ascii_digit())
        });
    if !valid {
        bail!("backend name must match [A-Za-z_][A-Za-z0-9_]{{0,62}}")
    }
    Ok(())
}

fn validate_config(config: &mut Config) -> Result<()> {
    if config.version != 1 {
        bail!("unsupported configuration version {}", config.version);
    }
    if config.workers == 0 || config.workers > 64 {
        bail!("workers must be between 1 and 64");
    }
    if config.threads_per_worker == 0 || config.threads_per_worker > 64 {
        bail!("threads_per_worker must be between 1 and 64");
    }
    if config.max_rows == 0 {
        bail!("max_rows must be greater than zero");
    }
    if config.request_timeout_seconds == 0 {
        bail!("request_timeout_seconds must be greater than zero");
    }
    config.backends.sort_by(|a, b| a.name.cmp(&b.name));
    for (index, backend) in config.backends.iter().enumerate() {
        validate_name(&backend.name)?;
        if config.backends[..index]
            .iter()
            .any(|item| item.name == backend.name)
        {
            bail!("duplicate backend name: {}", backend.name);
        }
        if !backend.path.is_absolute() {
            bail!("backend path must be absolute: {}", backend.path.display());
        }
    }
    Ok(())
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().context("path has no parent directory")?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn fill_random(bytes: &mut [u8]) -> Result<()> {
    let mut file = OpenOptions::new().read(true).open("/dev/urandom")?;
    file.read_exact(bytes)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_names_are_sql_identifier_safe() {
        for name in ["app_a", "Logs2", "_metrics"] {
            assert!(validate_name(name).is_ok());
        }
        for name in ["", "2bad", "bad-name", "a b", "x.y"] {
            assert!(validate_name(name).is_err());
        }
    }

    #[test]
    fn config_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::resolve(Some(dir.path().to_path_buf())).unwrap();
        paths.ensure().unwrap();
        let config = load_config(&paths).unwrap();
        assert_eq!(config.version, 1);
        assert_eq!(config.listen, DEFAULT_LISTEN);
        assert!(paths.admin_token.exists());
    }
}
