use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;

use crate::config::{Paths, load_config};

pub struct PidGuard {
    file: File,
    path: std::path::PathBuf,
}

impl PidGuard {
    pub fn acquire(paths: &Paths) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&paths.pid)?;
        file.try_lock_exclusive()
            .context("another duckdoor daemon is already running")?;
        file.set_len(0)?;
        writeln!(file, "{}", std::process::id())?;
        file.sync_all()?;
        Ok(Self {
            file,
            path: paths.pid.clone(),
        })
    }
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        let _ = fs::remove_file(&self.path);
    }
}

pub fn start(paths: &Paths) -> Result<()> {
    paths.ensure()?;
    if let Some(pid) = running_pid(paths)? {
        bail!("duckdoor is already running (pid {pid})");
    }
    let log = OpenOptions::new().create(true).append(true).open(&paths.log)?;
    let stderr = log.try_clone()?;
    let executable = std::env::current_exe().context("could not locate the duckdoor executable")?;
    let mut command = Command::new(executable);
    command
        .arg("--home")
        .arg(&paths.home)
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().context("could not launch daemon")?;
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            bail!(
                "daemon exited during startup with {status}; inspect {}",
                paths.log.display()
            );
        }
        if health(paths).is_ok() {
            println!("duckdoor started (pid {})", child.id());
            return Ok(());
        }
        thread::sleep(Duration::from_millis(150));
    }
    bail!(
        "daemon did not become healthy within 45 seconds; inspect {}",
        paths.log.display()
    )
}

pub fn stop(paths: &Paths) -> Result<()> {
    let Some(pid) = running_pid(paths)? else {
        bail!("duckdoor is not running");
    };
    let status = Command::new("kill").args(["-TERM", &pid.to_string()]).status()?;
    if !status.success() {
        bail!("could not signal daemon pid {pid}");
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if !process_alive(pid) {
            println!("duckdoor stopped");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!("daemon pid {pid} did not stop within 15 seconds")
}

pub fn restart(paths: &Paths) -> Result<()> {
    if running_pid(paths)?.is_some() {
        stop(paths)?;
    }
    start(paths)
}

pub fn status(paths: &Paths) -> Result<()> {
    match running_pid(paths)? {
        Some(pid) => match health(paths) {
            Ok(body) => println!("running (pid {pid})\n{}", serde_json::to_string_pretty(&body)?),
            Err(error) => println!("starting or unhealthy (pid {pid}): {error:#}"),
        },
        None if paths.pid.exists() => println!("stopped (stale pid file: {})", paths.pid.display()),
        None => println!("stopped"),
    }
    Ok(())
}

pub fn logs(paths: &Paths, lines: usize, follow: bool) -> Result<()> {
    if follow {
        let status = Command::new("tail")
            .arg("-n")
            .arg(lines.to_string())
            .arg("-f")
            .arg(&paths.log)
            .status()
            .context("could not run tail")?;
        if !status.success() {
            bail!("tail exited with {status}");
        }
        return Ok(());
    }
    let mut file =
        File::open(&paths.log).with_context(|| format!("could not open {}", paths.log.display()))?;
    let length = file.metadata()?.len();
    let read_from = length.saturating_sub(1024 * 1024);
    file.seek(SeekFrom::Start(read_from))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let selected = text.lines().rev().take(lines).collect::<Vec<_>>();
    for line in selected.into_iter().rev() {
        println!("{line}");
    }
    Ok(())
}

pub fn reload(paths: &Paths) -> Result<serde_json::Value> {
    let config = load_config(paths)?;
    let token = fs::read_to_string(&paths.admin_token)?;
    let url = format!("http://{}/v1/admin/reload", config.listen);
    let response = reqwest::blocking::Client::new()
        .post(url)
        .header("x-duckdoor-admin-token", token.trim())
        .send()
        .context("daemon is not reachable")?;
    let status = response.status();
    let body: serde_json::Value = response.json().context("daemon returned invalid JSON")?;
    if !status.is_success() {
        bail!("reload failed ({status}): {body}");
    }
    Ok(body)
}

pub fn health(paths: &Paths) -> Result<serde_json::Value> {
    let config = load_config(paths)?;
    let url = format!("http://{}/healthz", config.listen);
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()?
        .get(url)
        .send()?;
    if !response.status().is_success() {
        bail!("health endpoint returned {}", response.status());
    }
    Ok(response.json()?)
}

pub fn running_pid(paths: &Paths) -> Result<Option<u32>> {
    if !paths.pid.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&paths.pid)?;
    let pid = raw.trim().parse::<u32>().context("pid file is invalid")?;
    Ok(process_alive(pid).then_some(pid))
}

fn process_alive(pid: u32) -> bool {
    if pid < 2 {
        return false;
    }
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn ensure_sqlite_path(path: &Path) -> Result<std::path::PathBuf> {
    let canonical =
        fs::canonicalize(path).with_context(|| format!("could not resolve {}", path.display()))?;
    if !canonical.is_file() {
        bail!("not a regular file: {}", canonical.display());
    }
    Ok(canonical)
}
