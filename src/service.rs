//! Background service management (systemd on Linux, launchd on macOS).
#![cfg_attr(
    not(any(target_os = "linux", target_os = "macos")),
    allow(dead_code, unused_imports)
)]

use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Subcommand;
use quilr_remote_connector::tunnel::TunnelConfig;

use crate::{DEFAULT_HEARTBEAT_LOG_SECS, DEFAULT_PING_INTERVAL_SECS, RunArgs};

const NAME: &str = "quilr-connector";

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceCommand {
    /// Install the service and (re)start it
    ///
    /// Run as root (sudo) for a system service that starts at boot; otherwise a per-user service
    /// is installed. Re-run with a newer binary to upgrade. Without --token the existing
    /// configuration is kept. Logs are always written as JSON.
    Install(RunArgs),
    /// Stop the service and remove it
    Uninstall,
    /// Start the service
    Start,
    /// Stop the service
    Stop,
    /// Restart the service
    Restart,
    /// Show service status
    Status,
    /// Show service logs
    Logs {
        /// Keep printing new log lines
        #[arg(short, long)]
        follow: bool,
        /// Number of recent lines to show
        #[arg(short = 'n', long, default_value_t = 100)]
        lines: usize,
    },
}

pub(crate) fn run(cmd: ServiceCommand) -> Result<()> {
    platform::run(cmd)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

type EnvVars = Vec<(String, String)>;

/// Environment variables for the service: the token plus every option that differs from its default.
fn env_pairs(token: &str, args: &RunArgs) -> EnvVars {
    let mut env = vec![("QUILR_CONNECTOR_TOKEN", token.to_string())];
    if let Some(url) = &args.gateway_url {
        env.push(("QUILR_GATEWAY_URL", url.clone()));
    }
    if let Some(name) = &args.instance_name {
        env.push(("QUILR_CONNECTOR_INSTANCE_NAME", name.clone()));
    }
    if args.insecure_skip_tls_verify {
        env.push(("QUILR_CONNECTOR_INSECURE_TLS", "true".into()));
    }
    if args.ping_interval_secs != DEFAULT_PING_INTERVAL_SECS {
        env.push((
            "QUILR_CONNECTOR_PING_INTERVAL_SECS",
            args.ping_interval_secs.to_string(),
        ));
    }
    if args.log.log_level != "info" {
        env.push(("QUILR_LOG_LEVEL", args.log.log_level.clone()));
    }
    if let Some(file) = &args.log.log_file {
        env.push(("QUILR_LOG_FILE", file.display().to_string()));
    }
    if args.log.heartbeat_log_secs != DEFAULT_HEARTBEAT_LOG_SECS {
        env.push((
            "QUILR_HEARTBEAT_LOG_SECS",
            args.log.heartbeat_log_secs.to_string(),
        ));
    }
    env.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Validates the token/gateway URL and returns the service environment, or `None` when no token
/// was given and an existing configuration should be kept.
fn new_env(args: &RunArgs, have_existing: bool) -> Result<Option<EnvVars>> {
    match args
        .token
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        Some(token) => {
            let url = TunnelConfig {
                gateway_ws_url: args.gateway_url.clone(),
                token: token.to_string(),
                instance_name: None,
                insecure_skip_tls_verify: false,
                ping_interval: Duration::ZERO,
            }
            .normalized_ws_url()
            .context("invalid connector token or gateway URL")?;
            println!("✓ token ok (gateway {url})");
            Ok(Some(env_pairs(token, args)))
        }
        None if have_existing => Ok(None),
        None => bail!("missing connector token: pass --token or set QUILR_CONNECTOR_TOKEN"),
    }
}

/// Makes `--log-file` absolute, since the service runs with a different working directory.
fn absolutize_log_file(args: &mut RunArgs) -> Result<()> {
    if let Some(file) = &args.log.log_file {
        args.log.log_file = Some(std::path::absolute(file)?);
    }
    Ok(())
}

/// Atomically writes `contents` to `path` with `mode`, via a temp file in the same directory.
fn write_file(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    let tmp = dir.join(tmp_name);
    let result = (|| -> std::io::Result<()> {
        let _ = fs::remove_file(&tmp);
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, mode);
        let mut file = opts.open(&tmp)?;
        file.write_all(contents)?;
        file.sync_all()?;
        set_mode(&tmp, mode)?;
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.with_context(|| format!("failed to write {}", path.display()))
}

fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Ok(())
    }
}

/// Runs a command with inherited stdio and returns its exit status.
fn status_of(cmd: &mut Command) -> Result<ExitStatus> {
    cmd.status()
        .with_context(|| format!("failed to run `{}`", describe(cmd)))
}

/// Runs a command with inherited stdio, failing on a non-zero exit.
fn exec(cmd: &mut Command) -> Result<()> {
    let status = status_of(cmd)?;
    if !status.success() {
        bail!("`{}` failed ({status})", describe(cmd));
    }
    Ok(())
}

/// Runs a command silently and reports whether it succeeded.
fn quiet(cmd: &mut Command) -> bool {
    cmd.stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn describe(cmd: &Command) -> String {
    std::iter::once(cmd.get_program())
        .chain(cmd.get_args())
        .map(OsStr::to_string_lossy)
        .collect::<Vec<_>>()
        .join(" ")
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
        .context("HOME is not set")
}

#[cfg(unix)]
fn is_root() -> bool {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() == 0 }
}

/// Picks system scope for root, otherwise the user scope unless only a system install exists.
#[cfg(unix)]
fn use_system_scope(user_file: &Path, system_file: &Path) -> bool {
    is_root() || (!user_file.exists() && system_file.exists())
}

/// Tails `--log-file`, which the connector keeps as a symlink to the current day's file.
fn tail_log(log_file: &Path, lines: usize, follow: bool) -> Result<()> {
    let mut cmd = Command::new("tail");
    cmd.arg("-n").arg(lines.to_string());
    if follow {
        cmd.arg("-F");
    }
    exec(cmd.arg(log_file))
}

/// Copies the running binary to a permanent `target` (unless it already is that file), so the
/// service keeps working after the downloaded copy is deleted.
fn install_binary(target: &Path) -> Result<PathBuf> {
    let exe = std::env::current_exe()?.canonicalize()?;
    if target.canonicalize().is_ok_and(|t| t == exe) {
        return Ok(target.to_path_buf());
    }
    let bytes = fs::read(&exe).with_context(|| format!("failed to read {}", exe.display()))?;
    write_file(target, &bytes, 0o755)?;
    println!("✓ installed {}", target.display());
    Ok(target.to_path_buf())
}

/// The binary location for a service: /usr/local/bin for root, ~/.local/bin otherwise.
fn binary_path(system: bool) -> Result<PathBuf> {
    Ok(if system {
        PathBuf::from("/usr/local/bin").join(NAME)
    } else {
        home()?.join(".local/bin").join(NAME)
    })
}

fn print_next_steps(system: bool) {
    let sudo = if system { "sudo " } else { "" };
    println!("\nNext steps:");
    println!("  {sudo}{NAME} service status");
    println!("  {sudo}{NAME} service logs -f");
}

// ---------------------------------------------------------------------------
// systemd file rendering
// ---------------------------------------------------------------------------

/// Renders a systemd `EnvironmentFile` with every value double-quoted.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn render_env_file(env: &[(String, String)]) -> String {
    let mut out = format!("# Managed by `{NAME} service install`; re-run it to change settings.\n");
    for (key, value) in env {
        let value = value.replace('\\', "\\\\").replace('"', "\\\"");
        out.push_str(&format!("{key}=\"{value}\"\n"));
    }
    out
}

/// Reads one value back from an env file written by [`render_env_file`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn env_file_value(contents: &str, key: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let value = line.trim().strip_prefix(key)?.strip_prefix('=')?;
        let unquoted = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value);
        let mut out = String::new();
        let mut chars = unquoted.chars();
        while let Some(c) = chars.next() {
            out.push(if c == '\\' {
                chars.next().unwrap_or(c)
            } else {
                c
            });
        }
        Some(out)
    })
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn render_unit(exe: &Path, env_file: &Path, system: bool) -> String {
    let exe = exe.display().to_string();
    let exec_start = if exe.contains(char::is_whitespace) {
        format!("\"{exe}\"")
    } else {
        exe
    };
    let sandbox = if system {
        "DynamicUser=yes\nNoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\n\
         PrivateTmp=yes\nLogsDirectory=quilr-connector\n"
    } else {
        ""
    };
    let target = if system {
        "multi-user.target"
    } else {
        "default.target"
    };
    format!(
        "[Unit]
Description=Quilr Remote Connector
Documentation=https://github.com/quilrai/quilr-remote-connector
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
EnvironmentFile={env_file}
Environment=QUILR_LOG_FORMAT=json
ExecStart={exec_start}
Restart=always
RestartSec=5
{sandbox}
[Install]
WantedBy={target}
",
        env_file = env_file.display(),
    )
}

// ---------------------------------------------------------------------------
// launchd plist rendering
// ---------------------------------------------------------------------------

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const LAUNCHD_LABEL: &str = "ai.quilr.connector";

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn render_plist(exe: &Path, env: &[(String, String)], stderr_path: &Path) -> String {
    let env: String = env
        .iter()
        .map(|(k, v)| {
            format!(
                "\t\t<key>{}</key>\n\t\t<string>{}</string>\n",
                xml_escape(k),
                xml_escape(v)
            )
        })
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Managed by `{NAME} service install`; re-run it to change settings. -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{LAUNCHD_LABEL}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{exe}</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
{env}	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>StandardErrorPath</key>
	<string>{stderr}</string>
</dict>
</plist>
"#,
        exe = xml_escape(&exe.display().to_string()),
        stderr = xml_escape(&stderr_path.display().to_string()),
    )
}

/// Reads the `EnvironmentVariables` back from a plist written by [`render_plist`].
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn plist_env(plist: &str) -> EnvVars {
    let Some(start) = plist.find("<key>EnvironmentVariables</key>") else {
        return Vec::new();
    };
    let section = &plist[start..];
    let section = section
        .find("</dict>")
        .map_or(section, |end| &section[..end]);
    let mut out = Vec::new();
    let mut rest = section;
    while let Some(i) = rest.find("<key>") {
        let Some((key, after)) = rest[i + 5..].split_once("</key>") else {
            break;
        };
        rest = after;
        let Some((value, after)) = after
            .trim_start()
            .strip_prefix("<string>")
            .and_then(|v| v.split_once("</string>"))
        else {
            continue;
        };
        out.push((xml_unescape(key), xml_unescape(value)));
        rest = after;
    }
    out
}

// ---------------------------------------------------------------------------
// Linux (systemd)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    const UNIT: &str = "quilr-connector";
    const SYSTEM_LOG_DIR: &str = "/var/log/quilr-connector";

    struct Systemd {
        system: bool,
        unit_file: PathBuf,
        env_file: PathBuf,
    }

    impl Systemd {
        /// The service for `system` scope, after checking that systemd is available.
        fn new(system: bool) -> Result<Self> {
            let systemd_running = Path::new("/run/systemd/system").exists()
                && quiet(Command::new("systemctl").arg("--version"));
            if !systemd_running {
                bail!(
                    "systemd was not found on this machine.\n\
                     Run `{NAME}` in the foreground under your process supervisor of choice \
                     (configure it with the QUILR_* environment variables), or use the Docker image."
                );
            }
            Self::paths(system)
        }

        fn paths(system: bool) -> Result<Self> {
            if system {
                return Ok(Self {
                    system,
                    unit_file: PathBuf::from("/etc/systemd/system/quilr-connector.service"),
                    env_file: PathBuf::from("/etc/quilr-connector/connector.env"),
                });
            }
            let config = std::env::var_os("XDG_CONFIG_HOME")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .map_or_else(|| home().map(|h| h.join(".config")), Ok)?;
            Ok(Self {
                system,
                unit_file: config.join("systemd/user/quilr-connector.service"),
                env_file: config.join("quilr-connector/connector.env"),
            })
        }

        /// The installed service: system scope for root or when only a system unit exists.
        fn detect() -> Result<Self> {
            let user = Self::new(false)?;
            let system = Self::paths(true)?;
            Ok(if use_system_scope(&user.unit_file, &system.unit_file) {
                system
            } else {
                user
            })
        }

        fn systemctl(&self) -> Command {
            let mut cmd = Command::new("systemctl");
            if !self.system {
                cmd.arg("--user");
            }
            cmd
        }

        fn require_root(&self) -> Result<()> {
            if self.system && !is_root() {
                bail!("the {NAME} system service is managed by root; re-run with sudo");
            }
            Ok(())
        }
    }

    pub(super) fn run(cmd: ServiceCommand) -> Result<()> {
        match cmd {
            ServiceCommand::Install(args) => install(args),
            ServiceCommand::Uninstall => uninstall(),
            ServiceCommand::Start => passthrough("start"),
            ServiceCommand::Stop => passthrough("stop"),
            ServiceCommand::Restart => passthrough("restart"),
            ServiceCommand::Status => status(),
            ServiceCommand::Logs { follow, lines } => logs(follow, lines),
        }
    }

    fn install(mut args: RunArgs) -> Result<()> {
        let sd = Systemd::new(is_root())?;
        absolutize_log_file(&mut args)?;
        if sd.system
            && let Some(file) = &args.log.log_file
            && !(file.starts_with(SYSTEM_LOG_DIR) && file != Path::new(SYSTEM_LOG_DIR))
        {
            bail!(
                "--log-file must be under {SYSTEM_LOG_DIR}/ for the system service (it runs \
                 sandboxed and can only write there), e.g. {SYSTEM_LOG_DIR}/connector.log\n\
                 Or omit --log-file to log to the journal."
            );
        }

        match new_env(&args, sd.env_file.exists())? {
            Some(env) => {
                write_file(&sd.env_file, render_env_file(&env).as_bytes(), 0o600)?;
                println!("✓ wrote {}", sd.env_file.display());
            }
            None => {
                set_mode(&sd.env_file, 0o600)?;
                println!(
                    "✓ kept existing {} (pass --token to replace it)",
                    sd.env_file.display()
                );
            }
        }

        let exe = install_binary(&binary_path(sd.system)?)?;
        let unit = render_unit(&exe, &sd.env_file, sd.system);
        write_file(&sd.unit_file, unit.as_bytes(), 0o644)?;
        println!("✓ wrote {}", sd.unit_file.display());

        exec(sd.systemctl().arg("daemon-reload"))?;
        exec(sd.systemctl().args(["enable", "--quiet", UNIT]))?;
        exec(sd.systemctl().args(["restart", UNIT]))?;
        println!("✓ enabled and started {UNIT}");

        if !sd.system {
            println!(
                "\nThis is a per-user service. To keep it running at boot without logging in, run:\n  \
                 sudo loginctl enable-linger $USER\n\
                 (or re-run install with sudo for a system-wide service)"
            );
        }
        print_next_steps(sd.system);
        Ok(())
    }

    fn uninstall() -> Result<()> {
        let sd = Systemd::detect()?;
        sd.require_root()?;
        if !sd.unit_file.exists() && !sd.env_file.exists() {
            println!("{UNIT} is not installed");
            return Ok(());
        }
        if sd.unit_file.exists() {
            exec(sd.systemctl().args(["disable", "--now", "--quiet", UNIT]))
                .context("could not stop the service; nothing was removed")?;
            println!("✓ stopped and disabled {UNIT}");
        }
        for file in [&sd.unit_file, &sd.env_file] {
            match fs::remove_file(file) {
                Ok(()) => println!("✓ removed {}", file.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).context(format!("failed to remove {}", file.display())),
            }
        }
        if let Some(dir) = sd.env_file.parent() {
            let _ = fs::remove_dir(dir); // only succeeds when empty
        }
        exec(sd.systemctl().arg("daemon-reload"))?;
        let bin = binary_path(sd.system)?;
        if bin.exists() {
            println!(
                "{} was left in place; delete it if no longer needed",
                bin.display()
            );
        }
        Ok(())
    }

    fn passthrough(verb: &str) -> Result<()> {
        exec(Systemd::detect()?.systemctl().args([verb, UNIT]))
    }

    fn status() -> Result<()> {
        let status =
            status_of(
                Systemd::detect()?
                    .systemctl()
                    .args(["status", "--no-pager", UNIT]),
            )?;
        match status.code() {
            Some(0) => Ok(()),
            Some(3) => bail!("{UNIT} is not running"),
            Some(4) => bail!("{UNIT} is not installed (run `{NAME} service install`)"),
            _ => bail!("systemctl status failed ({status})"),
        }
    }

    fn logs(follow: bool, lines: usize) -> Result<()> {
        let sd = Systemd::detect()?;
        let log_file = match fs::read_to_string(&sd.env_file) {
            Ok(contents) => env_file_value(&contents, "QUILR_LOG_FILE"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(e).context(format!(
                    "cannot read {}; re-run with sudo",
                    sd.env_file.display()
                ));
            }
        };
        if let Some(file) = log_file {
            return tail_log(Path::new(&file), lines, follow);
        }
        let mut cmd = Command::new("journalctl");
        cmd.arg(if sd.system { "--unit" } else { "--user-unit" })
            .arg(UNIT)
            .args(["--output", "cat", "--no-pager", "--lines"])
            .arg(lines.to_string());
        if follow {
            cmd.arg("--follow");
        }
        exec(&mut cmd)
    }
}

// ---------------------------------------------------------------------------
// macOS (launchd)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    struct Launchd {
        plist: PathBuf,
        domain: String,
        log_dir: PathBuf,
    }

    impl Launchd {
        fn new(system: bool) -> Result<Self> {
            let plist_name = format!("{LAUNCHD_LABEL}.plist");
            if system {
                return Ok(Self {
                    plist: Path::new("/Library/LaunchDaemons").join(plist_name),
                    domain: "system".into(),
                    log_dir: PathBuf::from("/Library/Logs/quilr-connector"),
                });
            }
            let home = home()?;
            // SAFETY: getuid has no preconditions and cannot fail.
            let uid = unsafe { libc::getuid() };
            Ok(Self {
                plist: home.join("Library/LaunchAgents").join(plist_name),
                domain: format!("gui/{uid}"),
                log_dir: home.join("Library/Logs/quilr-connector"),
            })
        }

        fn detect() -> Result<Self> {
            let user = Self::new(false)?;
            let system = Self::new(true)?;
            Ok(if use_system_scope(&user.plist, &system.plist) {
                system
            } else {
                user
            })
        }

        fn target(&self) -> String {
            format!("{}/{LAUNCHD_LABEL}", self.domain)
        }

        fn launchctl(&self, verb: &str) -> Command {
            let mut cmd = Command::new("launchctl");
            cmd.arg(verb);
            match verb {
                "bootstrap" => cmd.arg(&self.domain).arg(&self.plist),
                _ => cmd.arg(self.target()),
            };
            cmd
        }

        fn is_loaded(&self) -> bool {
            quiet(&mut self.launchctl("print"))
        }

        fn env(&self) -> EnvVars {
            fs::read_to_string(&self.plist)
                .map(|p| plist_env(&p))
                .unwrap_or_default()
        }

        fn log_file(&self) -> PathBuf {
            self.env()
                .into_iter()
                .find(|(k, _)| k == "QUILR_LOG_FILE")
                .map_or_else(|| self.log_dir.join("connector.log"), |(_, v)| v.into())
        }
    }

    pub(super) fn run(cmd: ServiceCommand) -> Result<()> {
        match cmd {
            ServiceCommand::Install(args) => install(args),
            ServiceCommand::Uninstall => uninstall(),
            ServiceCommand::Start => {
                let ld = Launchd::detect()?;
                let verb = if ld.is_loaded() {
                    "kickstart"
                } else {
                    "bootstrap"
                };
                exec(&mut ld.launchctl(verb))
            }
            ServiceCommand::Stop => exec(&mut Launchd::detect()?.launchctl("bootout")),
            ServiceCommand::Restart => exec(Launchd::detect()?.launchctl("kickstart").arg("-k")),
            ServiceCommand::Status => status(),
            ServiceCommand::Logs { follow, lines } => {
                tail_log(&Launchd::detect()?.log_file(), lines, follow)
            }
        }
    }

    fn install(mut args: RunArgs) -> Result<()> {
        let ld = Launchd::new(is_root())?;
        absolutize_log_file(&mut args)?;
        let env = match new_env(&args, ld.plist.exists())? {
            Some(mut env) => {
                if args.log.log_file.is_none() {
                    let file = ld.log_dir.join("connector.log");
                    env.push(("QUILR_LOG_FILE".into(), file.display().to_string()));
                }
                env.push(("QUILR_LOG_FORMAT".into(), "json".into()));
                env
            }
            None => {
                let env = ld.env();
                if !env.iter().any(|(k, _)| k == "QUILR_CONNECTOR_TOKEN") {
                    bail!("missing connector token: pass --token or set QUILR_CONNECTOR_TOKEN");
                }
                println!("✓ kept existing settings (pass --token to replace them)");
                env
            }
        };
        fs::create_dir_all(&ld.log_dir)
            .with_context(|| format!("failed to create {}", ld.log_dir.display()))?;

        let exe = install_binary(&binary_path(ld.domain == "system")?)?;
        let plist = render_plist(&exe, &env, &ld.log_dir.join("stderr.log"));
        write_file(&ld.plist, plist.as_bytes(), 0o600)?;
        println!("✓ wrote {}", ld.plist.display());

        quiet(&mut ld.launchctl("bootout"));
        exec(&mut ld.launchctl("bootstrap"))?;
        println!("✓ loaded and started {LAUNCHD_LABEL}");
        if !is_root() {
            println!(
                "\nThis is a per-user agent that runs while you are logged in \
                 (re-run install with sudo for a system-wide daemon)."
            );
        }
        print_next_steps(ld.domain == "system");
        Ok(())
    }

    fn uninstall() -> Result<()> {
        let ld = Launchd::detect()?;
        if !ld.plist.exists() {
            println!("{LAUNCHD_LABEL} is not installed");
            return Ok(());
        }
        if ld.domain == "system" && !is_root() {
            bail!("the {NAME} system daemon is managed by root; re-run with sudo");
        }
        quiet(&mut ld.launchctl("bootout"));
        println!("✓ stopped {LAUNCHD_LABEL}");
        fs::remove_file(&ld.plist)
            .with_context(|| format!("failed to remove {}", ld.plist.display()))?;
        println!("✓ removed {}", ld.plist.display());
        println!("Logs were left in {}", ld.log_dir.display());
        Ok(())
    }

    fn status() -> Result<()> {
        let ld = Launchd::detect()?;
        let output = ld
            .launchctl("print")
            .stderr(Stdio::null())
            .output()
            .context("failed to run launchctl")?;
        if !output.status.success() {
            let what = if ld.plist.exists() {
                "is not loaded"
            } else {
                "is not installed"
            };
            bail!("{LAUNCHD_LABEL} {what}");
        }
        println!("{} ({})", LAUNCHD_LABEL, ld.plist.display());
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let line = line.trim();
            if ["state =", "pid =", "last exit code =", "runs ="]
                .iter()
                .any(|p| line.starts_with(p))
            {
                println!("  {line}");
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Other platforms
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::*;

    pub(super) fn run(_cmd: ServiceCommand) -> Result<()> {
        bail!(
            "service management is supported on Linux (systemd) and macOS (launchd); on Windows \
             run the connector under a service wrapper such as NSSM or as a scheduled task"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        cmd: ServiceCommand,
    }

    fn install_args(argv: &[&str]) -> RunArgs {
        let argv = ["test", "install"].iter().chain(argv);
        match TestCli::parse_from(argv).cmd {
            ServiceCommand::Install(args) => args,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn env_pairs_only_include_non_defaults() {
        let args = install_args(&["--log-level", "debug", "--insecure-skip-tls-verify"]);
        let keys: Vec<_> = env_pairs("qrc_x.y", &args)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        // Env vars from the test environment could leak in, so only assert on what we set.
        assert!(keys.contains(&"QUILR_CONNECTOR_TOKEN".to_string()));
        assert!(keys.contains(&"QUILR_LOG_LEVEL".to_string()));
        assert!(keys.contains(&"QUILR_CONNECTOR_INSECURE_TLS".to_string()));
        assert!(!keys.contains(&"QUILR_LOG_FORMAT".to_string()));
    }

    #[test]
    fn env_file_quotes_and_round_trips() {
        let env = vec![
            (
                "QUILR_CONNECTOR_TOKEN".to_string(),
                "qrc_abc.def".to_string(),
            ),
            (
                "QUILR_LOG_FILE".to_string(),
                r#"/var/log/a "b"\c.log"#.to_string(),
            ),
        ];
        let rendered = render_env_file(&env);
        assert!(rendered.starts_with("# Managed by"));
        assert!(rendered.contains("QUILR_CONNECTOR_TOKEN=\"qrc_abc.def\"\n"));
        assert!(rendered.contains(r#"QUILR_LOG_FILE="/var/log/a \"b\"\\c.log""#));
        assert_eq!(
            env_file_value(&rendered, "QUILR_LOG_FILE").as_deref(),
            Some(r#"/var/log/a "b"\c.log"#)
        );
        assert_eq!(env_file_value(&rendered, "QUILR_LOG"), None);
    }

    #[test]
    fn system_and_user_units() {
        let env_file = Path::new("/etc/quilr-connector/connector.env");
        let system = render_unit(Path::new("/usr/local/bin/quilr-connector"), env_file, true);
        assert!(system.contains("ExecStart=/usr/local/bin/quilr-connector\n"));
        assert!(system.contains("EnvironmentFile=/etc/quilr-connector/connector.env\n"));
        assert!(system.contains("Environment=QUILR_LOG_FORMAT=json\n"));
        assert!(system.contains("DynamicUser=yes\n"));
        assert!(system.contains("LogsDirectory=quilr-connector\n"));
        assert!(system.contains("WantedBy=multi-user.target\n"));

        let user = render_unit(
            Path::new("/home/me/my bin/quilr-connector"),
            env_file,
            false,
        );
        assert!(user.contains("ExecStart=\"/home/me/my bin/quilr-connector\"\n"));
        assert!(!user.contains("DynamicUser"));
        assert!(user.contains("WantedBy=default.target\n"));
    }

    #[test]
    fn plist_escapes_and_round_trips() {
        let env = vec![
            (
                "QUILR_CONNECTOR_TOKEN".to_string(),
                "qrc_a&b<c>".to_string(),
            ),
            ("QUILR_LOG_FORMAT".to_string(), "json".to_string()),
        ];
        let plist = render_plist(
            Path::new("/usr/local/bin/quilr-connector"),
            &env,
            Path::new("/tmp/err.log"),
        );
        assert!(plist.contains("<string>qrc_a&amp;b&lt;c&gt;</string>"));
        assert!(plist.contains("<string>ai.quilr.connector</string>"));
        assert_eq!(plist_env(&plist), env);
    }
}
