//! A pseudo-terminal pair and a program on it, in a session of its own (so its process group is
//! the one signals and a hang-up go to), with an environment built from a small base.
use std::collections::BTreeMap;
use std::ffi::{CStr, OsString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tessel_host_protocol::pty::Size;

pub struct Spawned {
    pub master: File,
    pub pid: u32,
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

pub fn set_size(master: &File, size: Size) -> io::Result<()> {
    let winsize = libc::winsize {
        ws_row: size.rows,
        ws_col: size.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ reads one winsize from the pointer.
    if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ as _, &winsize) } == 0 {
        Ok(())
    } else {
        Err(last_error())
    }
}

const BASE_ENV: [&str; 7] = ["HOME", "USER", "LOGNAME", "SHELL", "PATH", "LANG", "TZ"];

fn environment(extra: &BTreeMap<String, String>) -> Vec<(OsString, OsString)> {
    let mut env: BTreeMap<OsString, OsString> = std::env::vars_os()
        .filter(|(name, _)| {
            let name = name.to_string_lossy();
            BASE_ENV.contains(&name.as_ref()) || name.starts_with("LC_")
        })
        .collect();
    env.entry("PATH".into())
        .or_insert_with(|| "/usr/local/bin:/usr/bin:/bin".into());
    env.insert("TERM".into(), "xterm-256color".into());
    for (name, value) in extra {
        env.insert(name.into(), value.into());
    }
    env.into_iter().collect()
}

/// The user's shell, from the password database, else $SHELL, else /bin/sh.
pub fn login_shell() -> PathBuf {
    let mut buffer = vec![0u8; 16 * 1024];
    // SAFETY: passwd is plain data filled inside `buffer`, which outlives its use.
    let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
    let mut found: *mut libc::passwd = std::ptr::null_mut();
    let result = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            &mut entry,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut found,
        )
    };
    if result == 0 && !found.is_null() && !entry.pw_shell.is_null() {
        // SAFETY: NUL-terminated by getpwuid_r inside `buffer`.
        let shell = unsafe { CStr::from_ptr(entry.pw_shell) }
            .to_string_lossy()
            .into_owned();
        if shell.starts_with('/') {
            return shell.into();
        }
    }
    std::env::var_os("SHELL")
        .map(PathBuf::from)
        .filter(|s| s.is_absolute())
        .unwrap_or_else(|| "/bin/sh".into())
}

fn open_pair() -> io::Result<(File, OwnedFd)> {
    // SAFETY: plain calls; every descriptor returned is wrapped at once.
    unsafe {
        let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if master < 0 {
            return Err(last_error());
        }
        let master = File::from_raw_fd(master);
        if libc::grantpt(master.as_raw_fd()) != 0 || libc::unlockpt(master.as_raw_fd()) != 0 {
            return Err(last_error());
        }
        let mut name = [0 as libc::c_char; 128];
        if libc::ptsname_r(master.as_raw_fd(), name.as_mut_ptr(), name.len()) != 0 {
            return Err(last_error());
        }
        let slave = libc::open(
            name.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        );
        if slave < 0 {
            return Err(last_error());
        }
        Ok((master, OwnedFd::from_raw_fd(slave)))
    }
}

pub fn spawn(
    argv: &[String],
    cwd: Option<&str>,
    extra_env: &BTreeMap<String, String>,
    size: Size,
) -> io::Result<Spawned> {
    let (master, slave) = open_pair()?;
    set_size(&master, size)?;
    let env = environment(extra_env);
    let home = env
        .iter()
        .find(|(n, _)| n == "HOME")
        .map(|(_, v)| PathBuf::from(v))
        .unwrap_or_else(|| "/".into());
    let mut command = match argv.split_first() {
        Some((program, args)) => {
            let mut c = Command::new(program);
            c.args(args);
            c
        }
        None => {
            let shell = login_shell();
            let name = shell
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "sh".into());
            let mut c = Command::new(&shell);
            c.arg0(format!("-{name}"));
            c
        }
    };
    command
        .current_dir(cwd.map(Path::new).unwrap_or(&home))
        .env_clear()
        .envs(env)
        .stdin(Stdio::from(slave.try_clone()?))
        .stdout(Stdio::from(slave.try_clone()?))
        .stderr(Stdio::from(slave));
    // SAFETY: only async-signal-safe calls between fork and exec.
    unsafe {
        command.pre_exec(|| {
            // The daemon ignores hang-ups; the program must not inherit that.
            libc::signal(libc::SIGHUP, libc::SIG_DFL);
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    // Reaped by the terminal's waiter with waitid/waitpid, never by `Child`.
    let pid = child.id();
    drop(child);
    Ok(Spawned { master, pid })
}
