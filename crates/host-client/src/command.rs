use crate::link::{Connector, Pipe};
use std::ffi::OsString;
use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

/// Runs a command whose standard input and output are the connection, typically the system
/// ssh with `tessel_host::install`'s connect command as one argument:
/// `CommandConnector::new("ssh", ["-T", host, &connect])`, where `connect` is
/// `"$HOME/.tessel-server/host/bin/tessel-host-<version>-<arch>" connect --stdio` (double
/// quotes, so the host's shell expands `$HOME`). Never build that command here.
///
/// Dropping the connection ends the command **and every process it started** (an ssh's proxy
/// command, an askpass), not ssh alone: on Unix the command leads its own process group, killed
/// as a group; on Windows it runs in a kill-on-close job object. On Windows it also starts without
/// a console window of its own (`CREATE_NO_WINDOW`), so a desktop application does not flash one
/// at each connection or reconnection.
#[derive(Clone)]
pub struct CommandConnector {
    program: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    current: Arc<Mutex<Option<u32>>>,
}

impl CommandConnector {
    pub fn new(
        program: impl Into<OsString>,
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            env: Vec::new(),
            current: Arc::new(Mutex::new(None)),
        }
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// The process id of the current connection's command (to simulate a dropped channel).
    pub fn current_pid(&self) -> Option<u32> {
        *self.current.lock().unwrap()
    }
}

impl Connector for CommandConnector {
    fn connect(&self) -> io::Result<Pipe> {
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // Its own process group (its pid), so dropping the connection kills ssh and any helper
            // it spawned, not ssh alone. Non-interactive, so a background helper stays in it.
            command.process_group(0);
        }
        let mut child = command.spawn()?;
        let writer = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("no stdin"))?;
        let reader = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("no stdout"))?;
        *self.current.lock().unwrap() = Some(child.id());
        Ok(Pipe {
            reader: Box::new(reader),
            writer: Box::new(writer),
            guard: Some(guard_for(child)),
        })
    }
}

#[cfg(unix)]
fn guard_for(child: Child) -> Box<dyn Send> {
    Box::new(GroupKiller(Some(child)))
}

/// Kills the command's whole process group, then reaps it on a thread of its own, so dropping a
/// connection (closing a client or cancelling an opening from the UI thread) never waits for it.
#[cfg(unix)]
struct GroupKiller(Option<Child>);

#[cfg(unix)]
impl Drop for GroupKiller {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            let pid = child.id() as libc::pid_t;
            // SAFETY: a kill to the process group this connector just created with
            // `process_group(0)` and has not reaped; its id cannot have been reused.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            std::thread::spawn(move || {
                let mut child = child;
                let _ = child.wait();
            });
        }
    }
}

#[cfg(windows)]
fn guard_for(child: Child) -> Box<dyn Send> {
    windows_job::guard(child)
}

/// Kills the command alone (used when a job object could not be set up), reaped on its own thread.
#[cfg(windows)]
struct PlainKiller(Option<Child>);

#[cfg(windows)]
impl Drop for PlainKiller {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
    }
}

#[cfg(windows)]
mod windows_job {
    use std::os::windows::io::AsRawHandle;
    use std::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };

    /// Puts the command in a kill-on-close job object, so dropping the connection ends ssh and
    /// every process it started, not ssh alone. A process started in the brief moment between the
    /// spawn and the assignment would escape the job; an ssh has not forked helpers that early. If
    /// the job cannot be set up, the command alone is killed.
    pub(super) fn guard(child: Child) -> Box<dyn Send> {
        // SAFETY: each call below is a documented Win32 job-object call on handles owned here.
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if !job.is_null() {
                let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                // Best effort: kill-on-close also ends the tree if Tessel crashes without dropping
                // the guard. Even if it does not take, `Job::drop` terminates the job on a normal
                // close, so the assignment (membership) is the only step we require to succeed.
                let _ = SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    std::ptr::addr_of!(info).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                );
                if AssignProcessToJobObject(job, child.as_raw_handle() as HANDLE) != 0 {
                    return Box::new(JobKiller {
                        _job: Job(job),
                        _child: child,
                    });
                }
                CloseHandle(job);
            }
        }
        Box::new(super::PlainKiller(Some(child)))
    }

    /// Owns one job handle: terminating it on drop kills every process still in the job.
    struct Job(HANDLE);

    // SAFETY: the handle is owned by this value alone and only touched on drop.
    unsafe impl Send for Job {}

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: our own job handle; terminating it kills every process still in it, then the
            // handle is closed.
            unsafe {
                TerminateJobObject(self.0, 1);
                CloseHandle(self.0);
            }
        }
    }

    /// Terminates the job (killing the whole tree) before the child's own handle is closed: fields
    /// drop in declaration order.
    struct JobKiller {
        _job: Job,
        _child: Child,
    }
}
