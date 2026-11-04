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
/// The process is killed when its connection is dropped. On Windows it starts without a console
/// window of its own (`CREATE_NO_WINDOW`), so a desktop application does not flash one at each
/// connection or reconnection.
#[derive(Clone)]
pub struct CommandConnector {
    program: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    current: Arc<Mutex<Option<u32>>>,
}

struct Killer(Child);

impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
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
            guard: Some(Box::new(Killer(child))),
        })
    }
}
