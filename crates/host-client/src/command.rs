use crate::link::{Connector, Pipe};
use std::ffi::OsString;
use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

/// Runs a command whose standard input and output are the connection, typically
/// `ssh -T host '~/.tessel-server/host/bin/tessel-host-<version>-<arch>' connect --stdio`.
/// The process is killed when its connection is dropped.
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
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
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
