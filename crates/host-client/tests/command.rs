//! The command connector on every platform the desktop runs on: the command's standard input and
//! output are the connection, its standard error is not, and dropping the pipe ends the command.
//! On Windows this runs the same spawn path as a desktop, `CREATE_NO_WINDOW` included.
use std::io::{Read, Write};
use tessel_host_client::{CommandConnector, Connector};

/// A program that copies its input to its output (sorted, which changes nothing for one line).
fn echo() -> CommandConnector {
    #[cfg(windows)]
    {
        let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
        let sort = std::path::Path::new(&root)
            .join("System32")
            .join("sort.exe");
        CommandConnector::new(sort, std::iter::empty::<&str>())
    }
    #[cfg(not(windows))]
    {
        CommandConnector::new("cat", std::iter::empty::<&str>())
    }
}

#[test]
fn the_command_is_the_connection() {
    let connector = echo();
    assert_eq!(connector.current_pid(), None);
    let mut pipe = connector.connect().unwrap();
    assert!(connector.current_pid().is_some());
    pipe.writer.write_all(b"hello tessel\n").unwrap();
    drop(pipe.writer);
    let mut output = String::new();
    pipe.reader.read_to_string(&mut output).unwrap();
    assert_eq!(output.trim_end(), "hello tessel");
}

#[test]
fn dropping_the_pipe_ends_the_command() {
    let connector = echo();
    let pipe = connector.connect().unwrap();
    let Some(guard) = pipe.guard else {
        panic!("the pipe keeps its process")
    };
    let mut reader = pipe.reader;
    // The command waits for input that never comes; dropping the guard kills it, so its output
    // ends instead of blocking for ever.
    drop(guard);
    let mut rest = Vec::new();
    let _ = reader.read_to_end(&mut rest);
    drop(pipe.writer);
}

/// On Linux (where `libc` is available to the tests and CI runs), a pid that was killed but whose
/// reaper has not collected it yet lingers as a zombie: counted as gone, so timing never flakes.
#[cfg(target_os = "linux")]
fn gone_or_zombie(pid: i32) -> bool {
    // SAFETY: signal 0 only checks whether the pid exists.
    if unsafe { libc::kill(pid, 0) } != 0 {
        return true;
    }
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // `pid (comm) state ...`; comm may hold ')' and spaces, so read the state after the last.
        Ok(stat) => stat
            .rsplit(')')
            .next()
            .is_some_and(|rest| rest.trim_start().starts_with('Z')),
        Err(_) => true,
    }
}

#[cfg(target_os = "linux")]
#[test]
fn dropping_the_pipe_ends_the_whole_process_group() {
    use std::io::BufRead;
    use std::time::{Duration, Instant};
    // The shell leads its own group; the backgrounded sleep is a grandchild in that same group.
    let connector = CommandConnector::new("sh", ["-c", "sleep 30 & echo $!; wait"]);
    let pipe = connector.connect().unwrap();
    let mut reader = std::io::BufReader::new(pipe.reader);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let grandchild: i32 = line.trim().parse().expect("the grandchild's pid");
    // SAFETY: signal 0 only checks whether the pid exists.
    assert_eq!(
        unsafe { libc::kill(grandchild, 0) },
        0,
        "the grandchild is alive before the connection is dropped"
    );
    drop(pipe.guard);
    drop(pipe.writer);
    drop(reader);
    // Dropping the connection kills ssh's whole group, the grandchild included (not ssh alone).
    let deadline = Instant::now() + Duration::from_secs(5);
    while !gone_or_zombie(grandchild) {
        assert!(
            Instant::now() < deadline,
            "the grandchild outlived the connection"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_missing_program_is_an_error_not_a_panic() {
    let connector = CommandConnector::new(
        "tessel-host-test-no-such-program",
        std::iter::empty::<&str>(),
    );
    assert!(connector.connect().is_err());
    assert_eq!(connector.current_pid(), None);
}
