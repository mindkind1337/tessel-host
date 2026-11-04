//! Terminal modes the screen model does not keep, followed from the output so a snapshot can
//! set them again: the kitty keyboard protocol flags (`CSI > u`, which programs such as Claude
//! Code and Codex use so Shift+Enter is a new line), one stack for the main screen and one for
//! the alternate screen as that protocol defines; focus reporting (`?1004`); the cursor style
//! (DECSCUSR). A small parser that never allocates beyond a fixed bound.
const MAX_STACK: usize = 16;
const MAX_PARAMS: usize = 64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Parse {
    #[default]
    Ground,
    Escape,
    Csi,
}

#[derive(Clone, Debug, Default)]
struct Keyboard {
    base: u32,
    stack: Vec<u32>,
}

impl Keyboard {
    fn flags(&self) -> u32 {
        self.stack.last().copied().unwrap_or(self.base)
    }

    fn set(&mut self, flags: u32) {
        match self.stack.last_mut() {
            Some(top) => *top = flags,
            None => self.base = flags,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Modes {
    parse: Parse,
    params: Vec<u8>,
    alternate: bool,
    main: Keyboard,
    alt: Keyboard,
    focus: bool,
    cursor_style: u32,
}

fn number(text: &[u8], default: u32) -> u32 {
    if text.is_empty() {
        return default;
    }
    std::str::from_utf8(text)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

impl Modes {
    pub fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.parse = match (self.parse, byte) {
                (_, 0x1b) => Parse::Escape,
                (Parse::Escape, b'[') => {
                    self.params.clear();
                    Parse::Csi
                }
                (Parse::Csi, 0x20..=0x3f) => {
                    if self.params.len() < MAX_PARAMS {
                        self.params.push(byte);
                    }
                    Parse::Csi
                }
                (Parse::Csi, 0x40..=0x7e) => {
                    self.csi(byte);
                    Parse::Ground
                }
                _ => Parse::Ground,
            };
        }
    }

    fn keyboard(&mut self) -> &mut Keyboard {
        if self.alternate {
            &mut self.alt
        } else {
            &mut self.main
        }
    }

    fn csi(&mut self, last: u8) {
        let params = std::mem::take(&mut self.params);
        match (last, params.first()) {
            (b'u', Some(b'>')) => {
                let flags = number(&params[1..], 0);
                let keyboard = self.keyboard();
                if keyboard.stack.len() >= MAX_STACK {
                    keyboard.stack.remove(0);
                }
                keyboard.stack.push(flags);
            }
            (b'u', Some(b'<')) => {
                let count = number(&params[1..], 1) as usize;
                let keyboard = self.keyboard();
                let keep = keyboard.stack.len().saturating_sub(count);
                keyboard.stack.truncate(keep);
            }
            (b'u', Some(b'=')) => {
                let mut fields = params[1..].split(|&b| b == b';');
                let flags = number(fields.next().unwrap_or(&[]), 0);
                let mode = number(fields.next().unwrap_or(&[]), 1);
                let keyboard = self.keyboard();
                let now = keyboard.flags();
                keyboard.set(match mode {
                    2 => now | flags,
                    3 => now & !flags,
                    _ => flags,
                });
            }
            (b'h' | b'l', Some(b'?')) => {
                let on = last == b'h';
                for field in params[1..].split(|&b| b == b';') {
                    match number(field, 0) {
                        1004 => self.focus = on,
                        47 | 1047 | 1049 => self.alternate = on,
                        _ => {}
                    }
                }
            }
            (b'q', _) if params.last() == Some(&b' ') => {
                self.cursor_style = number(&params[..params.len() - 1], 0);
            }
            _ => {}
        }
    }

    /// Escape sequences that set the main screen's keyboard flags.
    pub fn main_keyboard(&self) -> Vec<u8> {
        keyboard_sequence(self.main.flags())
    }

    /// Escape sequences for the screen shown now: its keyboard flags, focus reporting and the
    /// cursor style.
    pub fn restore(&self) -> Vec<u8> {
        let mut bytes = keyboard_sequence(if self.alternate {
            self.alt.flags()
        } else {
            self.main.flags()
        });
        if self.focus {
            bytes.extend_from_slice(b"\x1b[?1004h");
        }
        if self.cursor_style != 0 {
            bytes.extend_from_slice(format!("\x1b[{} q", self.cursor_style).as_bytes());
        }
        bytes
    }
}

fn keyboard_sequence(flags: u32) -> Vec<u8> {
    if flags == 0 {
        Vec::new()
    } else {
        format!("\x1b[>{flags}u").into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyboard_flags_follow_pushes_pops_and_sets_per_screen() {
        let mut modes = Modes::default();
        modes.feed(b"\x1b[>1u");
        assert_eq!(modes.restore(), b"\x1b[>1u");
        modes.feed(b"\x1b[?1049h\x1b[>3");
        modes.feed(b"u");
        assert_eq!(modes.restore(), b"\x1b[>3u");
        assert_eq!(modes.main_keyboard(), b"\x1b[>1u");
        modes.feed(b"\x1b[=4;2u");
        assert_eq!(modes.restore(), b"\x1b[>7u");
        modes.feed(b"\x1b[<u\x1b[?1049l");
        assert_eq!(modes.restore(), b"\x1b[>1u");
        modes.feed(b"\x1b[<5u");
        assert!(modes.restore().is_empty());
    }

    #[test]
    fn focus_and_cursor_style() {
        let mut modes = Modes::default();
        modes.feed(b"\x1b[?1004h\x1b[5 q");
        assert_eq!(modes.restore(), b"\x1b[?1004h\x1b[5 q");
        modes.feed(b"\x1b[?1004l\x1b[0 q");
        assert!(modes.restore().is_empty());
    }

    #[test]
    fn garbage_never_grows_it() {
        let mut modes = Modes::default();
        let mut long = b"\x1b[".to_vec();
        long.extend(std::iter::repeat_n(b'1', 10_000));
        modes.feed(&long);
        assert!(modes.params.len() <= MAX_PARAMS);
        for _ in 0..100 {
            modes.feed(b"\x1b[>1u");
        }
        assert!(modes.main.stack.len() <= MAX_STACK);
    }
}
