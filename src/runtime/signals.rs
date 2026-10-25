use crate::error::{Error, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
static INTERRUPTED: AtomicBool = AtomicBool::new(false);
pub fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::Relaxed)
}
#[cfg(unix)]
extern "C" fn signal_handler(_: libc::c_int) {
    INTERRUPTED.store(true, Ordering::Relaxed);
}
pub struct Signals {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    #[cfg(unix)]
    previous: Vec<(libc::c_int, libc::sigaction)>,
}
impl Signals {
    pub fn new() -> Result<(Self, tokio::sync::oneshot::Receiver<()>)> {
        INTERRUPTED.store(false, Ordering::Relaxed);
        #[cfg(unix)]
        let mut previous = vec![];
        #[cfg(unix)]
        for signal in [libc::SIGINT, libc::SIGTERM] {
            let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
            action.sa_sigaction = signal_handler as *const () as usize;
            action.sa_flags = libc::SA_RESTART;
            let mut old = std::mem::MaybeUninit::uninit();
            if unsafe {
                libc::sigemptyset(&mut action.sa_mask);
                libc::sigaction(signal, &action, old.as_mut_ptr())
            } != 0
            {
                return Err(Error::sql(std::io::Error::last_os_error().to_string()));
            }
            previous.push((signal, unsafe { old.assume_init() }));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                if INTERRUPTED.load(Ordering::Relaxed) {
                    let _ = sender.send(());
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        Ok((
            Self {
                stop,
                thread: Some(thread),
                #[cfg(unix)]
                previous,
            },
            receiver,
        ))
    }
}
impl Drop for Signals {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        #[cfg(unix)]
        for (signal, old) in &self.previous {
            unsafe {
                libc::sigaction(*signal, old, std::ptr::null_mut());
            }
        }
    }
}
