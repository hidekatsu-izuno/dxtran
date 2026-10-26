use crate::error::{Error, Result};
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
static INTERRUPTED: LazyLock<Arc<AtomicBool>> = LazyLock::new(|| Arc::new(AtomicBool::new(false)));
pub fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::Relaxed)
}
pub struct Signals {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    #[cfg(unix)]
    registrations: Vec<signal_hook::SigId>,
}
impl Signals {
    pub fn new() -> Result<(Self, tokio::sync::oneshot::Receiver<()>)> {
        INTERRUPTED.store(false, Ordering::Relaxed);
        let mut signals = Self {
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
            #[cfg(unix)]
            registrations: vec![],
        };
        #[cfg(unix)]
        for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
            let registration = signal_hook::flag::register(signal, Arc::clone(&INTERRUPTED))
                .map_err(|e| Error::sql(e.to_string()))?;
            signals.registrations.push(registration);
        }
        let stopped = signals.stop.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        signals.thread = Some(
            std::thread::Builder::new()
                .spawn(move || {
                    while !stopped.load(Ordering::Relaxed) {
                        if INTERRUPTED.load(Ordering::Relaxed) {
                            let _ = sender.send(());
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                })
                .map_err(|e| Error::sql(e.to_string()))?,
        );
        Ok((signals, receiver))
    }
}
impl Drop for Signals {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        // At CLI shutdown, remove our actions; signal-hook keeps its OS handlers installed.
        #[cfg(unix)]
        for registration in self.registrations.drain(..) {
            signal_hook::low_level::unregister(registration);
        }
    }
}
