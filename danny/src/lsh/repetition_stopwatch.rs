use crate::logging::*;

use std::clone::Clone;
use std::time::Instant;
use timely::logging::Logger;

pub struct RepetitionStopWatch {
    start: Option<Instant>,
    counter: usize,
    name: String,
    logger: Option<Logger<(LogEvent, usize)>>,
    verbose: bool,
}

impl RepetitionStopWatch {
    pub fn new(name: &str, verbose: bool, logger: Option<Logger<(LogEvent, usize)>>) -> Self {
        Self {
            start: None,
            counter: 0usize,
            name: name.to_owned(),
            logger: logger,
            verbose,
        }
    }

    pub fn start(&mut self) {
        self.start.replace(Instant::now());
    }

    pub fn maybe_stop(&mut self) {
        if let Some(start) = self.start.take() {
            let elapsed = Instant::now() - start;
            if self.verbose {
                info!("{} {} ended in {:?}", self.name, self.counter, elapsed);
            }
            // log_event!(
            //     self.logger,
            //     LogEvent::Profile(self.counter, 0, self.name.clone(), elapsed)
            // );
            self.counter += 1;
        }
    }
}
