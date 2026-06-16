use std::future::Future;
use std::time::Duration;
use tracing::warn;

#[derive(Clone)]
pub struct RetryStrategy {
    pub max_retries: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryStrategy {
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(16),
        }
    }

    pub fn exponential_backoff(max_retries: u32) -> Self {
        Self::new(max_retries)
    }

    pub fn get_delay(&self, attempt: u32) -> Duration {
        let delay = self.base_delay * 2u32.pow(attempt.min(4));
        delay.min(self.max_delay)
    }

    pub async fn retry<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match f().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if attempt < self.max_retries {
                        let delay = self.get_delay(attempt);
                        warn!(
                            "Attempt {} failed: {}. Retrying in {:?}...",
                            attempt + 1,
                            e,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.expect("No error recorded, this shouldn't happen"))
    }

    pub async fn retry_with_state<F, Fut, T, E, S>(
        &self,
        mut f: F,
        state: S,
    ) -> Result<(T, S), (E, S)>
    where
        F: FnMut(S) -> Fut,
        Fut: Future<Output = Result<(T, S), (E, S)>>,
        E: std::fmt::Display,
    {
        let mut last_error = None;
        let mut current_state = state;

        for attempt in 0..=self.max_retries {
            match f(current_state).await {
                Ok((value, s)) => return Ok((value, s)),
                Err((e, s)) => {
                    current_state = s;
                    if attempt < self.max_retries {
                        let delay = self.get_delay(attempt);
                        warn!(
                            "Attempt {} failed: {}. Retrying in {:?}...",
                            attempt + 1,
                            e,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        Err((
            last_error.expect("No error recorded, this shouldn't happen"),
            current_state,
        ))
    }
}
