//! Shared rate-limit / backoff policy used by every subsystem that talks to
//! the outside world on a retry loop: the REST fallback poller (AR-6), the
//! likes/bookmarks poller (AR-7), and the media downloader (AR-8).
//!
//! Consolidates what used to be ad hoc, per-module backoff math into one
//! well-tested state machine: jittered exponential backoff with a
//! configurable cap, honoring a server-provided retry hint (`Retry-After` /
//! `ratelimit-reset`, parsed by [`crate::bluesky`]) over the computed delay
//! when present, and a circuit breaker that opens after too many consecutive
//! failures against the same endpoint so a struggling endpoint gets a much
//! longer rest instead of being hammered at the normal backoff cadence.
//!
//! The clock is injectable via [`Clock`] so tests can assert on backoff
//! *decisions* (how long would we wait, is the circuit open) without
//! sleeping for real.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;

/// A source of the current time, abstracted so tests can drive it manually
/// instead of sleeping for real durations.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Production clock: the real system monotonic clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Tunables for a [`Backoff`] instance. All fields have sane production
/// defaults via [`BackoffConfig::new`].
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Delay after the first failure.
    pub base_delay: Duration,
    /// Upper bound the computed (pre-jitter) delay backs off to.
    pub max_delay: Duration,
    /// Growth factor applied per consecutive failure (e.g. `2.0` doubles).
    pub multiplier: f64,
    /// Symmetric jitter fraction applied to the computed delay, e.g. `0.2`
    /// means +/-20%.
    pub jitter_fraction: f64,
    /// Number of consecutive failures after which the circuit breaker opens.
    pub circuit_breaker_threshold: u32,
    /// How long the circuit breaker stays open once it trips, substantially
    /// longer than the normal backoff cadence.
    pub circuit_breaker_cooldown: Duration,
}

impl BackoffConfig {
    /// A production configuration built from a baseline delay: caps growth
    /// at `max_delay`, dies down with +/-20% jitter, and trips the circuit
    /// breaker after 5 consecutive failures for 10x `max_delay`.
    pub fn new(base_delay: Duration, max_delay: Duration) -> Self {
        let max_delay = max_delay.max(base_delay);
        BackoffConfig {
            base_delay,
            max_delay,
            multiplier: 2.0,
            jitter_fraction: 0.2,
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown: max_delay.saturating_mul(10),
        }
    }
}

/// Jittered exponential backoff with a circuit breaker, shared by every
/// retry loop in the app. Not `Sync`-shared: each subsystem/endpoint owns
/// its own instance (that's what makes the circuit breaker "per endpoint").
pub struct Backoff {
    config: BackoffConfig,
    clock: Arc<dyn Clock>,
    current_delay: Duration,
    consecutive_failures: u32,
    circuit_open_until: Option<Instant>,
}

impl Backoff {
    /// Builds a backoff policy using the real system clock.
    pub fn new(config: BackoffConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Builds a backoff policy against an injected clock, for tests.
    pub fn with_clock(config: BackoffConfig, clock: Arc<dyn Clock>) -> Self {
        let current_delay = config.base_delay;
        Backoff {
            config,
            clock,
            current_delay,
            consecutive_failures: 0,
            circuit_open_until: None,
        }
    }

    /// Records a success: resets the backoff interval to baseline, clears
    /// the consecutive-failure count, and closes the circuit breaker.
    pub fn on_success(&mut self) {
        self.current_delay = self.config.base_delay;
        self.consecutive_failures = 0;
        self.circuit_open_until = None;
    }

    /// Records a failure and returns how long to wait before the next
    /// attempt. `server_hint`, if present (parsed from a `Retry-After` or
    /// `ratelimit-reset` response header), is honored over the computed
    /// backoff — unless the circuit breaker is open, in which case its
    /// cooldown wins since it's deliberately much longer.
    pub fn on_failure(&mut self, server_hint: Option<Duration>) -> Duration {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.current_delay = grow_capped(
            self.current_delay,
            self.config.multiplier,
            self.config.max_delay,
        );

        if self.consecutive_failures >= self.config.circuit_breaker_threshold {
            let now = self.clock.now();
            self.circuit_open_until = Some(now + self.config.circuit_breaker_cooldown);
        }

        if let Some(open_until) = self.circuit_open_until {
            let now = self.clock.now();
            if open_until > now {
                return open_until - now;
            }
        }

        if let Some(hint) = server_hint {
            return hint;
        }

        jittered(self.current_delay, self.config.jitter_fraction)
    }

    /// Whether the circuit breaker is currently open, i.e. callers should
    /// hold off entirely rather than making another attempt. Once the
    /// cooldown elapses this returns `false` again (a "half-open" probe
    /// attempt is allowed through); if that attempt fails, `on_failure`
    /// re-opens the circuit, and if it succeeds `on_success` closes it.
    pub fn is_open(&self) -> bool {
        match self.circuit_open_until {
            Some(until) => self.clock.now() < until,
            None => false,
        }
    }

    /// The current (pre-jitter) backoff delay, for logging/inspection.
    pub fn current_delay(&self) -> Duration {
        self.current_delay
    }
}

/// Grows `current` by `multiplier`, capped at `max`. Shared by every retry
/// loop that grows a delay on repeated failure/empty results, so they all
/// use the same growth curve.
pub fn grow_capped(current: Duration, multiplier: f64, max: Duration) -> Duration {
    let grown = current.mul_f64(multiplier.max(1.0));
    grown.min(max)
}

/// Applies symmetric jitter to `base`: a random factor in
/// `[1 - fraction, 1 + fraction]`.
pub fn jittered(base: Duration, fraction: f64) -> Duration {
    let fraction = fraction.clamp(0.0, 1.0);
    let millis = base.as_millis().max(1) as f64;
    let factor = rand::thread_rng().gen_range((1.0 - fraction)..=(1.0 + fraction));
    Duration::from_millis((millis * factor).round() as u64)
}

/// A manually-advanceable clock for tests: starts at the real "now" and only
/// moves forward when [`FakeClock::advance`] is called, so backoff/circuit
/// decisions can be asserted on instantly with no real sleeping.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct FakeClock {
    now: Arc<std::sync::Mutex<Instant>>,
}

#[cfg(test)]
impl FakeClock {
    pub fn new() -> Self {
        FakeClock {
            now: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().expect("fake clock mutex poisoned");
        *now += duration;
    }

    pub fn as_arc(&self) -> Arc<dyn Clock> {
        Arc::new(self.clone())
    }
}

#[cfg(test)]
impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().expect("fake clock mutex poisoned")
    }
}

/// A soft process-wide cap on outbound Bluesky-related HTTP requests in
/// flight at once (REST polling + media downloads combined), so a burst of
/// new content can't fire off dozens of simultaneous requests even though
/// each subsystem's own concurrency cap looks individually reasonable.
#[derive(Clone)]
pub struct RequestLimiter {
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl RequestLimiter {
    pub fn new(max_in_flight: usize) -> Self {
        RequestLimiter {
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_in_flight.max(1))),
        }
    }

    /// Waits for a slot, returning a guard that frees it on drop. The
    /// semaphore is never closed, so this only fails if that invariant is
    /// somehow violated.
    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("request limiter semaphore is never closed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(threshold: u32, cooldown: Duration) -> BackoffConfig {
        BackoffConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(8),
            multiplier: 2.0,
            jitter_fraction: 0.0,
            circuit_breaker_threshold: threshold,
            circuit_breaker_cooldown: cooldown,
        }
    }

    #[test]
    fn interval_grows_on_repeated_failure_and_caps_at_max() {
        let mut backoff = Backoff::new(config(100, Duration::from_secs(60)));

        assert_eq!(backoff.on_failure(None), Duration::from_secs(2));
        assert_eq!(backoff.on_failure(None), Duration::from_secs(4));
        assert_eq!(backoff.on_failure(None), Duration::from_secs(8));
        // Capped at max_delay, doesn't keep growing past it.
        assert_eq!(backoff.on_failure(None), Duration::from_secs(8));
    }

    #[test]
    fn success_resets_interval_and_failure_count() {
        let mut backoff = Backoff::new(config(100, Duration::from_secs(60)));

        backoff.on_failure(None);
        backoff.on_failure(None);
        assert_eq!(backoff.current_delay(), Duration::from_secs(4));

        backoff.on_success();
        assert_eq!(backoff.current_delay(), Duration::from_secs(1));

        // Growth restarts from baseline after the reset.
        assert_eq!(backoff.on_failure(None), Duration::from_secs(2));
    }

    #[test]
    fn server_hint_is_honored_over_computed_backoff() {
        let mut backoff = Backoff::new(config(100, Duration::from_secs(60)));

        let delay = backoff.on_failure(Some(Duration::from_secs(45)));
        assert_eq!(delay, Duration::from_secs(45));
    }

    #[test]
    fn circuit_breaker_opens_after_n_consecutive_failures() {
        let clock = FakeClock::new();
        let mut backoff = Backoff::with_clock(config(3, Duration::from_secs(60)), clock.as_arc());

        assert!(!backoff.is_open());
        backoff.on_failure(None);
        assert!(!backoff.is_open());
        backoff.on_failure(None);
        assert!(!backoff.is_open());

        let delay = backoff.on_failure(None);
        assert!(backoff.is_open());
        // The tripping failure itself should report the (long) cooldown,
        // not the small computed backoff delay.
        assert_eq!(delay, Duration::from_secs(60));
    }

    #[test]
    fn circuit_breaker_wins_over_a_server_hint_once_open() {
        let clock = FakeClock::new();
        let mut backoff = Backoff::with_clock(config(1, Duration::from_secs(60)), clock.as_arc());

        let delay = backoff.on_failure(Some(Duration::from_secs(5)));
        assert!(backoff.is_open());
        assert_eq!(
            delay,
            Duration::from_secs(60),
            "circuit breaker cooldown should win over a short server hint"
        );
    }

    #[test]
    fn circuit_breaker_allows_a_half_open_retry_after_cooldown() {
        let clock = FakeClock::new();
        let mut backoff = Backoff::with_clock(config(2, Duration::from_secs(30)), clock.as_arc());

        backoff.on_failure(None);
        backoff.on_failure(None);
        assert!(backoff.is_open());

        clock.advance(Duration::from_secs(29));
        assert!(backoff.is_open(), "cooldown has not fully elapsed yet");

        clock.advance(Duration::from_secs(2));
        assert!(
            !backoff.is_open(),
            "cooldown elapsed: a half-open probe attempt should be allowed"
        );

        // A failing probe re-opens the circuit for another full cooldown.
        let delay = backoff.on_failure(None);
        assert!(backoff.is_open());
        assert_eq!(delay, Duration::from_secs(30));

        // A successful probe closes the circuit and resets state fully.
        backoff.on_success();
        assert!(!backoff.is_open());
        assert_eq!(backoff.current_delay(), Duration::from_secs(1));
    }

    #[test]
    fn jittered_stays_within_configured_fraction() {
        let base = Duration::from_secs(100);
        for _ in 0..200 {
            let jittered = jittered(base, 0.2);
            assert!(jittered >= Duration::from_millis(79_000));
            assert!(jittered <= Duration::from_millis(121_000));
        }
    }

    #[test]
    fn zero_jitter_fraction_is_exact() {
        let base = Duration::from_secs(42);
        assert_eq!(jittered(base, 0.0), base);
    }

    #[tokio::test]
    async fn request_limiter_bounds_concurrent_permits() {
        let limiter = RequestLimiter::new(2);
        let _p1 = limiter.acquire().await;
        let _p2 = limiter.acquire().await;

        // A third acquire must not resolve while two permits are held.
        let limiter2 = limiter.clone();
        let mut third = Box::pin(limiter2.acquire());
        assert!(
            futures_util::poll!(&mut third).is_pending(),
            "third permit should not be granted while the cap is saturated"
        );

        drop(_p1);
        assert!(
            futures_util::poll!(&mut third).is_ready(),
            "releasing a permit should let the pending acquire proceed"
        );
    }
}
