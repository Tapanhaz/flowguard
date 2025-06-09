use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use pyo3::exceptions::PyValueError;
use chrono::{DateTime, Timelike, Utc};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::time::{sleep, Duration as TokioDuration};
use std::sync::atomic::{AtomicBool, Ordering, AtomicU64};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};


#[derive(Debug)]
struct TimeWindow {
    limit: u64,
    window_duration_secs: u64,
    current_count: AtomicU64,
    window_start: AtomicU64,
    limit_type: String
}

#[derive(Debug)]
struct AsyncTimeWindow {
    limit: u64,
    window_duration_secs: u64,
    current_count: AtomicU64,
    window_start: AtomicU64,
    limit_type: String
}

#[pyclass]
pub struct RateLimiter {
    windows: Arc<Mutex<Vec<TimeWindow>>>,
    blocking_mode: bool,
    interrupted: Arc<AtomicBool>,
    max_burst_limit: Option<u64>,           
    current_burst_count: Arc<AtomicU64>,    
}

#[pyclass]
pub struct AsyncRateLimiter {
    windows: Arc<Mutex<Vec<AsyncTimeWindow>>>,
    blocking_mode: bool,
    interrupted: Arc<AtomicBool>,
    max_burst_limit: Option<u64>,           
    current_burst_count: Arc<AtomicU64>
}

fn interruptible_sleep(duration: Duration, interrupted: &AtomicBool) -> Result<(), ()> {
    let start = Instant::now();
    let chunk_duration = Duration::from_millis(10); 
    let mut total_sleep = Duration::ZERO;
    // I find 500ms interval is suitable to prevent frequent acquisition of gil.
    let check_interval = Duration::from_millis(500);
    
    while start.elapsed() < duration {
        if interrupted.load(Ordering::SeqCst) {
            return Err(()); 
        }
        
        let elapsed = start.elapsed();
        if elapsed >= duration {
            break;
        }
        
        let remaining = duration - elapsed;
        let sleep_time = remaining.min(chunk_duration);
        
        if sleep_time > Duration::from_millis(0) {
            std::thread::sleep(sleep_time);
            total_sleep += sleep_time;
        }

        if total_sleep >= check_interval {
            let signal = Python::with_gil(|py| {
                py.check_signals()
            });
            total_sleep = Duration::ZERO;
            
            if signal.is_err() {
                interrupted.store(true, Ordering::SeqCst);
                return Err(());
            }

        }
        
    }

    Ok(())
}

impl TimeWindow {
    fn new(limit: u64, window_duration_secs: u64, limit_type: String) -> Self {
        
        Self {
            limit,
            window_duration_secs,
            current_count: AtomicU64::new(0),
            window_start: AtomicU64::new(0), 
            limit_type
        }
    }

    fn reset_if_needed(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let old_start = self.window_start.load(Ordering::Acquire);
        
        if old_start == 0 {
            let initial_start = Self::get_window_start(now_secs, self.window_duration_secs);
            self.window_start.compare_exchange(
                0,
                initial_start,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).ok();
            return;
        }
        
        let current_window_start = Self::get_window_start(now_secs, self.window_duration_secs);
        if current_window_start > old_start {
            if self.window_start.compare_exchange(
                old_start,
                current_window_start,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                self.current_count.store(0, Ordering::Release);
            }
        }
    }

    fn get_window_start(timestamp: u64, window_duration_secs: u64) -> u64 {
        match window_duration_secs {
            1 => timestamp,
            60 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_second(0).unwrap().timestamp() as u64
            },
            3600 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_minute(0).unwrap()
                  .with_second(0).unwrap()
                  .timestamp() as u64
            },
            86400 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_hour(0).unwrap()
                  .with_minute(0).unwrap()
                  .with_second(0).unwrap()
                  .timestamp() as u64
            },
            _ => {
                (timestamp / window_duration_secs) * window_duration_secs
            }
        }
    }

    fn try_acquire(&self) -> bool {
        self.reset_if_needed();
        
        loop {
            let current = self.current_count.load(Ordering::Acquire);
            if current >= self.limit {
                return false;
            }
            
            match self.current_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire
            ) {
                Ok(_) => return true,
                Err(_) => continue, 
            }
        }
    }

    fn rollback_acquire(&self) {
        self.current_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn available_permits(&self) -> u64 {
        self.reset_if_needed();
        let current = self.current_count.load(Ordering::Acquire);
        self.limit.saturating_sub(current)
    }

    fn time_until_next_permit(&self) -> Option<Duration> {
        self.reset_if_needed();
        
        let current = self.current_count.load(Ordering::Acquire);
        if current < self.limit {
            return None;
        }
    
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        let window_start = self.window_start.load(Ordering::Acquire);
        let next_window_start_nanos = (window_start + self.window_duration_secs) * 1_000_000_000;
        
        if next_window_start_nanos > now_nanos {
            Some(Duration::from_nanos(next_window_start_nanos - now_nanos))
        } else {
            Some(Duration::from_nanos(0))
        }
    }
}

impl AsyncTimeWindow {
    fn new(limit: u64, window_duration_secs: u64, limit_type: String) -> Self {
        Self {
            limit,
            window_duration_secs,
            current_count: AtomicU64::new(0),
            window_start: AtomicU64::new(0), 
            limit_type
        }
    }

    fn reset_if_needed(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let old_start = self.window_start.load(Ordering::Acquire);
        
        if old_start == 0 {
            let initial_start = Self::get_window_start(now_secs, self.window_duration_secs);
            self.window_start.compare_exchange(
                0,
                initial_start,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).ok();
            return;
        }
        
        let current_window_start = Self::get_window_start(now_secs, self.window_duration_secs);
        if current_window_start > old_start {
            if self.window_start.compare_exchange(
                old_start,
                current_window_start,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                self.current_count.store(0, Ordering::Release);
            }
        }
    }

    fn get_window_start(timestamp: u64, window_duration_secs: u64) -> u64 {
        match window_duration_secs {
            1 => timestamp,
            60 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_second(0).unwrap().timestamp() as u64
            },
            3600 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_minute(0).unwrap()
                  .with_second(0).unwrap()
                  .timestamp() as u64
            },
            86400 => {
                let dt = DateTime::from_timestamp(timestamp as i64, 0)
                    .unwrap_or_else(|| Utc::now());
                dt.with_hour(0).unwrap()
                  .with_minute(0).unwrap()
                  .with_second(0).unwrap()
                  .timestamp() as u64
            },
            _ => {
                (timestamp / window_duration_secs) * window_duration_secs
            }
        }
    }

    fn try_acquire(&self) -> bool {
        self.reset_if_needed();
        
        loop {
            let current = self.current_count.load(Ordering::Acquire);
            if current >= self.limit {
                return false;
            }
            
            match self.current_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire
            ) {
                Ok(_) => return true,
                Err(_) => continue, 
            }
        }
    }

    fn rollback_acquire(&self) {
        self.current_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn available_permits(&self) -> u64 {
        self.reset_if_needed();
        let current = self.current_count.load(Ordering::Acquire);
        self.limit.saturating_sub(current)
    }

    fn time_until_next_permit(&self) -> Option<TokioDuration> {
        self.reset_if_needed();
        
        let current = self.current_count.load(Ordering::Acquire);
        if current < self.limit {
            return None;
        }
    
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        let window_start = self.window_start.load(Ordering::Acquire);
        let next_window_start_nanos = (window_start + self.window_duration_secs) * 1_000_000_000;
        
        if next_window_start_nanos > now_nanos {
            Some(TokioDuration::from_nanos(next_window_start_nanos - now_nanos))
        } else {
            Some(TokioDuration::from_nanos(0))
        }
    }
}

impl RateLimiter {
    fn build_windows(
        sec: Option<u64>,
        min: Option<u64>, 
        hour: Option<u64>,
        day: Option<u64>,
        sec_window: Option<u64>,
        min_window: Option<u64>,
        hour_window: Option<u64>, 
        day_window: Option<u64>
    ) -> PyResult<Vec<TimeWindow>> {
        let mut windows = Vec::new();
        
        let limits = [
            (sec, sec_window.unwrap_or(1), "second"),        
            (min, min_window.unwrap_or(1) * 60, "minute"),       
            (hour, hour_window.unwrap_or(1) * 3600, "hour"),     
            (day, day_window.unwrap_or(1) * 86400, "day")        
        ];
    
        for (limit_opt, duration_secs, limit_type) in limits {
            if let Some(limit) = limit_opt {
                if limit == 0 {
                    return Err(PyValueError::new_err(
                        format!("Rate limit for {} must be greater than 0", limit_type)
                    ));
                }
                windows.push(TimeWindow::new(limit, duration_secs, limit_type.to_string())); 
            }
        }
        
        Ok(windows)
    }
    
    fn compute_wait_duration(&self) -> Option<Duration> {
        let windows_guard = self.windows.lock().ok()?;
        let mut min_wait: Option<Duration> = None;
    
        for window in windows_guard.iter() {
            if let Some(wait_time) = window.time_until_next_permit() {
                min_wait = Some(match min_wait {
                    Some(current) => current.min(wait_time),
                    None => wait_time,
                });
            }
        }
    
        min_wait.map(|d| d.max(Duration::from_millis(1)))
    }

    fn try_acquire_limits(&self) -> PyResult<Option<()>> {
        let mut burst_acquired = false;
        if let Some(max_burst) = self.max_burst_limit {
            loop {
                let current = self.current_burst_count.load(Ordering::Acquire);
                if current >= max_burst {
                    return Ok(None); 
                }
                
                match self.current_burst_count.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire
                ) {
                    Ok(_) => {
                        burst_acquired = true;
                        break;
                    }
                    Err(_) => continue, 
                }
            }
        }

        let windows_guard = self.windows.lock();
        match windows_guard {
            Ok(windows) => {
                let mut acquired_count = 0;
                
                for window in windows.iter() {
                    if window.try_acquire() {
                        acquired_count += 1;
                    } else {
                        for i in 0..acquired_count {
                            windows[i].rollback_acquire();
                        }
                        if burst_acquired {
                            self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                        }
                        return Ok(None); 
                    }
                }
                
                Ok(Some(()))
            }
            Err(_) => {
                if burst_acquired {
                    self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                }
                Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"))
            }
        }
    }

    fn acquire_with_backoff(&self) -> PyResult<Option<()>> {
        let mut attempt = 0u64;
        let max_backoff_ms = 1000;
        let mut burst_acquired = false;
        
        loop {
            if self.interrupted.load(Ordering::SeqCst) {
                if burst_acquired {
                    self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                }
                return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
            }

            if let Some(max_burst) = self.max_burst_limit {
                if !burst_acquired {
                    loop {
                        let current = self.current_burst_count.load(Ordering::Acquire);
                        if current >= max_burst {
                            break;
                        }
                        
                        match self.current_burst_count.compare_exchange_weak(
                            current,
                            current + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire
                        ) {
                            Ok(_) => {
                                burst_acquired = true;
                                break;
                            }
                            Err(_) => continue, 
                        }
                    }
                    
                    if !burst_acquired {
                        let backoff_ms = (2u64.saturating_pow(attempt.min(6).try_into().unwrap()) * 10).min(100);
                        if let Err(_) = interruptible_sleep(Duration::from_millis(backoff_ms), &self.interrupted) {
                            return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
                        }
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                }
            }

            let windows_guard = self.windows.lock();
            match windows_guard {
                Ok(windows) => {
                    let mut acquired_count = 0;
                    let mut all_acquired = true;
                    
                    for window in windows.iter() {
                        if window.try_acquire() {
                            acquired_count += 1;
                        } else {
                            for i in 0..acquired_count {
                                windows[i].rollback_acquire();
                            }
                            all_acquired = false;
                            break;
                        }
                    }
                    
                    if all_acquired {
                        return Ok(Some(())); 
                    }
                }
                Err(_) => {
                    if burst_acquired {
                        self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                    }
                    return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
                }
            }
            
            if let Some(wait_duration) = self.compute_wait_duration() {
                let wait_ms = wait_duration.as_millis().min(max_backoff_ms as u128) as u64;
                let backoff_ms = (2u64.saturating_pow(attempt.min(10).try_into().unwrap()) * 10).min(wait_ms);
                
                if let Err(_) = interruptible_sleep(Duration::from_millis(backoff_ms), &self.interrupted) {
                    if burst_acquired {
                        self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                    }
                    return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
                }
            } else {
                let backoff_ms = (2u64.saturating_pow(attempt.min(6).try_into().unwrap()) * 10).min(100);
                if let Err(_) = interruptible_sleep(Duration::from_millis(backoff_ms), &self.interrupted) {
                    if burst_acquired {
                        self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                    }
                    return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
                }
            }
            
            attempt = attempt.saturating_add(1);
        }
    }

}

impl AsyncRateLimiter {
    fn build_windows(
        sec: Option<u64>,
        min: Option<u64>, 
        hour: Option<u64>,
        day: Option<u64>,
        sec_window: Option<u64>,
        min_window: Option<u64>,
        hour_window: Option<u64>, 
        day_window: Option<u64>
    ) -> PyResult<Vec<AsyncTimeWindow>> {
        let mut windows = Vec::new();
        
        let limits = [
            (sec, sec_window.unwrap_or(1), "second"),        
            (min, min_window.unwrap_or(1) * 60, "minute"),       
            (hour, hour_window.unwrap_or(1) * 3600, "hour"),     
            (day, day_window.unwrap_or(1) * 86400, "day")        
        ];
    
        for (limit_opt, duration_secs, limit_type) in limits {
            if let Some(limit) = limit_opt {
                if limit == 0 {
                    return Err(PyValueError::new_err(
                        format!("Rate limit for {} must be greater than 0", limit_type)
                    ));
                }
                windows.push(AsyncTimeWindow::new(limit, duration_secs, limit_type.to_string())); 
            }
        }
        
        Ok(windows)
    }
    
    fn compute_wait_duration(&self) -> Option<TokioDuration> {
        let windows_guard = self.windows.lock().ok()?;
        let mut min_wait: Option<TokioDuration> = None;
    
        for window in windows_guard.iter() {
            if let Some(wait_time) = window.time_until_next_permit() {
                min_wait = Some(match min_wait {
                    Some(current) => current.min(wait_time),
                    None => wait_time,
                });
            }
        }
    
        min_wait.map(|d| d.max(TokioDuration::from_millis(1)))
    }

    async fn try_acquire_limits_async(&self) -> PyResult<Option<()>> {
        let mut burst_acquired = false;
        if let Some(max_burst) = self.max_burst_limit {
            loop {
                let current = self.current_burst_count.load(Ordering::Acquire);
                if current >= max_burst {
                    return Ok(None); 
                }
                
                match self.current_burst_count.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire
                ) {
                    Ok(_) => {
                        burst_acquired = true;
                        break;
                    }
                    Err(_) => continue, 
                }
            }
        }

        let windows_guard = self.windows.lock();
        match windows_guard {
            Ok(windows) => {
                let mut acquired_count = 0;
                
                for window in windows.iter() {
                    if window.try_acquire() {
                        acquired_count += 1;
                    } else {
                        for i in 0..acquired_count {
                            windows[i].rollback_acquire();
                        }
                        if burst_acquired {
                            self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                        }
                        return Ok(None); 
                    }
                }
                
                Ok(Some(()))
            }
            Err(_) => {
                if burst_acquired {
                    self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                }
                Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"))
            }
        }
    }

    async fn acquire_with_backoff_async(&self) -> PyResult<Option<()>> {
        let mut attempt = 0u64;
        let max_backoff_ms = 1000;
        let mut burst_acquired = false;
        
        loop {
            if self.interrupted.load(Ordering::SeqCst) {
                if burst_acquired {
                    self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                }
                return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
            }
    
            if let Some(max_burst) = self.max_burst_limit {
                if !burst_acquired {
                    loop {
                        let current = self.current_burst_count.load(Ordering::Acquire);
                        if current >= max_burst {
                            break; 
                        }
                        
                        match self.current_burst_count.compare_exchange_weak(
                            current,
                            current + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire
                        ) {
                            Ok(_) => {
                                burst_acquired = true;
                                break;
                            }
                            Err(_) => continue, 
                        }
                    }
                    
                    if !burst_acquired {
                        let backoff_ms = (2u64.saturating_pow(attempt.min(6).try_into().unwrap()) * 10).min(100);
                        sleep(TokioDuration::from_millis(backoff_ms)).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                }
            }

            let all_acquired = {
                let windows_guard = self.windows.lock();
                match windows_guard {
                    Ok(windows) => {
                        let mut acquired_count = 0;
                        let mut all_acquired = true;
                        
                        for window in windows.iter() {
                            if window.try_acquire() {
                                acquired_count += 1;
                            } else {
                                for i in 0..acquired_count {
                                    windows[i].rollback_acquire();
                                }
                                all_acquired = false;
                                break;
                            }
                        }
                        
                        all_acquired
                    }
                    Err(_) => {
                        if burst_acquired {
                            self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
                        }
                        return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
                    }
                }
            };
            
            if all_acquired {
                return Ok(Some(())); 
            }
            
            if let Some(wait_duration) = self.compute_wait_duration() {
                let wait_ms = wait_duration.as_millis().min(max_backoff_ms as u128) as u64;
                let backoff_ms = (2u64.saturating_pow(attempt.min(10).try_into().unwrap()) * 10).min(wait_ms);
                
                sleep(TokioDuration::from_millis(backoff_ms)).await;
            } else {
                let backoff_ms = (2u64.saturating_pow(attempt.min(6).try_into().unwrap()) * 10).min(100);
                sleep(TokioDuration::from_millis(backoff_ms)).await;
            }
            
            attempt = attempt.saturating_add(1);
        }
    }

}

#[pymethods]
impl RateLimiter {
    #[new]
    #[pyo3(signature = (
        sec=None, 
        min=None, 
        hour=None, 
        day=None, 
        sec_window=None, 
        min_window=None, 
        hour_window=None, 
        day_window=None, 
        blocking=true, 
        max_burst=None
    ))]
    fn new(
        sec: Option<u64>,
        min: Option<u64>, 
        hour: Option<u64>,
        day: Option<u64>,
        sec_window: Option<u64>, 
        min_window: Option<u64>, 
        hour_window: Option<u64>, 
        day_window: Option<u64>,
        blocking: bool,
        max_burst: Option<u64>
    ) -> PyResult<Self> {
        
        let windows = Self::build_windows(sec, min, hour, day, sec_window, min_window, hour_window, day_window)?;
        
        if windows.is_empty() {
            return Err(PyValueError::new_err("At least one rate limit must be specified"));
        }

        if let Some(burst) = max_burst {
            if burst == 0 {
                return Err(PyValueError::new_err("max_burst must be greater than 0"));
            }
        }

        Ok(Self {
            windows: Arc::new(Mutex::new(windows)),
            blocking_mode: blocking,
            interrupted: Arc::new(AtomicBool::new(false)),
            max_burst_limit: max_burst,                   
            current_burst_count: Arc::new(AtomicU64::new(0)),
        })
    }

    fn acquire(&self, py: Python<'_>) -> PyResult<bool> {  
        self.interrupted.store(false, Ordering::SeqCst);    
        
        let result = if !self.blocking_mode {
            py.allow_threads(|| {
                self.try_acquire_limits().map(|opt| opt.is_some())
            })
        } else {
            py.allow_threads(|| {
                self.acquire_with_backoff().map(|opt| opt.is_some())
            })
        };
        
        let signal = py.check_signals();
        if signal.is_err() {
            self.interrupted.store(true, Ordering::SeqCst);
            return Err(pyo3::exceptions::PyKeyboardInterrupt::new_err("Operation interrupted"));
        }
        
        result 
    }
    

    fn release(&self) -> PyResult<()> {
        if self.max_burst_limit.is_some() {
            self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(())
    }

    fn get_remaining(&self) -> PyResult<HashMap<String, u64>> {
        let mut result = HashMap::with_capacity(8);
    
        if let Ok(windows_guard) = self.windows.lock() {
            for window in windows_guard.iter() {
                result.insert(window.limit_type.clone(), window.available_permits());
            }
        }

        if let Some(max_burst) = self.max_burst_limit {
            let current = self.current_burst_count.load(Ordering::Acquire);
            result.insert("burst".to_string(), max_burst.saturating_sub(current));
        }

        Ok(result)
    }

    fn wait_time(&self) -> PyResult<Option<f64>> {
        Ok(self.compute_wait_duration().map(|d| d.as_secs_f64()))
    }

    fn __enter__(&self, py: Python<'_>) -> PyResult<()> {
        self.interrupted.store(false, Ordering::SeqCst);
        
        if !self.acquire(py)? {  
            return Err(PyValueError::new_err("Failed to acquire permit"));
        }
        Ok(())
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>
    ) -> PyResult<bool> {
        if self.max_burst_limit.is_some() {
            self.current_burst_count.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(false)
    }
}

#[pymethods]
impl AsyncRateLimiter {
    #[new]
    #[pyo3(signature = (
        sec=None, 
        min=None, 
        hour=None, 
        day=None, 
        sec_window=None, 
        min_window=None, 
        hour_window=None, 
        day_window=None, 
        blocking=true, 
        max_burst=None
    ))]
    fn new(
        sec: Option<u64>,
        min: Option<u64>, 
        hour: Option<u64>,
        day: Option<u64>,
        sec_window: Option<u64>, 
        min_window: Option<u64>, 
        hour_window: Option<u64>, 
        day_window: Option<u64>,
        blocking: bool,
        max_burst: Option<u64>,
    ) -> PyResult<Self> {
        
        let windows = Self::build_windows(sec, min, hour, day, sec_window, min_window, hour_window, day_window)?;
        
        if windows.is_empty() {
            return Err(PyValueError::new_err("At least one rate limit must be specified"));
        }

        if let Some(burst) = max_burst {
            if burst == 0 {
                return Err(PyValueError::new_err("max_burst must be greater than 0"));
            }
        }

        Ok(Self {
            windows: Arc::new(Mutex::new(windows)),
            blocking_mode: blocking,
            interrupted: Arc::new(AtomicBool::new(false)),
            max_burst_limit: max_burst,
            current_burst_count: Arc::new(AtomicU64::new(0))
        })
    }

    fn acquire<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.interrupted.store(false, Ordering::SeqCst);
        
        if !self.blocking_mode {
            let limiter = Arc::new(self.clone());
            future_into_py(py, async move {
                limiter.try_acquire_limits_async().await
            })
        } else {
            let limiter = Arc::new(self.clone());
            future_into_py(py, async move {
                limiter.acquire_with_backoff_async().await
            })
        }
    }

    fn release<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let limiter = Arc::new(self.clone());
        future_into_py(py, async move {
            if limiter.max_burst_limit.is_some() {
                limiter.current_burst_count.fetch_sub(1, Ordering::AcqRel);
            }
            Ok(())
        })
    }

    fn get_remaining<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let limiter = Arc::new(self.clone());
        future_into_py(py, async move {
            let mut result = HashMap::with_capacity(8);
        
            if let Ok(windows_guard) = limiter.windows.lock() {
                for window in windows_guard.iter() {
                    result.insert(window.limit_type.clone(), window.available_permits());
                }
            }

            if let Some(max_burst) = limiter.max_burst_limit {
                let current = limiter.current_burst_count.load(Ordering::Acquire);
                result.insert("burst".to_string(), max_burst.saturating_sub(current));
            }

            Ok(result)
        })
    }

    fn wait_time<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let limiter = Arc::new(self.clone());
        future_into_py(py, async move {
            Ok(limiter.compute_wait_duration().map(|d| d.as_secs_f64()))
        })
    }

    fn __aenter__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.interrupted.store(false, Ordering::SeqCst);
        
        let limiter = Arc::new(self.clone());
        future_into_py(py, async move {
            let _ = (
                if limiter.blocking_mode {
                    limiter.acquire_with_backoff_async().await
                } else {
                    limiter.try_acquire_limits_async().await
                }
            )?.ok_or_else(|| PyValueError::new_err("Failed to acquire permit"))?;

            Ok(())
        })
    }

    fn __aexit__<'p>(
        &self,
        py: Python<'p>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>
    ) -> PyResult<Bound<'p, PyAny>> {
        let limiter = Arc::new(self.clone());
        future_into_py(py, async move {
            if limiter.max_burst_limit.is_some() {
                limiter.current_burst_count.fetch_sub(1, Ordering::AcqRel);
            }
            Ok(false)
        })
    }
}

impl Clone for AsyncRateLimiter {
    fn clone(&self) -> Self {
        Self {
            windows: Arc::clone(&self.windows),
            blocking_mode: self.blocking_mode,
            interrupted: Arc::clone(&self.interrupted),
            max_burst_limit: self.max_burst_limit,
            current_burst_count: Arc::clone(&self.current_burst_count)
        }
    }
}

#[pymodule]
fn flowguard(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RateLimiter>()?;
    m.add_class::<AsyncRateLimiter>()?;
    Ok(())
}
