# Flowguard
Flowguard is a rate limiting library for Python. It provides both synchronous (RateLimiter) 
and asynchronous (AsyncRateLimiter) classes to manage request rates with flexible time windows (seconds, minutes, 
hours, days) and optional burst limits.

## Features
* Synchronous and Asynchronous Support: Use RateLimiter for blocking operations or AsyncRateLimiter for async/await workflows.
* Customizable Time Windows: Set rate limits per second, minute, hour, or day, with configurable window durations.
* Burst Limiting: Optional maximum burst capacity to control maximum concurrent requests.
* Thread-Safe and Interruptible: Built with atomic operations and mutexes for safe concurrent use.
* Context Manager Support: For automatic resource management.

## Installation
Flowguard is available as a Python package. Install it using pip:

```shell
pip install flowguard
```
Ensure you have a compatible Python version (3.8 or higher) installed.

## Parameters
* sec, min, hour, day: Rate limits for respective time units.
* sec_window, min_window, hour_window, day_window: Custom window durations (in seconds) for respective units. Default is 1.
* blocking: Default True (For using as a context manager). if False, returns immediately if no permit is available.
* max_burst: Optional maximum number of concurrent permits allowed.

## Usage
### Synchronous Rate Limiter Example :: 

```python

import logging
import threading
from time import sleep
from flowguard import RateLimiter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def non_blocking_example(i, limiter):
    permit = limiter.acquire()
    if permit:
        logger.info(f"Permit acquired for {i}.")
        # Simulate work ::
        sleep(1)
        limiter.release()
    else:
        logger.info("No permit is available.")
        # Here we can do other task when no immediate permit is available

def context_manager_example(i, limiter):
    with limiter:
        logger.info(f"Permit acquired for {i}.")
        # Simulate work ::
        sleep(1)

def explicit_example(i, limiter):
    # This call will block until it gets a permit
    permit = limiter.acquire()
    if permit:
        logger.info(f"Permit acquired for {i}")
        # Simulate work
        sleep(1)
        limiter.release()

def main():
    # This will create a ratelimiter instance with 3 req/2 sec and 15 req/min with a max allowed concurrent req = 2
    blocking_limiter = RateLimiter(sec= 3, min= 15, sec_window=2, max_burst= 2)
    non_blocking_limiter = RateLimiter(sec= 3, min= 15, sec_window=2, max_burst= 2, blocking=False)
    
    print("Blocking Limiter".center(60, "="), "\n")    
    print("Using Context Manager".center(60, "="), "\n")

    threads = []

    for i in range(30):
        t = threading.Thread(target=context_manager_example, args=(i, blocking_limiter), daemon= True)
        threads.append(t)
        t.start()
    
    # Waiting for the work to finish oktherwise both function output will be mixed
    for t in threads:
        t.join()

    blocking_limiter = RateLimiter(sec= 3, min= 15, sec_window=2, max_burst= 2)
    
    print("\n", "Explicit acquire/ release".center(60, "="), "\n")

    threads = []

    for i in range(30):
        t = threading.Thread(target=explicit_example, args=(i, blocking_limiter), daemon= True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n", "Non-Blocking Mode".center(60, "="), "\n")

    threads = []

    for i in range(30):
        t = threading.Thread(target=non_blocking_example, args=(i, non_blocking_limiter), daemon= True)
        threads.append(t)
        t.start()
        # Without the sleep it will give permit = max_burst count and all other threads will failed to acquire any permit
        sleep(.1)
    
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()


```
### Asynchronous Rate Limiter Example :: 

```python
import asyncio
import logging
from flowguard import AsyncRateLimiter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


async def explicit_example(i, limiter):
    await limiter.acquire()
    logger.info(f"Permit acquired for {i}")
    # Simulate work 
    await asyncio.sleep(1)
    await limiter.release()

async def context_manager_example(i, limiter):
    async with limiter:
        logger.info(f"Permit acquired for {i}")

async def main():
    limiter = AsyncRateLimiter(sec=10)

    print("\n", "Explicit Example".center(60, "="), "\n")
    explicit_tasks = [
        explicit_example(i, limiter) 
        for i in range(15)  
    ]

    await asyncio.gather(*explicit_tasks)

    print("\n", "Context Manager Example".center(60, "="), "\n")
    burst_limiter = AsyncRateLimiter(sec=5, max_burst=3, sec_window= 2)
    
    burst_tasks = [
        context_manager_example(f"burst-{i}", burst_limiter)
        for i in range(15)  
    ]
    
    await asyncio.gather(*burst_tasks)
    

if __name__ == "__main__":
    asyncio.run(main())

```

## License
Flowguard is licensed under the MIT License. See the LICENSE file for details.

## Contributing
Contributions are welcome! Feel free to submit pull requests, report issues, or suggest improvements.
