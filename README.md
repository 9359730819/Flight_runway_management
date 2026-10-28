# Airport Runway Management Simulation

A beginner-friendly Operating Systems producer–consumer project with a live, top-down 2D airport dashboard. Python worker threads generate landing and takeoff requests, pass them through a bounded circular FIFO, and operate independently allocated runways. The dashboard polls the real Python state; aircraft, counters, semaphore-equivalent values, and event messages are not fabricated by the browser.

## Requirements

- Windows 10 or 11
- Python 3.10 or newer
- VS Code (optional)
- No third-party Python packages and no C compiler are needed for the web dashboard.

## Run from VS Code on Windows PowerShell

1. Open this project folder in VS Code.
2. Choose **Terminal → New Terminal** and confirm the terminal is PowerShell in this folder.
3. Check Python is installed:

   ```powershell
   py --version
   ```

   If `py` is unavailable, try `python --version`. Install Python 3.10+ from python.org if neither command works, and enable the Python launcher or PATH option in its installer.
4. Start the dashboard:

   ```powershell
   py dashboard_server.py
   ```

   Or, when using the `python` command:

   ```powershell
   python dashboard_server.py
   ```

5. Open <http://127.0.0.1:8000/> in a browser. Keep the PowerShell terminal open while using the simulation. Press **Ctrl+C** in that terminal to stop the server.

There are no `pip install` commands. The server uses Python's standard library and opens the existing HTML dashboard.

## Dashboard controls

- **Start** begins the selected scenario. You can change queue capacity, producer count, flights per producer, and runway count.
- **Pause / Resume** suspends and continues producer and runway workers.
- **Reset** stops the current workers and clears the dashboard state.
- **Animation speed** changes how long producer and runway phases take.
- **Presets**: Normal Operation, Buffer Full, Buffer Empty, and High Contention.

The airport view shows a terminal/apron, taxiways, runway markings, queued aircraft, and aircraft moving through taxi-in, landing or takeoff, and taxi-out phases. Flight IDs match Python request IDs. The buffer panel shows each FIFO slot and the IN/OUT indices. `empty` and `full` show the actual available and occupied slot counts; `mutex` is available when a snapshot is taken outside the protected queue operation. The event log records actual `wait()` and `signal()` conditions. Producer-full and consumer-empty messages are emitted only when those workers encounter those states.

Runway assignment uses a dedicated allocator lock, separate from the queue condition and its empty/full synchronization. A runway worker processes only one request at a time. Final lost/duplicated count is generated minus processed, and normal completion is successful only when every generated request is processed, the queue is empty, and all runways are released.

## Scenarios

| Scenario | Queue | Producers | Flights each | Runways | Total requests | Behavior |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Normal Operation | 3 | 2 | 4 | 2 | 8 | Balanced production and runway service |
| Buffer Full | 2 | 3 | 6 | 1 | 18 | Producer waits when both queue slots are occupied |
| Buffer Empty | 5 | 1 | 2 | 2 | 2 | Consumer waits before the next request arrives |
| High Contention | 2 | 5 | 8 | 1 | 40 | Multiple producers contend for a small queue |
| Multiple Runways | 5 | 3 | 6 | 3 | 18 | Shows parallel runway operation |

Allowed ranges: queue capacity 1–30, producers 1–20, flights per producer 1–100, runways 1–10, and at most 1,000 total requests.

## Project files

- `airport_runway.py` — bounded FIFO, Python producer/consumer threads, pause/reset, separate runway lock, snapshots, and input validation.
- `dashboard_server.py` — standard-library HTTP server and JSON endpoints.
- `dashboard.html` — interactive airport map, controls, live statistics, buffer, semaphores, worker states, and event log.
- `README.md` — setup and project guide.

The requested `airport_runway.c` file was not present in the project folder when this dashboard was updated. The dashboard preserves and uses the existing Python simulation; it does not require a C source file.
