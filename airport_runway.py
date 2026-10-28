"""Threaded airport runway producer/consumer simulation for the live dashboard."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import threading
import time
from typing import Any


PRESETS = {
    "normal": {"queue_slots": 3, "producers": 2, "flights_per_producer": 4, "runways": 2, "producer_delay": .25, "operation_delay": 1.2},
    "full": {"queue_slots": 2, "producers": 3, "flights_per_producer": 6, "runways": 1, "producer_delay": .08, "operation_delay": 1.5},
    "empty": {"queue_slots": 5, "producers": 1, "flights_per_producer": 2, "runways": 2, "producer_delay": 1.0, "operation_delay": .8},
    "contention": {"queue_slots": 2, "producers": 5, "flights_per_producer": 8, "runways": 1, "producer_delay": .04, "operation_delay": 1.3},
    "multiple": {"queue_slots": 5, "producers": 3, "flights_per_producer": 6, "runways": 3, "producer_delay": .08, "operation_delay": 1.0},
}


@dataclass
class Flight:
    flight_id: str
    operation: str
    producer_id: int
    status: str = "Generated"
    queued_at: float | None = None


class RunwaySimulation:
    """Bounded FIFO synchronized by a condition; runway assignment has its own lock."""
    def __init__(self, config: dict[str, Any], speed: float = 1.0):
        self.config = config
        self.speed = speed
        self.capacity = config["queue_slots"]
        self.queue: list[Flight | None] = [None] * self.capacity
        self.in_index = self.out_index = self.occupancy = 0
        self.generated = self.processed = 0
        self.events: list[dict[str, str]] = []
        self.active: dict[int, dict[str, Any]] = {}
        self.pending: dict[int, Flight] = {}
        self.producer_status = {i: "Starting" for i in range(1, config["producers"] + 1)}
        self.consumer_status = {i: "Starting" for i in range(1, config["runways"] + 1)}
        self.finished_producers = 0
        self.done = self.paused = self.stopping = False
        self.started_at = time.time()
        self.cv = threading.Condition(threading.RLock())
        self.runway_lock = threading.Lock()
        self.threads: list[threading.Thread] = []
        self._event(f"Simulation started: {self.total} requests expected.", "system")
        self._event(f"wait(empty={self.capacity}); wait(full=0); mutex=1", "semaphore")
        self._event(f"Buffer status: EMPTY (0/{self.capacity})", "buffer")

    @property
    def total(self) -> int:
        return self.config["producers"] * self.config["flights_per_producer"]

    def _event(self, message: str, kind: str = "info") -> None:
        self.events.append({"time": datetime.now().astimezone().strftime("%H:%M:%S"), "message": message, "kind": kind})

    def start(self) -> None:
        for runway in range(1, self.config["runways"] + 1):
            thread = threading.Thread(target=self._consumer, args=(runway,), name=f"Runway-{runway}", daemon=True)
            self.threads.append(thread)
            thread.start()
        for producer in range(1, self.config["producers"] + 1):
            thread = threading.Thread(target=self._producer, args=(producer,), name=f"Producer-{producer}", daemon=True)
            self.threads.append(thread)
            thread.start()
        threading.Thread(target=self._monitor, name="Simulation-monitor", daemon=True).start()

    def _wait_if_paused(self) -> bool:
        with self.cv:
            while self.paused and not self.stopping:
                self.cv.wait()
            return not self.stopping

    def _delay(self, seconds: float, runway: int | None = None) -> bool:
        """Pause-aware delay that publishes accurate phase progress."""
        remaining = seconds / self.speed
        while remaining > 0:
            with self.cv:
                while self.paused and not self.stopping:
                    self.cv.wait()
                if self.stopping:
                    return False
                step = min(.05, remaining)
                if runway is not None and runway in self.active:
                    self.active[runway]["progress"] = 1 - remaining / (seconds / self.speed)
                before = time.monotonic()
                self.cv.wait(timeout=step)
                remaining -= min(step, time.monotonic() - before)
        if runway is not None:
            with self.cv:
                if runway in self.active:
                    self.active[runway]["progress"] = 1.0
        return not self.stopping

    def pause(self) -> None:
        with self.cv:
            self.paused = True
            self._event("Simulation paused.", "system")
            self.cv.notify_all()

    def resume(self) -> None:
        with self.cv:
            self.paused = False
            self._event("Simulation resumed.", "system")
            self.cv.notify_all()

    def stop(self) -> None:
        with self.cv:
            self.stopping = True
            self.paused = False
            self.cv.notify_all()
        for thread in self.threads:
            thread.join(timeout=.2)

    def _producer(self, producer_id: int) -> None:
        with self.cv:
            self.producer_status[producer_id] = "Running"
        for sequence in range(1, self.config["flights_per_producer"] + 1):
            if not self._wait_if_paused():
                return
            flight = Flight(f"P{producer_id}-F{sequence:03d}", "LANDING" if sequence % 2 else "TAKEOFF", producer_id)
            with self.cv:
                if self.stopping:
                    return
                self.generated += 1
                self.pending[producer_id] = flight
                self._event(f"Producer {producer_id} generated {flight.flight_id} ({flight.operation}).", "generated")
                if self.occupancy == self.capacity:
                    self.producer_status[producer_id] = "WAITING: BUFFER FULL"
                    self._event(f"PRODUCER WAITING: BUFFER FULL — Producer {producer_id} calls wait(empty).", "wait")
                while (self.occupancy == self.capacity or self.paused) and not self.stopping:
                    self.cv.wait()
                if self.stopping:
                    return
                self._event(f"Producer {producer_id}: wait(empty) succeeded; wait(mutex).", "semaphore")
                slot = self.in_index
                flight.status = "Queued"
                flight.queued_at = time.time()
                self.queue[slot] = flight
                self.pending.pop(producer_id, None)
                self.occupancy += 1
                self.in_index = (self.in_index + 1) % self.capacity
                self.producer_status[producer_id] = f"Queued {flight.flight_id}"
                self._event(f"{flight.flight_id} entered circular-buffer slot {slot}.", "insert")
                self._event(f"signal(mutex); signal(full); empty={self.capacity-self.occupancy}; full={self.occupancy}.", "semaphore")
                self._buffer_event()
                self.cv.notify_all()
            if not self._delay(self.config["producer_delay"]):
                return
        with self.cv:
            self.producer_status[producer_id] = "Complete"
            self.finished_producers += 1
            self._event(f"Producer {producer_id} completed.", "success")
            self.cv.notify_all()

    def _consumer(self, runway: int) -> None:
        with self.cv:
            self.consumer_status[runway] = "Ready"
        while True:
            with self.cv:
                if self.stopping:
                    return
                if self.occupancy == 0 and self.finished_producers < self.config["producers"]:
                    self.consumer_status[runway] = "WAITING: BUFFER EMPTY"
                    self._event(f"CONSUMER WAITING: BUFFER EMPTY — Runway {runway} calls wait(full).", "wait")
                while ((self.occupancy == 0 and self.finished_producers < self.config["producers"]) or self.paused) and not self.stopping:
                    self.cv.wait()
                if self.stopping:
                    return
                if self.occupancy == 0 and self.finished_producers == self.config["producers"]:
                    self.consumer_status[runway] = "Complete"
                    self.cv.notify_all()
                    return
                self._event(f"Runway {runway}: wait(full) succeeded; wait(mutex).", "semaphore")
                slot = self.out_index
                flight = self.queue[slot]
                assert flight is not None
                self.queue[slot] = None
                self.occupancy -= 1
                self.out_index = (self.out_index + 1) % self.capacity
                flight.status = "On taxiway"
                self.consumer_status[runway] = f"Taxiing {flight.flight_id}"
                self._event(f"{flight.flight_id} removed from slot {slot} in FIFO order.", "remove")
                self._event(f"signal(mutex); signal(empty); empty={self.capacity-self.occupancy}; full={self.occupancy}.", "semaphore")
                self._buffer_event()
                self.cv.notify_all()

            # Runway claiming is protected independently from the bounded queue.
            with self.runway_lock:
                with self.cv:
                    if self.stopping:
                        return
                    self.active[runway] = {"flight_id": flight.flight_id, "operation": flight.operation, "phase": "taxi-in", "progress": 0.0, "phase_duration": .45 / self.speed * 1000, "producer_id": flight.producer_id, "queue_slot": slot}
                    self.consumer_status[runway] = f"Taxiing {flight.flight_id}"
                    self._event(f"Runway allocator lock: assigned {flight.flight_id} to runway {runway}.", "assign")
                    self.cv.notify_all()

            phases = [("taxi-in", .45), ("landing" if flight.operation == "LANDING" else "takeoff", self.config["operation_delay"]), ("taxi-out", .45)]
            stopped = False
            for phase, duration in phases:
                with self.cv:
                    if runway in self.active:
                        self.active[runway]["phase"] = phase
                        self.active[runway]["progress"] = 0.0
                        self.active[runway]["phase_duration"] = duration / self.speed * 1000
                        self.consumer_status[runway] = f"{phase.title()} {flight.flight_id}"
                        self._event(f"{flight.flight_id}: {phase.replace('-', ' ')} started on runway {runway}.", "phase")
                if not self._delay(duration, runway):
                    stopped = True
                    break
            if stopped:
                return
            with self.runway_lock:
                with self.cv:
                    self.active.pop(runway, None)
                    self.processed += 1
                    flight.status = "Processed"
                    self.consumer_status[runway] = "Ready"
                    self._event(f"Runway {runway} released; {flight.flight_id} processed exactly once.", "release")
                    self.cv.notify_all()

    def _buffer_event(self) -> None:
        state = "EMPTY" if self.occupancy == 0 else "FULL" if self.occupancy == self.capacity else "PARTIAL"
        self._event(f"Buffer {state}: {self.occupancy}/{self.capacity}; IN={self.in_index}; OUT={self.out_index}.", "buffer")

    def _monitor(self) -> None:
        for thread in self.threads:
            thread.join()
        with self.cv:
            self.done = True
            if self.stopping:
                self._event("Simulation reset; workers stopped cleanly.", "system")
            else:
                self._event(f"Simulation completed: {self.processed}/{self.total} requests processed.", "success")
            self.cv.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self.cv:
            return {
                "config": self.config, "total": self.total, "generated": self.generated,
                "processed": self.processed, "occupancy": self.occupancy, "capacity": self.capacity,
                "slots": [asdict(f) if f else None for f in self.queue],
                "pending": [asdict(f) for f in self.pending.values()],
                "in_index": self.in_index, "out_index": self.out_index,
                "runways": [{"id": i, **self.active.get(i, {}), "status": self.consumer_status.get(i, "Ready")} for i in range(1, self.config["runways"] + 1)],
                "producers": [{"id": i, "status": s} for i, s in self.producer_status.items()],
                "consumers": [{"id": i, "status": s} for i, s in self.consumer_status.items()],
                "events": list(self.events[-250:]), "revision": len(self.events), "done": self.done,
                "paused": self.paused, "stopping": self.stopping, "run_id": self.started_at,
                "elapsed": round(time.time() - self.started_at, 1), "speed": self.speed,
                "buffer_status": "EMPTY" if self.occupancy == 0 else ("FULL" if self.occupancy == self.capacity else "PARTIALLY FULL"),
                "semaphores": {"empty": self.capacity - self.occupancy, "full": self.occupancy, "mutex": 1},
                "runway_lock": "independent runway allocator lock",
                "final_summary": ({"generated": self.generated, "processed": self.processed,
                    "lost_or_duplicated": self.generated - self.processed, "final_occupancy": self.occupancy,
                    "all_runways_released": not self.active,
                    "result": "SUCCESS" if self.generated == self.total and self.processed == self.total and self.occupancy == 0 and not self.active else "RESET" if self.stopping else "CHECK RESULTS"} if self.done else None),
            }


def validate_config(raw: dict[str, Any]) -> dict[str, Any]:
    limits = {"queue_slots": (1, 30), "producers": (1, 20), "flights_per_producer": (1, 100), "runways": (1, 10)}
    result: dict[str, Any] = {}
    for key, (low, high) in limits.items():
        try:
            value = int(raw.get(key))
        except (TypeError, ValueError):
            raise ValueError(f"{key.replace('_', ' ').title()} must be a whole number.")
        if not low <= value <= high:
            raise ValueError(f"{key.replace('_', ' ').title()} must be between {low} and {high}.")
        result[key] = value
    if result["producers"] * result["flights_per_producer"] > 1000:
        raise ValueError("A simulation may contain at most 1,000 requests.")
    for key, default in (("producer_delay", .25), ("operation_delay", 1.2)):
        try:
            value = float(raw.get(key, default))
        except (TypeError, ValueError):
            value = default
        result[key] = min(5.0, max(.01, value))
    return result
