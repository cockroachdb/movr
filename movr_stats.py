import numpy
from tabulate import tabulate
import time
from threading import Lock

class MovRStats:



    def __init__(self):
        self.cumulative_counts = {}
        self.instantiation_time = time.time()
        self.mutex = Lock()
        self.new_window()

    # reset stats while keeping cumulative counts
    def new_window(self):
        self.mutex.acquire()
        try:
            self.window_start_time = time.time()
            self.window_stats = {}
        finally:
            self.mutex.release()

    # add one latency measurement in seconds
    def add_latency_measurement(self, action, measurement):
        self.mutex.acquire()
        try:
            self.window_stats.setdefault(action,[]).append(measurement)
            self.cumulative_counts.setdefault(action,0)
            self.cumulative_counts[action]+=1
        finally:
            self.mutex.release()

    # print the current stats this instance has collected.
    # If action_list is empty, it will only prevent rows it has captured this period, otherwise it will print a row for each action.
    def print_stats(self, action_list = []):
        def get_percentile_measurement(action, percentile):
            return numpy.percentile(self.window_stats.setdefault(action, [0]), percentile)

        def get_stats_row(action):
            elapsed = time.time() - self.instantiation_time

            if action in self.window_stats:
                return [action, round(elapsed, 0),  self.cumulative_counts[action], len(self.window_stats[action]),
                        len(self.window_stats[action]) / elapsed,
                        round(float(get_percentile_measurement(action, 50)) * 1000, 2),
                        round(float(get_percentile_measurement(action, 90)) * 1000, 2),
                        round(float(get_percentile_measurement(action, 95)) * 1000, 2),
                        round(float(get_percentile_measurement(action, 100)) * 1000, 2)]
            else:
                return [action, round(elapsed, 0), self.cumulative_counts.get(action, 0), 0, 0, 0, 0, 0, 0]

        header = ["transaction name", "time(total)",  "ops(total)", "ops", "ops/second", "p50(ms)", "p90(ms)", "p95(ms)", "max(ms)"]
        rows = []

        self.mutex.acquire()
        try:
            if len(action_list):
                for action in sorted(action_list):
                    rows.append(get_stats_row(action))
            else:
                for action in sorted(list(self.window_stats)):
                    rows.append(get_stats_row(action))
            print(tabulate(rows, header), "\n")
        finally:
            self.mutex.release()


