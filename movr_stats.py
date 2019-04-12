import numpy
from tabulate import tabulate
import time


class MovRStats:

    def __init__(self):
        self.window_stats = {}
        self.window_start_time = time.time()
        self.cumulative_counts = {}
        self.instantiation_time = time.time()

    # reset stats while keeping cumulative counts
    def new_window(self):
        self.window_start_time = time.time()
        self.window_stats = {}

    # add one latency measurement in seconds
    def add_latency_measurement(self, command, measurement):
        self.window_stats.setdefault(command,[]).append(measurement)
        self.cumulative_counts.setdefault(command,0)
        self.cumulative_counts[command]+=1

    # print the current stats this instance has collected
    def print_stats(self):
        def get_percentile_measurement(command, percentile):
            return numpy.percentile(self.window_stats.setdefault(command, [0]), percentile)

        def get_stats_row(command):
            if command in self.window_stats:
                elapsed = time.time() - self.instantiation_time
                return [command, round(elapsed, 0),  self.cumulative_counts[command], len(self.window_stats[command]),
                        len(self.window_stats[command]) / elapsed,
                        round(float(get_percentile_measurement(command, 50)) * 1000, 2),
                        round(float(get_percentile_measurement(command, 95)) * 1000, 2),
                        round(float(get_percentile_measurement(command, 99)) * 1000, 2),
                        round(float(get_percentile_measurement(command, 100)) * 1000, 2)]
            else:
                return []

        header = ["action", "time(total)",  "ops(total)", "ops", "ops/second", "p50(ms)", "p95(ms)", "p99(ms)", "max(ms)"]
        rows = []

        for command in sorted(list(self.window_stats)):
            rows.append(get_stats_row(command))

        print(tabulate(rows, header), "\n")
