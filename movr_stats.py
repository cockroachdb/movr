import numpy
from tabulate import tabulate
import time

#@todo: is this threadsafe? turn this into a class
class MovRStats:

    def __init__(self):
        self.window_stats = {}
        self.total_measurements = 0
        self.instantiation_time = time.time()
        self.window_start_time = time.time()

    def new_window(self):
        self.window_start_time = time.time()
        self.window_stats = {}


    def get_stats_row(self, command):
        if command in self.window_stats:
            elapsed = time.time() - self.instantiation_time
            return [command, round(elapsed,0), len(self.window_stats[command]), len(self.window_stats[command])/elapsed,
                    round(float(self.get_percentile_measurement(command, 50))*1000,2),
                          round(float(self.get_percentile_measurement(command, 95))*1000,2),
                          round(float(self.get_percentile_measurement(command, 99))*1000,2),
                                round(float(self.get_percentile_measurement(command, 100))*1000,2)]
        else:
            return []


    def add_latency_measurement(self, command, measurement):
        self.window_stats.setdefault(command,[]).append(measurement)
        self.total_measurements+=1

    def get_percentile_measurement(self,command, percentile):
        return numpy.percentile(self.window_stats.setdefault(command,[0]),percentile)

    def print_stats(self):
        header = ["action", "elapsed secs", "num_operations", "ops/second", "p50(ms)", "p95(ms)", "p99(ms)", "max(ms)"]
        rows = []

        for command in sorted(list(self.window_stats)):
            rows.append(self.get_stats_row(command))

        print(tabulate(rows, header))
        print("")
