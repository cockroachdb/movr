#!/usr/bin/python

import argparse
from movr import MovR
import random, math
import sys, os
import time
import threading
from threading import Thread
import logging
import signal

RUNNING_THREADS = []
TERMINATE_GRACEFULLY = False


def signal_handler(sig, frame):
    global TERMINATE_GRACEFULLY
    grace_period = 15
    logging.info('Waiting at most %d seconds for threads to shutdown...', grace_period)
    TERMINATE_GRACEFULLY = True

    start = time.time()
    while threading.active_count() > 1:
        if (time.time() - start) > grace_period:
            logging.info("grace period has passed. killing threads.")
            os._exit(1)
        else:
            time.sleep(.1)

    logging.info("shutting down gracefully.")
    sys.exit(0)



logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

DEFAULT_PARTITION_MAP = {
    "us_east": ["new york", "boston", "washington dc"],
    "us_west": ["san francisco", "seattle", "los angeles"],
    "eu_west": ["amsterdam", "paris", "rome"]
}

# Create a connection to the movr database and populate a set of cities with rides, vehicles, and users.
def load_movr_data(conn_string, num_users, num_vehicles, num_rides, cities, echo_sql = False):
    with MovR(conn_string, echo=echo_sql) as movr:
        for city in cities:
            if TERMINATE_GRACEFULLY:
                logging.debug("terminating")
                return
            logging.info("Generating data for %s...", city)

            movr.add_users(num_users, city)
            movr.add_vehicles(num_vehicles, city)
            movr.add_rides(num_rides, city)

            logging.info("populated %s in %f seconds",
                  city, time.time() - start_time)

    return

# Generates load evenly distributed among the provided cities
def simulate_movr_load(conn_string, cities, movr_objects, active_rides, read_percentage, echo_sql = False):

    with MovR(conn_string, echo=echo_sql) as movr:
        num_retries = 0
        exception_message = ""
        while True and num_retries < 5:
            try:
                if TERMINATE_GRACEFULLY:
                    logging.debug("Terminating thread.")
                    return

                active_city = random.choice(cities)

                if random.random() < read_percentage:
                    movr.get_vehicles(active_city,25) #simulate user loading screen
                elif random.random() < .1:
                    movr_objects[active_city]["users"].append(movr.add_user(active_city)) #simulate new signup
                elif random.random() < .1:
                    movr_objects[active_city]["vehicles"].append(
                        movr.add_vehicle(active_city, random.choice(movr_objects[active_city]["users"])['id'])) #add vehicles
                elif random.random() < .5:
                    ride = movr.start_ride(active_city, random.choice(movr_objects[active_city]["users"])['id'],
                                           random.choice(movr_objects[active_city]["vehicles"])['id'])
                    active_rides.append(ride)
                else:
                    if len(active_rides):
                        ride = active_rides.pop()
                        movr.end_ride(ride['city'], ride['id'])
                num_retries = 0
            except Exception as e: #@todo: catch the right exception
                num_retries += 1
                exception_message = str(e)
                logging.debug("Retry attempt %d, last attempt failed with %s", num_retries, exception_message)

        logging.error("Too many errors. Killing thread after exception: %s", exception_message)




def extract_partition_pairs_from_cli(pair_list):
    if pair_list is None:
        return DEFAULT_PARTITION_MAP

    partition_pairs = {}

    for partition_pair in pair_list:
        pair = partition_pair.split(":")
        if len(pair) < 1:
            pair = ["default"].append(pair[0])
        else:
            pair = [pair[0], ":".join(pair[1:])]  # if there are many semicolons convert this to only two items


        if pair[0] in partition_pairs:
            partition_pairs[pair[0]].append(pair[1])
        else:
            partition_pairs[pair[0]] = [pair[1]]

    return partition_pairs

def setup_parser():
    parser = argparse.ArgumentParser(description='CLI for MovR.')
    subparsers = parser.add_subparsers(dest='subparser_name')

    ###########
    # GENERAL COMMANDS
    ##########
    parser.add_argument('--num-threads', dest='num_threads', type=int, default=5,
                            help='The number threads to use for MovR (default =5)')
    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")

    parser.add_argument('--echo-sql', dest='echo_sql', action='store_true',
                        help='set this if you want to print all executed SQL statements')

    ###############
    # LOAD COMMANDS
    ###############
    load_parser = subparsers.add_parser('load', help="load movr data into a database")
    load_parser.add_argument('--num-users', dest='num_users', type=int, default=50,
                             help='The number of random users to add to the dataset')
    load_parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10,
                             help='The number of random vehicles to add to the dataset')
    load_parser.add_argument('--num-rides', dest='num_rides', type=int, default=500,
                             help='The number of random rides to add to the dataset')
    load_parser.add_argument('--partition-by', dest='partition_pair', action='append',
                             help='Pairs in the form <partition>:<city_id> that will be used to enable geo-partitioning. Example: us_west:seattle. Use this flag multiple times to add multiple cities.')
    load_parser.add_argument('--enable-geo-partitioning', dest='enable_geo_partitioning', action='store_true',
                             help='Set this if your cluster has an enterprise license (https://cockroa.ch/2BoAlgB) and you want to use geo-partitioning functionality (https://cockroa.ch/2wd96zF)')
    load_parser.add_argument('--skip-init', dest='skip_reload_tables', action='store_true',
                             help='Keep existing tables and data when loading Movr tables')

    ###############
    # RUN COMMANDS
    ###############
    run_parser = subparsers.add_parser('run', help="generate fake traffic for the movr database")

    run_parser.add_argument('--city', dest='city', action='append',
                            help='The names of the cities to use when generating load. Use this flag multiple times to add multiple cities.')
    run_parser.add_argument('--read-only-percentage', dest='read_percentage', type=float,
                            help='Value between 0-1 indicating how many simulated read-only home screen loads to perform as a percentage of overall activities',
                            default=.9)

    return parser

if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler) #support ctrl + c to exit multithreaded operation

    args = setup_parser().parse_args()

    if args.conn_string.find("/movr") < 0:
        logging.error("The connection string needs to point to a database named 'movr'")
        sys.exit(1)

    if args.num_threads <= 0:
        logging.error("Number of threads must be greater than 0.")
        sys.exit(1)


    logging.info("Connected to movr database @ %s" % args.conn_string)

    #format connection string to work with our cockroachdb driver.
    conn_string = args.conn_string.replace("postgres://", "cockroachdb://")
    conn_string = conn_string.replace("postgresql://", "cockroachdb://")

    # population partitions
    partition_city_map = extract_partition_pairs_from_cli(args.partition_pair if args.subparser_name=='load' else None)

    all_cities = []
    for partition in partition_city_map:
        all_cities += partition_city_map[partition]

    if args.subparser_name=='load':

        if args.num_users <= 0 or args.num_rides <= 0 or args.num_vehicles <= 0:
            logging.error("The number of objects to generate must be > 0")
            sys.exit(1)

        start_time = time.time()
        with MovR(conn_string, init_tables=(not args.skip_reload_tables), echo=args.echo_sql) as movr:
            if args.enable_geo_partitioning:
                movr.add_geo_partitioning(partition_city_map)

            logging.info("loading cities %s", all_cities)
            logging.info("loading movr data with ~%d users, ~%d vehicles, and ~%d rides",
                  args.num_users, args.num_vehicles, args.num_rides)


        usable_threads = min(args.num_threads, len(all_cities)) #don't create more than 1 thread per city

        cities_per_thread = int(math.ceil((float(len(all_cities)) / usable_threads)))
        num_users_per_city = int(math.ceil((float(args.num_users) / len(all_cities))))
        num_rides_per_city = int(math.ceil((float(args.num_rides) / len(all_cities))))
        num_vehicles_per_city = int(math.ceil((float(args.num_vehicles) / len(all_cities))))

        cities_to_load = all_cities

        RUNNING_THREADS = []

        for i in range(usable_threads):
            if len(cities_to_load) > 0:
                t = Thread(target=load_movr_data, args=(conn_string, num_users_per_city, num_vehicles_per_city,
                                                        num_rides_per_city, cities_to_load[:cities_per_thread], args.echo_sql))
                cities_to_load = cities_to_load[cities_per_thread:]
                t.start()
                RUNNING_THREADS.append(t)

        while threading.active_count() > 1: #keep main thread alive so we can catch ctrl + c
            time.sleep(0.1)


        duration = time.time() - start_time

        logging.info("populated %s cities in %f seconds", len(all_cities), duration)
        logging.info("- %f users/second", float(num_users_per_city * len(all_cities))/duration)
        logging.info("- %f rides/second", float(num_rides_per_city * len(all_cities))/duration)
        logging.info("- %f vehicles/second", float(num_vehicles_per_city * len(all_cities))/duration)


    else:

        if args.read_percentage < 0 or args.read_percentage > 1:
            logging.error("read percentage must be between 0 and 1")
            sys.exit(1)

        cities = all_cities if args.city is None else args.city
        logging.info("simulating movr load for cities %s", cities)

        movr_objects = {}

        with MovR(conn_string, echo=args.echo_sql) as movr:
            for city in cities:
                movr_objects[city] = {"users": movr.get_users(city), "vehicles": movr.get_vehicles(city)}
                if len(list(movr_objects[city]["vehicles"])) == 0 or len(list(movr_objects[city]["users"])) == 0:
                    logging.error("must have users and vehicles for '%s' in the movr database to generte load. try running with the 'load' command.", city)
                    sys.exit(1)

            active_rides = movr.get_active_rides()

        RUNNING_THREADS = []
        for i in range(args.num_threads):
            t = Thread(target=simulate_movr_load, args=(conn_string, cities, movr_objects,
                                                        active_rides, args.read_percentage, args.echo_sql ))
            t.start()
            RUNNING_THREADS.append(t)

        while True: #keep main thread alive to catch exit signals
            time.sleep(0.5)




