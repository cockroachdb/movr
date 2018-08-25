#!/usr/bin/python

#@todo: keyboard interrupt needs to work when using threads

import argparse
from movr import MovR
import random, math
import sys, os
import time
from threading import Thread
import logging
import signal
import psycopg2

#@todo: close connections to the database. open connections should be restores to zero.
def signal_handler(sig, frame):
    print('Exiting...')
    os._exit(0)



logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

DEFAULT_PARTITION_MAP = {
    "us_east": ["new york", "boston", "washington dc"],
    "us_west": ["san francisco", "seattle", "los angeles"],
    "eu_west": ["amsterdam", "paris", "rome"]
}


#@todo: do this in parallel. argument shouldnt be load anymore, it should be, create partitions
def load_movr_data(conn_string, num_users, num_vehicles, num_rides, cities, echo_sql):
    movr = MovR(conn_string, echo=echo_sql)
    #@todo: rounding means the requests values don't equal actual. fix this.
    for city in cities:
        logging.info("populating %s..", city)
        # add users
        start_time = time.time()
        city_user_count = int(num_users / len(cities)) if int(num_users / len(cities)) > 0 else 1
        movr.add_users(city_user_count, city)


        # add vehicles
        city_vehicle_count = int(num_vehicles / len(cities)) if int(num_vehicles / len(cities)) > 0 else 1
        movr.add_vehicles(city_vehicle_count, city)

        # add rides
        city_ride_count = int(num_rides/len(cities)) if int(num_rides/len(cities)) > 0 else 1
        movr.add_rides(city_ride_count, city)
        logging.info("populated %s in %f seconds",
              city, time.time() - start_time)

    return


def simulate_movr_load(conn_string, cities, movr_objects, active_rides, read_percentage, echo_sql = False):
    movr = MovR(conn_string, echo=echo_sql)

    num_retries = 0
    exception_message = ""
    while True and num_retries < 5:
        try:
            active_city = random.choice(cities)
            #@todo: need to work out the probability distribution math here. Use https://docs.scipy.org/doc/numpy/reference/generated/numpy.random.choice.html#numpy.random.choice

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
        except psycopg2.InternalError as e:
            num_retries += 1
            exception_message = e.pgerror
            logging.warn("Retry attempt %d, last attempt failed with %s", num_retries, exception_message)

    logging.error("Too many errors. Killing thread after exception: ", exception_message)




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
    load_parser.add_argument('--init', dest='reload_tables', action='store_true',
                             help='Drop and reload MovR tables')

    ###############
    # RUN COMMANDS
    ###############
    run_parser = subparsers.add_parser('run', help="generate fake traffic for the movr database")

    run_parser.add_argument('--city', dest='city', action='append',
                            help='The names of the cities to use when generating load. Use this flag multiple times to add multiple cities.')
    run_parser.add_argument('--read-only-percentage', dest='read_percentage', type=float,
                            help='Value between 0-1 indicating how many simulated read-only home screen loads to perform as a percentage of overall activities',
                            default=.9)

    ###########
    # GENERAL COMMANDS
    ##########
    parser.add_argument('--num-threads', dest='num_threads', type=int, default=5,
                            help='The number threads to use for MovR (default =5)')
    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")

    parser.add_argument('--echo-sql', dest='echo_sql', action='store_true',
                        help='set this if you want to print all executed SQL statements')

    return parser

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    args = setup_parser().parse_args()

    if args.conn_string.find("/movr") < 0:
        logging.error("The connection string needs to point to a database named 'movr'")
        sys.exit(1)

    #@todo: check threads is positive

    logging.info("connected to movr database @ %s" % args.conn_string)

    conn_string = args.conn_string.replace("postgres://", "cockroachdb://")
    conn_string = conn_string.replace("postgresql://", "cockroachdb://")

    # population partitions
    partition_city_map = extract_partition_pairs_from_cli(args.partition_pair if args.subparser_name=='load' else None)

    enable_geo_partitioning = args.enable_geo_partitioning if args.subparser_name == 'load' else None
    reload_tables = args.reload_tables if args.subparser_name == 'load' else None

    all_cities = []
    for partition in partition_city_map:
        all_cities += partition_city_map[partition]


    if args.subparser_name == 'load' and (args.num_users <= 0 or args.num_rides <= 0 or args.num_vehicles <= 0):
        logging.error("The number of objects to generate must be > 0")
        sys.exit(1)

    if args.subparser_name=='load':

        start_time = time.time()
        movr = MovR(conn_string, init_tables=reload_tables, echo=args.echo_sql)

        if enable_geo_partitioning:
            logging.info("geo-partitioning tables")
            movr.add_geo_partitioning(partition_city_map)

        logging.info("loading cities %s", all_cities)
        logging.info("loading movr data with %d users, %d vehicles, and %d rides",
              args.num_users, args.num_vehicles, args.num_rides)

        threads = []
        #@todo: add cities in parallel
        cities_per_thread = int(math.ceil((float(len(all_cities)) / args.num_threads)))
        cities_to_load = all_cities

        for i in range(args.num_threads):
            if len(cities_to_load) > 0:
                t = Thread(target=load_movr_data, args=(conn_string, args.num_users, args.num_vehicles,
                                                        args.num_rides, cities_to_load[:cities_per_thread], args.echo_sql))
                cities_to_load = cities_to_load[cities_per_thread:]
                t.start()
                threads.append(t)

        #@todo: join threads and wait for them to finish
        for thread in threads:
            thread.join()

        duration = time.time() - start_time
        logging.info("populated %s cities in %f seconds", len(all_cities), duration)
        logging.info("- %f users/second", float(args.num_users)/duration)
        logging.info("- %f rides/second", float(args.num_vehicles)/duration)
        logging.info("- %f vehicles/second", float(args.num_rides)/duration)

        print 'finished loading!!'


    else:
        # @todo: give each thead its own connection

        if args.read_percentage < 0 or args.read_percentage > 1:
            logging.error("read percentage must be between 0 and 1")
            sys.exit(1)
        cities = all_cities if args.city is None else args.city

        movr = MovR(conn_string, echo=args.echo_sql)

        movr_objects = {}
        for city in cities:
            movr_objects[city] = {"users": movr.get_users(city), "vehicles": movr.get_vehicles(city)}
            if len(movr_objects[city]["vehicles"]) == 0 or len(movr_objects[city]["users"]) == 0:
                logging.error("must have users and vehicles for '%s' in the movr database to generte load. try running with the 'load' command.", city)
                sys.exit(1)

        active_rides = movr.get_active_rides()
        logging.info("simulating movr load for cities %s", cities)
        logging.info("conn string is " + conn_string)

        threads = []
        for i in range(args.num_threads):
            t = Thread(target=simulate_movr_load, args=(conn_string, cities, movr_objects,
                                                        active_rides, args.read_percentage, args.echo_sql ))
            t.start()
            threads.append(t)

        while True: #keep main thread alive to catch exit signals
            time.sleep(0.5)




