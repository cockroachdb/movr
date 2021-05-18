#!/usr/bin/python

from movr import MovR
from generators import MovRGenerator
import argparse
import sys
import os
import time
import datetime
import random
import math
import signal
import threading
import re
import logging
from faker import Faker
from models import User, Vehicle, Ride, VehicleLocationHistory, PromoCode
from cockroachdb.sqlalchemy import run_transaction
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode
from movr_stats import MovRStats


RUNNING_THREADS = []
TERMINATE_GRACEFULLY = False
DEFAULT_READ_PERCENTAGE = .95

# @todo: add checks for multi-region operations on single region schemas.

ACTION_ADD_VEHICLE = "add vehicle"
ACTION_GET_VEHICLES = "get vehicles"
ACTION_UPDATE_RIDE_LOC = "log ride location"
ACTION_NEW_CODE = "new promo code"
ACTION_APPLY_CODE = "apply promo code"
ACTION_NEW_USER = "new user"
ACTION_START_RIDE = "start ride"
ACTION_END_RIDE = "end ride"


def signal_handler(sig, frame):
    global TERMINATE_GRACEFULLY
    grace_period = 15
    logging.info(
        'Waiting at most %d seconds for threads to shutdown...', grace_period)
    TERMINATE_GRACEFULLY = True

    start = time.time()
    while threading.active_count() > 1:
        if (time.time() - start) > grace_period:
            logging.info("Grace period has passed. Killing threads.")
            os._exit(1)
        else:
            time.sleep(.1)

    logging.info("Shutting down gracefully.")
    sys.exit(0)

# Create a connection to the movr database and populate a set of cities with rides, vehicles, and users.


def load_movr_data(conn_string, num_users, num_vehicles, num_rides, num_histories, num_promo_codes_per_thread, cities, echo_sql=False):
    if num_users <= 0 or num_rides <= 0 or num_vehicles <= 0:
        raise ValueError("The number of objects to generate must be > 0.")

    start_time = time.time()
    with MovR(conn_string, echo=echo_sql) as movr:
        engine = create_engine(
            conn_string, convert_unicode=True, echo=echo_sql)
        for city in cities:
            if TERMINATE_GRACEFULLY:
                logging.debug("Terminating...")
                break

            logging.info("Generating user data for %s...", city)
            add_users(engine, num_users, city)
            logging.info("Generating vehicle data for %s...", city)
            add_vehicles(engine, num_vehicles, city)
            logging.info("Generating ride data for %s...", city)
            add_rides(engine, num_rides, city)
            logging.info("Generating location history data for %s...", city)
            add_vehicle_location_histories(engine, num_histories, city)
            logging.info("Populated %s in %f seconds.",
                         city, time.time() - start_time)

        logging.info("Generating %s promo codes...",
                     num_promo_codes_per_thread)
        add_promo_codes(engine, num_promo_codes_per_thread)

    return

# Generates evenly distributed load among the provided cities


def simulate_movr_load(conn_string, cities, movr_objects, active_rides, read_percentage, follower_reads, connection_duration_in_seconds, echo_sql=False):

    datagen = Faker()
    while True:
        logging.debug("Creating a new connection to %s, which will reset in %d seconds.",
                      conn_string, connection_duration_in_seconds)
        try:
            with MovR(conn_string, echo=echo_sql) as movr:
                # refresh connections so load can balance among cluster nodes even if the cluster size changes
                timeout = time.time() + connection_duration_in_seconds
                while True:

                    if TERMINATE_GRACEFULLY:
                        logging.debug("Terminating thread...")
                        return

                    if time.time() > timeout:
                        break

                    active_city = random.choice(cities)

                    if random.random() < read_percentage:
                        # simulate user loading screen
                        start = time.time()
                        movr.get_vehicles(active_city, follower_reads, 25)
                        stats.add_latency_measurement(
                            "get vehicles", time.time() - start)

                    else:

                        # every write tick, simulate the various vehicles updating their locations if they are being used for rides
                        for ride in active_rides[0:10]:

                            latlong = MovRGenerator.generate_random_latlong()
                            start = time.time()
                            movr.update_ride_location(ride['city'], ride_id=ride['id'], lat=latlong['lat'],
                                                      long=latlong['long'])
                            stats.add_latency_measurement(
                                ACTION_UPDATE_RIDE_LOC, time.time() - start)

                        # do write operations randomly
                        if random.random() < .03:
                            # simulate a movr marketer creating a new promo code
                            start = time.time()
                            promo_code = movr.create_promo_code(
                                code="_".join(datagen.words(nb=3)) +
                                "_" + str(time.time()),
                                description=datagen.paragraph(),
                                expiration_time=datetime.datetime.now() + datetime.timedelta(
                                    days=random.randint(0, 30)),
                                rules={"type": "percent_discount", "value": "10%"})
                            stats.add_latency_measurement(
                                ACTION_NEW_CODE, time.time() - start)
                            movr_objects["global"].get(
                                "promo_codes", []).append(promo_code)

                        elif random.random() < .1:
                            # simulate a user applying a promo code to her account
                            start = time.time()
                            movr.apply_promo_code(active_city, random.choice(movr_objects["local"][active_city]["users"])['id'],
                                                  random.choice(movr_objects["global"]["promo_codes"]))
                            stats.add_latency_measurement(
                                ACTION_APPLY_CODE, time.time() - start)
                        elif random.random() < .3:
                            # simulate new signup
                            start = time.time()
                            new_user = movr.add_user(active_city, datagen.name(
                            ), datagen.address(), datagen.credit_card_number())
                            stats.add_latency_measurement(
                                ACTION_NEW_USER, time.time() - start)
                            movr_objects["local"][active_city]["users"].append(
                                new_user)

                        elif random.random() < .1:
                            # simulate a user adding a new vehicle to the population
                            start = time.time()
                            new_vehicle = movr.add_vehicle(active_city,
                                                           owner_id=random.choice(
                                                               movr_objects["local"][active_city]["users"])['id'],
                                                           type=MovRGenerator.generate_random_vehicle(),
                                                           vehicle_metadata=MovRGenerator.generate_vehicle_metadata(
                                                               type),
                                                           status=MovRGenerator.get_vehicle_availability(),
                                                           current_location=datagen.address())
                            stats.add_latency_measurement(
                                ACTION_ADD_VEHICLE, time.time() - start)
                            movr_objects["local"][active_city]["vehicles"].append(
                                new_vehicle)

                        elif random.random() < .5:
                            # simulate a user starting a ride
                            start = time.time()
                            ride = movr.start_ride(active_city, random.choice(movr_objects["local"][active_city]["users"])['id'],
                                                   random.choice(movr_objects["local"][active_city]["vehicles"])['id'])
                            stats.add_latency_measurement(
                                ACTION_START_RIDE, time.time() - start)
                            active_rides.append(ride)

                        else:
                            if len(active_rides):
                                # simulate a ride ending
                                ride = active_rides.pop()
                                start = time.time()
                                movr.end_ride(ride['city'], ride['id'])
                                stats.add_latency_measurement(
                                    ACTION_END_RIDE, time.time() - start)
        except DBAPIError:
            logging.error("Lost connection to the database. Sleeping for 10 seconds.")
            time.sleep(10)


def set_query_parameter(url, param_name, param_value):
    scheme, netloc, path, query_string, fragment = urlsplit(url)
    query_params = parse_qs(query_string)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return urlunsplit((scheme, netloc, path, new_query_string, fragment))


def setup_parser():
    parser = argparse.ArgumentParser(description='CLI for MovR.')
    subparsers = parser.add_subparsers(dest='subparser_name')

    ###########
    # GENERAL COMMANDS
    ##########
    parser.add_argument('--num-threads', dest='num_threads', type=int, default=5,
                        help='The number threads to use for MovR. (default =5)')
    parser.add_argument('--log-level', dest='log_level', default='info',
                        help='The log level ([debug|info|warning|error]) for MovR messages. (default = info)')
    parser.add_argument('--app-name', dest='app_name', default='movr',
                        help='The name that can be used for filtering statements by client in the Admin UI.')
    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="The connection string to the database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")
    parser.add_argument('--echo-sql', dest='echo_sql', action='store_true',
                        help='If set, the application prints all executed SQL statements.')

    ###############
    # LOAD COMMANDS
    ###############
    load_parser = subparsers.add_parser(
        'load', help="Load data into an existing database.\nWARNING: This command will reset any existing tables to a single-region configuration unless '--multi-region' is specified.\nTo keep the existing tables in the movr database, use the '--skip-init' flag.")
    load_parser.add_argument('--multi-region', dest='multi_region', action='store_true', default=False,
                             help='Load MovR data to a database with a multi-region schema. Useful for showing an app built from day-one for a global deployment. You can convert a single-region MovR to multi-region one using the "configure-multi-region" command.')
    load_parser.add_argument('--num-users', dest='num_users', type=int, default=50,
                             help='The number of random users to add to the dataset.')
    load_parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10,
                             help='The number of random vehicles to add to the dataset.')
    load_parser.add_argument('--num-rides', dest='num_rides', type=int, default=500,
                             help='The number of random rides to add to the dataset.')
    load_parser.add_argument('--num-histories', dest='num_histories', type=int, default=1000,
                             help='The number of ride location histories to add to the dataset.')
    load_parser.add_argument('--num-promo-codes', dest='num_promo_codes', type=int, default=1000,
                             help='The number of promo codes to add to the dataset.')
    load_parser.add_argument('--city', dest='city', action='append',
                             help='Load random data for each of the cities specified. Use this flag multiple times to add multiple cities.')
    load_parser.add_argument('--skip-init', dest='skip_reload_tables', action='store_true',
                             help='Keep the existing tables in the movr database.')

    ###############
    # RUN COMMANDS
    ###############
    run_parser = subparsers.add_parser(
        'run', help="Generate fake traffic to tables in an initialized database.")
    run_parser.add_argument('--connection-duration', dest='connection_duration_in_seconds', type=int,
                            help='The number of seconds to keep database connections alive before resetting them.',
                            default=30)
    run_parser.add_argument('--follower-reads', dest='follower_reads', action='store_true', default=False,
                            help='Use the closest replica to serve fast, but slightly stale, read requests.')
    run_parser.add_argument('--city', dest='city', action='append',
                            help='The names of the cities to use when generating load. Use this flag multiple times to add multiple cities.')
    run_parser.add_argument('--read-only-percentage', dest='read_percentage', type=float,
                            help='Value between 0-1 indicating how many simulated read-only home screen loads to perform as a percentage of overall activities.',
                            default=.95)

    ###################
    # configure_multi_region
    ###################

    scale_out_parser = subparsers.add_parser(
        'configure-multi-region', help="Perform online schema change from a single-region schema to a multi-region schema.")
    scale_out_parser.add_argument('--region-city-pair', dest='region_city_pair', action='append',
                             help='Pairs in the form <region>:<city_id> that will be used to assign cities to regions. Example: us_west:seattle. Use this flag multiple times to assign multiple cities.\nIf no region pairs are specified, the application will guess.')
    scale_out_parser.add_argument('--primary-region', dest='primary_region', type=str, default=None,
                                  help='The primary region of the database.')
    scale_out_parser.add_argument('--preview-queries', dest='preview_queries', action='store_true',
                                  default=False,
                                  help='See commands to transform from single region to multi-region.')

    return parser

##############
# BULK DATA LOADING
##############


def add_rides(engine, num_rides, city):
    chunk_size = 800
    datagen = Faker()

    def add_rides_helper(sess, chunk, n):
        users = sess.query(User).filter_by(city=city).all()
        vehicles = sess.query(Vehicle).filter_by(city=city).all()

        rides = []
        for i in range(chunk, min(chunk + chunk_size, num_rides)):
            start_time = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
            rides.append(Ride(id=MovRGenerator.generate_uuid(),
                              city=city,
                              rider_id=random.choice(users).id,
                              vehicle_id=random.choice(vehicles).id,
                              start_time=start_time,
                              start_address=datagen.address(),
                              end_address=datagen.address(),
                              revenue=MovRGenerator.generate_revenue(),
                              end_time=start_time + datetime.timedelta(minutes=random.randint(0, 60))))
        sess.bulk_save_objects(rides)

    for chunk in range(0, num_rides, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_rides_helper(s, chunk, min(chunk + chunk_size, num_rides)))


def add_promo_codes(engine, num_codes):
    chunk_size = 800
    datagen = Faker()

    def add_codes_helper(sess, chunk, n):
        codes = []
        for i in range(chunk, min(chunk + chunk_size, num_codes)):
            code = "_".join(datagen.words(nb=3)) + "_" + str(time.time())
            codes.append(PromoCode(code=code,
                                   description=datagen.paragraph(),
                                   expiration_time=datetime.datetime.now(
                                   ) + datetime.timedelta(days=random.randint(0, 30)),
                                   rules={"type": "percent_discount", "value": "10%"}))
        sess.bulk_save_objects(codes)

    for chunk in range(0, num_codes, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_codes_helper(s, chunk, min(chunk + chunk_size, num_codes)))


def add_vehicle_location_histories(engine, num_histories, city):
    chunk_size = 5000

    def add_vehicle_location_histories_helper(sess, chunk, n):
        rides = sess.query(Ride).filter_by(city=city).all()

        histories = []
        for i in range(chunk, min(chunk + chunk_size, num_histories)):
            latlong = MovRGenerator.generate_random_latlong()
            histories.append(VehicleLocationHistory(
                city=city,
                ride_id=random.choice(rides).id,
                lat=latlong["lat"],
                long=latlong["long"]))

        sess.bulk_save_objects(histories)

    for chunk in range(0, num_histories, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_vehicle_location_histories_helper(s, chunk, min(chunk + chunk_size, num_histories)))


def add_users(engine, num_users, city):
    chunk_size = 1000
    datagen = Faker()

    def add_users_helper(sess, chunk, n):
        users = []
        for i in range(chunk, n):
            users.append(User(id=MovRGenerator.generate_uuid(),
                              city=city,
                              name=datagen.name(),
                              address=datagen.address(),
                              credit_card=datagen.credit_card_number()))
        sess.bulk_save_objects(users)

    for chunk in range(0, num_users, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_users_helper(s, chunk, min(chunk + chunk_size, num_users)))


def add_vehicles(engine, num_vehicles, city):
    chunk_size = 1000
    datagen = Faker()

    def add_vehicles_helper(sess, chunk, n):
        owners = sess.query(User).filter_by(city=city).all()
        vehicles = []
        for i in range(chunk, n):
            vehicle_type = MovRGenerator.generate_random_vehicle()
            vehicles.append(Vehicle(id=MovRGenerator.generate_uuid(),
                                    type=vehicle_type,
                                    city=city,
                                    current_location=datagen.address(),
                                    owner_id=(random.choice(owners)).id,
                                    status=MovRGenerator.get_vehicle_availability(),
                                    ext=MovRGenerator.generate_vehicle_metadata(vehicle_type)))
        sess.bulk_save_objects(vehicles)

    for chunk in range(0, num_vehicles, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_vehicles_helper(s, chunk, min(chunk + chunk_size, num_vehicles)))


def run_data_loader(conn_string, multi_region, cities, num_users, num_rides, num_vehicles, num_histories, num_promo_codes, num_threads,
                    skip_reload_tables, echo_sql):
    if num_users <= 0 or num_rides <= 0 or num_vehicles <= 0:
        raise ValueError("The number of objects to generate must be > 0.")

    start_time = time.time()

    logging.info("Loading MovR")

    with MovR(conn_string, multi_region=multi_region, reset_tables=(not skip_reload_tables), echo=echo_sql) as movr:

        logging.info("Loading cities %s.", cities)
        logging.info("Loading movr data with ~%d users, ~%d vehicles, ~%d rides, ~%d histories, and ~%d promo codes.",
                     num_users, num_vehicles, num_rides, num_histories, num_promo_codes)

    # don't create more than 1 thread per city
    usable_threads = min(num_threads, len(cities))
    if usable_threads < num_threads:
        logging.info("Only using %d of %d requested threads, since we only create at most one thread per city.",
                     usable_threads, num_threads)

    num_users_per_city = int(math.ceil(float(num_users) / len(cities)))
    num_rides_per_city = int(math.ceil(float(num_rides) / len(cities)))
    num_vehicles_per_city = int(math.ceil(float(num_vehicles) / len(cities)))
    num_histories_per_city = int(math.ceil(float(num_histories) / len(cities)))

    cities_per_thread = int(math.ceil(float(len(cities)) / usable_threads))
    num_promo_codes_per_thread = int(
        math.ceil(float(num_promo_codes) / (float(len(cities))/cities_per_thread)))

    RUNNING_THREADS = []

    original_city_count = len(cities)
    for i in range(usable_threads):
        if len(cities) > 0:
            t = threading.Thread(target=load_movr_data, args=(conn_string, num_users_per_city, num_vehicles_per_city,
                                                              num_rides_per_city, num_histories_per_city, num_promo_codes_per_thread,
                                                              cities[:cities_per_thread],
                                                              echo_sql))
            cities = cities[cities_per_thread:]
            t.start()
            RUNNING_THREADS.append(t)

    while threading.active_count() > 1:  # keep main thread alive so we can catch ctrl + c
        time.sleep(0.1)

    duration = time.time() - start_time

    logging.info("Populated %s cities in %f seconds.",
                 original_city_count, duration)

# generate fake load for objects within the provided city list


def run_load_generator(conn_string, read_percentage, connection_duration_in_seconds, city_list, follower_reads, echo_sql, num_threads):
    if read_percentage < 0 or read_percentage > 1:
        raise ValueError("Read percentage must be between 0 and 1.")

    logging.info("Simulating movr load for cities %s.", city_list)

    movr_objects = {"local": {}, "global": {}}

    logging.info("Warming up....")
    with MovR(conn_string, echo=echo_sql) as movr:
        active_rides = []
        for city in city_list:
            movr_objects["local"][city] = {"users": movr.get_users(
                city, follower_reads), "vehicles": movr.get_vehicles(city, follower_reads)}
            if len(list(movr_objects["local"][city]["vehicles"])) == 0 or len(list(movr_objects["local"][city]["users"])) == 0:
                logging.error(
                    "Must have users and vehicles for city '%s' in the database to generate load. Try running with the 'load' command.", city)
                sys.exit(1)

            active_rides.extend(movr.get_active_rides(city, follower_reads))
        movr_objects["global"]["promo_codes"] = movr.get_promo_codes()

    RUNNING_THREADS = []
    logging.info("Running queries...")
    for i in range(num_threads):
        t = threading.Thread(target=simulate_movr_load, args=(conn_string, city_list, movr_objects,
                                                              active_rides, read_percentage, follower_reads,
                                                              connection_duration_in_seconds, echo_sql))
        t.start()
        RUNNING_THREADS.append(t)

    while True:  # keep main thread alive to catch exit signals
        time.sleep(15)

        stats.print_stats(action_list=[ACTION_ADD_VEHICLE, ACTION_GET_VEHICLES, ACTION_UPDATE_RIDE_LOC,
                                       ACTION_NEW_CODE, ACTION_APPLY_CODE, ACTION_NEW_USER,
                                       ACTION_START_RIDE, ACTION_END_RIDE])

        stats.new_window()

DEFAULT_REGION_MAP = {
        'us_east': ['new york', 'boston', 'washington dc'],
        'us_west': ['san francisco', 'seattle', 'los angeles'],
        'us_central': ['chicago', 'detroit', 'minneapolis'],
        'eu_west': ['amsterdam', 'paris', 'rome']
    }

def get_city_list(city_list=None, region_map=DEFAULT_REGION_MAP):
    if city_list is None:
        cities = []
        for region in region_map:
            cities.extend(region_map[region])
    else:
        cities = city_list
    return cities

def assign_regions(cities, regions, primary_region, region_pairs=None):
    if regions is None:
        logging.error("Regions required.")
        sys.exit(1)
    if primary_region not in regions:
        logging.error("{0} must exist as a node locality in order to be a primary region.".format(primary_region))
        sys.exit(1)
    region_map = {region:[] for region in regions}
    unpaired_cities = list(cities)
    if region_pairs is not None:
        for pair in region_pairs:
            pair = pair.split(':')
            city = pair[0]
            region = pair[1]
            if region not in regions:
                logging.error("{0} must exist as a node locality in order to be paired.".format(region))
                sys.exit(1)
            region_map[region].append(city)
            if city in cities:
                unpaired_cities.remove(city)
            else:
                logging.info("{0} not in provided list of cities.".format(city))
    for city in cities:
        for region in regions:
            if 'us' in region:
                if 'east' in region:
                    if city in DEFAULT_REGION_MAP['us_east']:
                        region_map[region].append(city)
                        unpaired_cities.remove(city)
                if 'central' in region:
                    if city in DEFAULT_REGION_MAP['us_central']:
                        region_map[region].append(city)
                        unpaired_cities.remove(city)
                if 'west' in region:
                    if city in DEFAULT_REGION_MAP['us_west']:
                        region_map[region].append(city)
                        unpaired_cities.remove(city)
            elif 'eu' in region:
                if 'west' in region:
                    if city in DEFAULT_REGION_MAP['eu_west']:
                        region_map[region].append(city)
                        unpaired_cities.remove(city)
    if len(unpaired_cities) > 0:
        logging.info("Unable to map {0} to a region. Assigning to the primary region {1}.".format(unpaired_cities, primary_region))
        region_map[primary_region].extend(unpaired_cities)
    return region_map

if __name__ == '__main__':

    global stats
    stats = MovRStats()
    # support ctrl + c for exiting multithreaded operation
    signal.signal(signal.SIGINT, signal_handler)

    args = setup_parser().parse_args()

    if not re.search('.*://.*/(.*)\?', args.conn_string):
        logging.error(
            "The connection string needs to point to a database. Example: postgres://root@localhost:26257/mymovrdatabase?sslmode=disable")
        sys.exit(1)

    if args.num_threads <= 0:
        logging.error("Number of threads must be greater than 0.")
        sys.exit(1)

    if args.log_level not in ['debug', 'info', 'warning', 'error']:
        logging.error("Invalid log level: %s", args.log_level)
        sys.exit(1)

    level_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }

    logging.basicConfig(level=level_map[args.log_level],
                        format='[%(levelname)s] (%(threadName)-10s) %(message)s', )

    logging.info("connected to database @ %s" % args.conn_string)

    # format connection string to work with our cockroachdb driver.
    conn_string = args.conn_string.replace("postgres://", "cockroachdb://")
    conn_string = conn_string.replace("postgresql://", "cockroachdb://")
    conn_string = set_query_parameter(
        conn_string, "application_name", args.app_name)

    if args.subparser_name == 'load':

        run_data_loader(conn_string, multi_region=args.multi_region, cities=get_city_list(args.city), num_users=args.num_users, num_rides=args.num_rides, num_vehicles=args.num_vehicles, num_histories=args.num_histories,
                        num_promo_codes=args.num_promo_codes, num_threads=args.num_threads,
                        skip_reload_tables=args.skip_reload_tables, echo_sql=args.echo_sql)

    elif args.subparser_name == "configure-multi-region":
        with MovR(conn_string, primary_region=args.primary_region, echo=args.echo_sql) as movr:
            regions = movr.get_regions()
            cities = movr.get_cities()
            if regions is None:
                logging.error("To configure your database for multi-region features, you must specify cluster regions at startup.")
                sys.exit(1)
            if cities is None:
                logging.error("To configure your database for multi-region features, the database must have rows of data with city values.")
                sys.exit(1)
            region_map = assign_regions(cities, regions, movr.primary_region, args.region_city_pair)
            if args.preview_queries:
                queries = movr.get_multi_region_transformations(region_map)
                print("DDL to convert a single region database to multi-region")
                for query in queries:
                    print(query)
                sys.exit(0)
            else:
                movr.run_multi_region_transformations(region_map)

    elif args.subparser_name == "run":
        run_load_generator(conn_string, read_percentage=args.read_percentage, connection_duration_in_seconds=args.connection_duration_in_seconds,
                           city_list=get_city_list(args.city), follower_reads=args.follower_reads, echo_sql=args.echo_sql, num_threads=args.num_threads)
    else:
        run_load_generator(conn_string, read_percentage=DEFAULT_READ_PERCENTAGE,
                           connection_duration_in_seconds=60, city_list=get_city_list(None),
                           follower_reads=False, echo_sql=args.echo_sql, num_threads=args.num_threads)
