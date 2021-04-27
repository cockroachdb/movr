#!/usr/bin/python

from movr import MovR
from generators import MovRGenerator
import argparse
import sys, os, time, datetime, random, math, signal, threading, re
import logging
from faker import Faker
from models import User, Vehicle, Ride, VehicleLocationHistory, PromoCode
from cockroachdb.sqlalchemy import run_transaction
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode
from movr_stats import MovRStats
from tabulate import tabulate


RUNNING_THREADS = []
TERMINATE_GRACEFULLY = False
DEFAULT_READ_PERCENTAGE = .95

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


DEFAULT_REGIONS = ["us-east1", "us-west1", "europe-west1"]

# Create a connection to the movr database and populate a set of regions with rides, vehicles, and users.
def load_movr_data(conn_string, num_users, num_vehicles, num_rides, num_histories, num_promo_codes_per_thread, regions, echo_sql = False):
    if num_users <= 0 or num_rides <= 0 or num_vehicles <= 0:
        raise ValueError("The number of objects to generate must be > 0")

    start_time = time.time()
    with MovR(conn_string, echo=echo_sql) as movr:
        engine = create_engine(conn_string, convert_unicode=True, echo=echo_sql)
        for region in regions:
            if TERMINATE_GRACEFULLY:
                logging.debug("terminating")
                break

            logging.info("Generating user data for %s...", region)
            add_users(engine, num_users, region)
            logging.info("Generating vehicle data for %s...", region)
            add_vehicles(engine, num_vehicles, region)
            logging.info("Generating ride data for %s...", region)
            add_rides(engine, num_rides, region)
            logging.info("Generating location history data for %s...", region)
            add_vehicle_location_histories(engine, num_histories, region)
            logging.info("populated %s in %f seconds",
                  region, time.time() - start_time)

        logging.info("Generating %s promo codes...", num_promo_codes_per_thread)
        add_promo_codes(engine, num_promo_codes_per_thread)

    return

# Generates evenly distributed load among the provided regions


def simulate_movr_load(conn_string, use_multi_region, regions, movr_objects, active_rides, read_percentage, follower_reads, connection_duration_in_seconds, echo_sql = False):

    datagen = Faker()
    while True:
        logging.debug("creating a new connection to %s, which will reset in %d seconds", conn_string, connection_duration_in_seconds)
        try:
            with MovR(conn_string, multi_region= use_multi_region, echo=echo_sql) as movr:
                timeout = time.time() + connection_duration_in_seconds #refresh connections so load can balance among cluster nodes even if the cluster size changes
                while True:

                    if TERMINATE_GRACEFULLY:
                        logging.debug("Terminating thread.")
                        return

                    if time.time() > timeout:
                        break


                    if random.random() < read_percentage:
                        # simulate user loading screen
                        start = time.time()
                        movr.get_vehicles(follower_reads, 25)
                        stats.add_latency_measurement("get vehicles",time.time() - start )

                    else:

                        # every write tick, simulate the various vehicles updating their locations if they are being used for rides
                        for ride in active_rides[0:10]:

                            latlong = MovRGenerator.generate_random_latlong()
                            start = time.time()
                            movr.update_ride_location(ride_id=ride['id'], lat=latlong['lat'],
                                                      long=latlong['long'])
                            stats.add_latency_measurement(ACTION_UPDATE_RIDE_LOC, time.time() - start)


                        #do write operations randomly
                        if random.random() < .03:
                            # simulate a movr marketer creating a new promo code
                            start = time.time()
                            promo_code = movr.create_promo_code(
                                code="_".join(datagen.words(nb=3)) + "_" + str(time.time()),
                                description=datagen.paragraph(),
                                expiration_time=datetime.datetime.now() + datetime.timedelta(
                                    days=random.randint(0, 30)),
                                rules={"type": "percent_discount", "value": "10%"})
                            stats.add_latency_measurement(ACTION_NEW_CODE, time.time() - start)
                            movr_objects["global"].get("promo_codes", []).append(promo_code)


                        elif random.random() < .1:
                            # simulate a user applying a promo code to her account
                            start = time.time()
                            movr.apply_promo_code(random.choice(movr_objects["local"]["users"])['id'],
                                random.choice(movr_objects["global"]["promo_codes"]))
                            stats.add_latency_measurement(ACTION_APPLY_CODE, time.time() - start)
                        elif random.random() < .3:
                            # simulate new signup
                            start = time.time()
                            new_user = movr.add_user(datagen.name(), datagen.address(), datagen.credit_card_number())
                            stats.add_latency_measurement(ACTION_NEW_USER, time.time() - start)
                            movr_objects["local"]["users"].append(new_user)

                        elif random.random() < .1:
                            # simulate a user adding a new vehicle to the population
                            start = time.time()
                            new_vehicle = movr.add_vehicle(
                                                owner_id = random.choice(movr_objects["local"]["users"])['id'],
                                                type = MovRGenerator.generate_random_vehicle(),
                                                vehicle_metadata = MovRGenerator.generate_vehicle_metadata(type),
                                                status=MovRGenerator.get_vehicle_availability(),
                                                current_location = datagen.address())
                            stats.add_latency_measurement(ACTION_ADD_VEHICLE, time.time() - start)
                            movr_objects["local"]["vehicles"].append(new_vehicle)

                        elif random.random() < .5:
                            # simulate a user starting a ride
                            start = time.time()
                            ride = movr.start_ride(random.choice(movr_objects["local"]["users"])['id'],
                                                   random.choice(movr_objects["local"]["vehicles"])['id'])
                            stats.add_latency_measurement(ACTION_START_RIDE, time.time() - start)
                            active_rides.append(ride)

                        else:
                            if len(active_rides):
                                #simulate a ride ending
                                ride = active_rides.pop()
                                start = time.time()
                                movr.end_ride(ride['id'])
                                stats.add_latency_measurement(ACTION_END_RIDE, time.time() - start)
        except DBAPIError:
            logging.error("lost connection to the db. sleeping for 10 seconds")
            time.sleep(10)


def get_regions(region_list):
    regions = []
    if region_list is None:
        return DEFAULT_REGIONS
    else:
        return region_list

def extract_zone_pairs_from_cli(pair_list):
    if pair_list is None:
        return {}

    zone_pairs = {}

    for zone_pair in pair_list:
        pair = zone_pair.split(":")
        if len(pair) < 1:
            pair = ["default"].append(pair[0])
        else:
            pair = [pair[0], ":".join(pair[1:])]  # if there are many colons convert this to only two items

        zone_pairs.setdefault(pair[0], "")
        zone_pairs[pair[0]] = pair[1]

    return zone_pairs

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
                            help='The number threads to use for MovR (default =5)')
    parser.add_argument('--log-level', dest='log_level', default='info',
                        help='The log level ([debug|info|warning|error]) for MovR messages. (default = info)')
    parser.add_argument('--app-name', dest='app_name', default='movr',
                        help='The name that can be used for filtering statements by client in the Admin UI.')
    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")




    parser.add_argument('--echo-sql', dest='echo_sql', action='store_true',
                        help='set this if you want to print all executed SQL statements')

    ###############
    # LOAD COMMANDS
    ###############
    load_parser = subparsers.add_parser('load', help="load movr data into a database")
    load_parser.add_argument('--multi-region', dest='multi_region', action='store_true', default=False,
                        help='Load MovR with multi-region schemas. Useful for showing an app built from day-one for a global deployment. You can convert a single-region MovR to multi-region one using the "configure-multi-region" command')
    load_parser.add_argument('--num-users', dest='num_users', type=int, default=50,
                             help='The number of random users to add to the dataset')
    load_parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10,
                             help='The number of random vehicles to add to the dataset')
    load_parser.add_argument('--num-rides', dest='num_rides', type=int, default=500,
                             help='The number of random rides to add to the dataset')
    load_parser.add_argument('--num-histories', dest='num_histories', type=int, default=1000,
                             help='The number of ride location histories to add to the dataset')
    load_parser.add_argument('--num-promo-codes', dest='num_promo_codes', type=int, default=1000,
                             help='The number of promo codes to add to the dataset')
    load_parser.add_argument('--region', dest='region', action='append',
                             help='this will load random data for each of the regions specified. Use this flag multiple times to add multiple regions.')
    load_parser.add_argument('--skip-init', dest='skip_reload_tables', action='store_true',
                             help='Keep existing tables and data when loading Movr tables')

    ###############
    # RUN COMMANDS
    ###############
    run_parser = subparsers.add_parser('run', help="generate fake traffic for the movr database")
    run_parser.add_argument('--multi-region', dest='multi_region', action='store_true', default=False,
                            help='Run MovR with single-region queries that use composite primary keys. Requires multi-region schema options.')
    run_parser.add_argument('--connection-duration', dest='connection_duration_in_seconds', type=int,
                            help='The number of seconds to keep database connections alive before resetting them.',
                            default=30)
    run_parser.add_argument('--follower-reads', dest='follower_reads', action='store_true', default=False,
                            help='Use the closest replica to serve fast, but slightly stale, read requests')
    run_parser.add_argument('--region', dest='region', action='append',
                            help='The names of the regions to use when generating load. Use this flag multiple times to add multiple regions.')
    run_parser.add_argument('--read-only-percentage', dest='read_percentage', type=float,
                            help='Value between 0-1 indicating how many simulated read-only home screen loads to perform as a percentage of overall activities',
                            default=.95)

    ###################
    #configure_multi_region
    ###################

    #@todo: rename - this is adding regions to the database
    scale_out_parser = subparsers.add_parser('configure-multi-region', help="perform online update to single-region schema to enable multi-region deployments")

    scale_out_parser.add_argument('--region', dest='region', action='append',
                            help='The names of the regions to use. Use this flag multiple times to add multiple regions.')

    scale_out_parser.add_argument('--preview-queries', dest='preview_queries', action='store_true',
                        default=False,
                        help='See commands to transform from single region to multi-region')

    ####################
    # PARTITION COMMANDS
    ####################

    #@todo: want to be able to do this in multiple steps
    partition_parser = subparsers.add_parser('partition', help="partition the movr data to improve performance in geo-distributed environments. Your cluster must have an enterprise license to use this feature (https://cockroa.ch/2BoAlgB)")

    partition_parser.add_argument('--preview-queries', dest='preview_queries', action='store_true',
                             help='If this flag is set, movr will print the commands to partition the data, but will not actually run them.')

    return parser

##############
# BULK DATA LOADING
##############

def add_rides(engine, num_rides, region):
    chunk_size = 800
    datagen = Faker()

    def add_rides_helper(sess, chunk, n):
        #@todo:
        #users = sess.query(User).filter_by(region=region).all()
        users = sess.query(User).all()
        #vehicles = sess.query(Vehicle).filter_by(region=region).all()
        vehicles = sess.query(Vehicle).all()

        rides = []
        for i in range(chunk, min(chunk + chunk_size, num_rides)):
            start_time = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
            rides.append(Ride(id=MovRGenerator.generate_uuid(),
                              #region=region, #@todo: use system table
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
            codes.append(PromoCode(code = code,
                                   description = datagen.paragraph(),
                                   expiration_time = datetime.datetime.now() + datetime.timedelta(days=random.randint(0,30)),
                                   rules = {"type": "percent_discount", "value": "10%"}))
        sess.bulk_save_objects(codes)

    for chunk in range(0, num_codes, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_codes_helper(s, chunk, min(chunk + chunk_size, num_codes)))



def add_vehicle_location_histories(engine, num_histories, region):
    chunk_size = 5000

    def add_vehicle_location_histories_helper(sess, chunk, n):
        #rides = sess.query(Ride).filter_by(region=region).all() #@todo
        rides = sess.query(Ride).all()

        histories = []
        for i in range(chunk, min(chunk + chunk_size, num_histories)):
            latlong = MovRGenerator.generate_random_latlong()
            histories.append(VehicleLocationHistory(
                ride_id=random.choice(rides).id,
                lat=latlong["lat"],
                long=latlong["long"]))

        sess.bulk_save_objects(histories)

    for chunk in range(0, num_histories, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_vehicle_location_histories_helper(s, chunk, min(chunk + chunk_size, num_histories)))

def add_users(engine, num_users, region):
    chunk_size = 1000
    datagen = Faker()

    def add_users_helper(sess, chunk, n):
        users = []
        for i in range(chunk, n):
            users.append(User(id=MovRGenerator.generate_uuid(),
                              # region=region, #@todo: how do we override this in an orm?
                              name=datagen.name(),
                              address=datagen.address(),
                              credit_card=datagen.credit_card_number()))
        sess.bulk_save_objects(users)

    for chunk in range(0, num_users, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_users_helper(s, chunk, min(chunk + chunk_size, num_users)))

def add_vehicles(engine, num_vehicles, region):
    chunk_size = 1000
    datagen = Faker()

    def add_vehicles_helper(sess, chunk, n):
        #owners = sess.query(User).filter_by(crdb_region=region).all() #@todo: this fails
        owners = sess.query(User).all() #@todo: this fails
        vehicles = []
        for i in range(chunk, n):
            vehicle_type = MovRGenerator.generate_random_vehicle()
            vehicles.append(Vehicle(id=MovRGenerator.generate_uuid(),
                                    type=vehicle_type,
                                    #region=region, #@todo: figure this out
                                    current_location=datagen.address(),
                                    owner_id=(random.choice(owners)).id,
                                    status=MovRGenerator.get_vehicle_availability(),
                                    ext=MovRGenerator.generate_vehicle_metadata(vehicle_type)))
        sess.bulk_save_objects(vehicles)

    for chunk in range(0, num_vehicles, chunk_size):
        run_transaction(sessionmaker(bind=engine),
                        lambda s: add_vehicles_helper(s, chunk, min(chunk + chunk_size, num_vehicles)))

def run_data_loader(conn_string, regions, num_users, num_rides, num_vehicles, num_histories, num_promo_codes, num_threads,
                    skip_reload_tables, use_multi_region, echo_sql):
    if num_users <= 0 or num_rides <= 0 or num_vehicles <= 0:
        raise ValueError("The number of objects to generate must be > 0")

    start_time = time.time()

    logging.info("Loading single region MovR") if not use_multi_region else logging.info("Loading multi region MovR")

    with MovR(conn_string, init_tables=(not skip_reload_tables), multi_region = use_multi_region, echo=echo_sql) as movr:

        logging.info("loading regions %s", regions)
        logging.info("loading movr data with ~%d users, ~%d vehicles, ~%d rides, ~%d histories, and ~%d promo codes",
                     num_users, num_vehicles, num_rides, num_histories, num_promo_codes)

    usable_threads = min(num_threads, len(regions))  # don't create more than 1 thread per region
    if usable_threads < num_threads:
        logging.info("Only using %d of %d requested threads, since we only create at most one thread per region",
                     usable_threads, num_threads)

    regions_per_thread = int(math.ceil((float(len(regions)) / usable_threads)))
    num_users_per_region = int(math.ceil((float(num_users) / len(regions))))
    num_rides_per_region = int(math.ceil((float(num_rides) / len(regions))))
    num_vehicles_per_region = int(math.ceil((float(num_vehicles) / len(regions))))
    num_histories_per_region = int(math.ceil((float(num_histories) / len(regions))))

    num_promo_codes_per_thread = int(math.ceil((float(num_promo_codes) / usable_threads)))

    RUNNING_THREADS = []


    original_region_count = len(regions)
    for i in range(usable_threads):
        if len(regions) > 0:
            t = threading.Thread(target=load_movr_data, args=(conn_string, num_users_per_region, num_vehicles_per_region,
                                                              num_rides_per_region, num_histories_per_region, num_promo_codes_per_thread,
                                                              regions[:regions_per_thread],
                                                              echo_sql))
            regions = regions[regions_per_thread:]
            t.start()
            RUNNING_THREADS.append(t)

    while threading.active_count() > 1:  # keep main thread alive so we can catch ctrl + c
        time.sleep(0.1)

    duration = time.time() - start_time

    logging.info("populated %s regions in %f seconds", original_region_count, duration)

# generate fake load for objects within the provided region list
def run_load_generator(conn_string, read_percentage, connection_duration_in_seconds, region_list, use_multi_region, follower_reads, echo_sql, num_threads):
    if read_percentage < 0 or read_percentage > 1:
        raise ValueError("read percentage must be between 0 and 1")


    logging.info("simulating movr load for regions %s", region_list)

    movr_objects = { "local": {}, "global": {}}

    logging.info("warming up....")
    with MovR(conn_string, multi_region=  use_multi_region, echo=echo_sql) as movr:
        active_rides = []
        for region in region_list:
            movr_objects["local"] = {"users": movr.get_users(follower_reads), "vehicles": movr.get_vehicles(follower_reads)}
            if len(list(movr_objects["local"]["vehicles"])) == 0 or len(list(movr_objects["local"]["users"])) == 0:
                logging.error("must have users and vehicles loaded in the movr database to generate load. try running with the 'load' command.")
                sys.exit(1)

            active_rides.extend(movr.get_active_rides(region, follower_reads))
        movr_objects["global"]["promo_codes"] = movr.get_promo_codes()

    RUNNING_THREADS = []
    logging.info("running single region queries...") if not use_multi_region else logging.info("running multi-region queries...")
    for i in range(num_threads):
        t = threading.Thread(target=simulate_movr_load, args=(conn_string, use_multi_region, region_list, movr_objects,
                                                    active_rides, read_percentage, follower_reads,
                                                              connection_duration_in_seconds, echo_sql ))
        t.start()
        RUNNING_THREADS.append(t)

    while True: #keep main thread alive to catch exit signals
        time.sleep(15)

        stats.print_stats(action_list=[ACTION_ADD_VEHICLE, ACTION_GET_VEHICLES, ACTION_UPDATE_RIDE_LOC,
                           ACTION_NEW_CODE, ACTION_APPLY_CODE, ACTION_NEW_USER,
                           ACTION_START_RIDE, ACTION_END_RIDE])

        stats.new_window()


if __name__ == '__main__':

    global stats
    stats = MovRStats()
    # support ctrl + c for exiting multithreaded operation
    signal.signal(signal.SIGINT, signal_handler)

    args = setup_parser().parse_args()

    if not re.search('.*://.*/(.*)\?', args.conn_string):
        logging.error("The connection string needs to point to a database. Example: postgres://root@localhost:26257/mymovrdatabase?sslmode=disable")
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


    logging.info("connected to movr database @ %s" % args.conn_string)

    #format connection string to work with our cockroachdb driver.
    conn_string = args.conn_string.replace("postgres://", "cockroachdb://")
    conn_string = conn_string.replace("postgresql://", "cockroachdb://")
    conn_string = set_query_parameter(conn_string, "application_name", args.app_name)




    if args.subparser_name=='load':

        run_data_loader(conn_string, regions= get_regions(args.region), num_users= args.num_users, num_rides= args.num_rides, num_vehicles= args.num_vehicles, num_histories= args.num_histories,
                        num_promo_codes= args.num_promo_codes, num_threads= args.num_threads, use_multi_region=args.multi_region,
                        skip_reload_tables= args.skip_reload_tables, echo_sql= args.echo_sql)

    elif args.subparser_name == "configure-multi-region":
        if args.preview_queries:
            with MovR(conn_string, multi_region=True, init_tables=False, echo=args.echo_sql) as movr:

                queries = movr.get_multi_region_transformations(get_regions(args.region))
                print("DDL to convert a single region database to multi-region")

                print("===database regions===")
                for query in queries["database_regions"]:
                    print(query)

                print("===table localities ===")
                for query in queries["table_localities"]:
                    print(query)
            sys.exit(0)
        else:
            with MovR(conn_string, multi_region=True, init_tables=False, echo=args.echo_sql) as movr:
                movr.run_multi_region_transformations(get_regions(args.region))


    elif args.subparser_name=="partition":
        #@todo: ruggedize this so it doesnt break when run on a single region cluster. look at pg metadata
        # population partitions
        partition_region_map = extract_region_region_pairs_from_cli(args.region_region_pair)
        partition_zone_map = extract_zone_pairs_from_cli(args.region_zone_pair)

        print("\nPartitioning Setting Summary\n")
        rows = []
        for partition in sorted(list(partition_region_map)):
            for region in sorted(partition_region_map[partition]):
                rows.append([partition,region])
        print(tabulate(rows, ["partition", "region"]), "\n")

        rows = []
        for partition in partition_zone_map:
            rows.append([partition, partition_zone_map[partition]])
        print(tabulate(rows, ["partition", "zone where partitioned data will be moved"]), "\n")

        rows = []
        for partition in partition_zone_map:
            rows.append(["promo_codes", partition_zone_map[partition]])
        print(tabulate(rows, ["reference table", "zones where index data will be replicated"]), "\n")



        with MovR(conn_string, multi_region=True, init_tables=False, echo=args.echo_sql) as movr:
            if args.preview_queries:
                queries = movr.get_geo_partitioning_queries(partition_region_map, partition_zone_map)
                print("queries to geo-partition the database")

                rows = []

                print("===table and index partitions===")
                for query in queries["table_partitions"]:
                    print(query)

                for query in queries["index_partitions"]:
                    print(query)

                print("===table and index zones===")
                for query in queries["table_zones"]:
                    print(query)
                for query in queries["index_zones"]:
                    print(query)

                print("===promo code indices for locality aware optimization===")
                for query in queries["promo_code_indices"]:
                    print(query)

                print("===promo code zones for locality aware optimization===")
                for query in queries["promo_code_zones"]:
                    print(query)

            else:
                print("partitioning tables...")
                movr.add_geo_partitioning(partition_region_map, partition_zone_map)
                print("done.")

    elif args.subparser_name == "run":
        run_load_generator(conn_string, read_percentage= args.read_percentage, connection_duration_in_seconds= args.connection_duration_in_seconds,
                           region_list= get_regions(args.region), use_multi_region=args.multi_region, follower_reads= args.follower_reads, echo_sql= args.echo_sql, num_threads= args.num_threads)
    else:
        run_load_generator(conn_string, read_percentage= DEFAULT_READ_PERCENTAGE,
                           connection_duration_in_seconds= 60, region_list= get_regions(None),
                           use_multi_region=False,
                           follower_reads= False, echo_sql= args.echo_sql, num_threads= args.num_threads)









