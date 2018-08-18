#!/usr/bin/python

import argparse
from movr import MovR
import random
import sys
import time

DEFAULT_PARTITION_MAP = {
    "us_east": ["new york", "boston", "washington dc"],
    "us_west": ["san francisco", "seattle", "los angeles"],
    "eu_west": ["amsterdam", "paris", "rome"]
}


def load_movr_data(movr, num_users, num_vehicles, num_rides, cities):

    for city in cities:
        print "populating %s" % city
        # add users
        start_time = time.time()
        city_user_count = int(num_users / len(cities)) if int(num_users / len(cities)) > 0 else 1
        movr.add_users(city_user_count, city)
        print "added %d users in %f seconds (%f users/second)" % \
              (city_user_count,  time.time() - start_time, city_user_count / float(time.time() - start_time))

        # add vehicles
        start_time = time.time()
        city_vehicle_count = int(num_vehicles / len(cities)) if int(num_vehicles / len(cities)) > 0 else 1
        movr.add_vehicles(city_vehicle_count, city)
        print "added %d vehicles in %f seconds (%f vehicles/second)" % \
              (city_vehicle_count, time.time() - start_time, city_vehicle_count / float(time.time() - start_time))

        # add rides
        start_time = time.time()
        city_ride_count = int(num_rides/len(cities)) if int(num_rides/len(cities)) > 0 else 1
        movr.add_rides(city_ride_count, city)
        print "added %d rides in %f seconds (%f rides/second)" % \
              (city_ride_count, time.time() - start_time, city_ride_count / float(time.time() - start_time))

    return

def simulate_movr_load(movr, cities, read_percentage):
    #note this is all in memory so be careful

    movr_objects = {}
    for city in cities:
        movr_objects[city] = {"users": movr.get_users(city), "vehicles": movr.get_vehicles(city) }
        if len(movr_objects[city]["vehicles"]) == 0 or len(movr_objects[city]["users"]) == 0:
            print "must have users and vehicles for '%s' in the movr database to generte load. try running with the --load command." % city
            sys.exit(1)


    active_rides =  movr.get_active_rides()

    #@todo: it looks like keeping movr objects in arrays is causing too many queries from the ORM.
    while True:
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
        except KeyboardInterrupt:
            break


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

    load_parser = subparsers.add_parser('load', help="load movr data into a database")
    load_parser.add_argument('--num-users', dest='num_users', type=int, default=50)
    load_parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10)
    load_parser.add_argument('--num-rides', dest='num_rides', type=int, default=500)
    load_parser.add_argument('--partition-by', dest='partition_pair', action='append',
                             help='Pairs in the form <partition>:<city_id> that will be used to enable geo-partitioning. Example: us_west:seattle. Use this flag multiple times to add multiple cities.')
    load_parser.add_argument('--enable-geo-partitioning', dest='enable_geo_partitioning', action='store_true',
                             help='set this if your cluster has an enterprise license')
    load_parser.add_argument('--reload-tables', dest='reload_tables', action='store_true',
                             help='Drop and reload MovR tables. Use with --load')

    run_parser = subparsers.add_parser('run', help="generate fake traffic for the movr database")
    run_parser.add_argument('--city', dest='city', action='append',
                            help='The names of the cities to use when generating load. Use this flag multiple times to add multiple cities.')
    run_parser.add_argument('--read-percentage', dest='read_percentage', type=float,
                            help='Value between 0-1 indicating how many reads to perform as a percentage of overall traffic',
                            default=.9)

    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")

    parser.add_argument('--echo-sql', dest='echo_sql', action='store_true',
                        help='set this if you want to print all executed SQL statements')

    return parser

if __name__ == '__main__':
    args = setup_parser().parse_args()

    if args.conn_string.find("/movr") < 0:
        print "The connection string needs to point to a database named 'movr'"
        sys.exit(1)

    conn_string = args.conn_string.replace("postgres://", "cockroachdb://")
    conn_string = conn_string.replace("postgresql://", "cockroachdb://")

    # population partitions
    partition_city_map = extract_partition_pairs_from_cli(args.partition_pair if args.subparser_name=='load' else None)

    enable_geo_partitioning = args.enable_geo_partitioning if args.subparser_name == 'load' else None
    reload_tables = args.reload_tables if args.subparser_name == 'load' else None
    movr = MovR(conn_string, partition_city_map,
                enable_geo_partitioning=enable_geo_partitioning, reload_tables=reload_tables,
                echo=args.echo_sql)


    print "connected to movr database @ %s" % args.conn_string

    all_cities = []
    for partition in partition_city_map:
        all_cities += partition_city_map[partition]


    if args.subparser_name == 'load' and (args.num_users <= 0 or args.num_rides <= 0 or args.num_vehicles <= 0):
        print "The number of objects to generate must be > 0"
        sys.exit(1)


    if args.subparser_name=='load':
        print "loading cities %s" % all_cities
        print "loading movr data with %d users, %d vehicles, and %d rides" % \
              (args.num_users, args.num_vehicles, args.num_rides)
        load_movr_data(movr, args.num_users, args.num_vehicles, args.num_rides, all_cities)

    else:
        if args.read_percentage < 0 or args.read_percentage > 1:
            print "read percentage must be between 0 and 1"
            sys.exit(1)
        cities = all_cities if args.city is None else args.city
        print "simulating movr load for cities %s" % cities
        simulate_movr_load(movr, cities, args.read_percentage)



