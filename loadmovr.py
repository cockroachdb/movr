#!/usr/bin/python

import argparse
from movr import MovR
import random
import sys
import time

MOVR_PARTITIONS = {
    "us_east": ["new york", "boston", "washington dc"],
    "us_west": ["san francisco", "seattle", "los angeles"],
    "eu_west": ["amsterdam", "paris", "rome"]
}

def load_movr_data(movr, num_users, num_vehicles, num_rides):

    #get all cities
    cities = []
    for region in MOVR_PARTITIONS:
        cities += MOVR_PARTITIONS[region]

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

def simulate_movr_load(movr, cities):
    #note this is all in memory so be careful

    movr_objects = {}
    for city in cities:
        movr_objects[city] = {"users": movr.get_users(city), "vehicles": movr.get_vehicles(city) }
        if len(movr_objects[city]["vehicles"]) == 0 or len(movr_objects[city]["users"]) == 0:
            print "must have users and vehicles in the movr database to generte load. try running with the --load command."
            sys.exit(1)


    active_rides =  movr.get_active_rides()

    while True:
        try:
            active_city = random.choice(cities)
            if random.random() < .01:
                movr_objects[active_city]["users"].append(movr.add_user(active_city)) #simulate new login
            elif random.random() < .15:
                movr.get_vehicles(active_city,25) #simulate user loading screen
            elif random.random() < .001:
                movr_objects[active_city]["vehicles"].append(
                    movr.add_vehicle(active_city, random.choice(movr_objects[active_city]["users"]).id)) #add vehicles
            elif random.random() < .42:
                ride = movr.start_ride(active_city, random.choice(movr_objects[active_city]["users"]).id,
                                       random.choice(movr_objects[active_city]["vehicles"]).id)
                active_rides.append(ride)
            else:
                if len(active_rides):
                    ride = active_rides.pop()
                    movr.end_ride(ride.city, ride.id)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    #@todo: add subparsers for loadgen: https://stackoverflow.com/questions/10448200/how-to-parse-multiple-nested-sub-commands-using-python-argparse
    parser = argparse.ArgumentParser(description='CLI for MovR.')
    parser.add_argument('--url', dest='conn_string', default='postgres://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database. Default is 'postgres://root@localhost:26257/movr?sslmode=disable'")
    parser.add_argument('--num-users', dest='num_users', type=int, default=50)
    parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10)
    parser.add_argument('--num-rides', dest='num_rides', type=int, default=500)
    parser.add_argument('--city', dest='city', action='append',
                        help='The names of the cities to use with generating load.')
    parser.add_argument('--load', dest='load', action='store_true', help='Load data into the MovR database')
    parser.add_argument('--reload-tables', dest='reload_tables', action='store_true',
                        help='Drop and reload MovR tables. Use with --load')
    parser.add_argument('--enable-ccl-features', dest='is_enterprise', action='store_true',
                        help='set this if your cluster has an enterprise license')
    parser.add_argument('--exponential-txn-backoff', dest='exponential_txn_backoff', action='store_true',
                        help='set this if you want retriable transactions to backoff exponentially')
    args = parser.parse_args()

    if args.conn_string.find("/movr") < 0:
        print "The connection string needs to point to a database named 'movr'"
        sys.exit(1)

    movr = MovR(args.conn_string.replace("postgres://", "cockroachdb://"), MOVR_PARTITIONS,
                is_enterprise=args.is_enterprise, reload_tables=args.reload_tables,
                exponential_txn_backoff=args.exponential_txn_backoff)

    print "connected to movr database @ %s" % args.conn_string

    cities = ['new york'] if args.city == None else args.city

    if args.num_users <= 0 or args.num_rides <= 0 or args.num_vehicles <= 0:
        print "The number of objects to generate must be > 0"
        sys.exit(1)



    if args.reload_tables or args.load:
        print "loading movr data with %d users, %d vehicles, and %d rides" % \
              (args.num_users, args.num_vehicles, args.num_rides)
        load_movr_data(movr, args.num_users, args.num_vehicles, args.num_rides)

    else:
        print "simulating movr load for cities %s" % cities
        simulate_movr_load(movr, cities)



