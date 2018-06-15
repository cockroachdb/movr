#!/usr/bin/python

import argparse
from movr import MovR
import random
import sys
import time

MOVR_CITIES = ["new york", "boston", "washington dc", "san francisco", "seattle", "los angeles", "amsterdam", "paris", "rome" ]

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

def simulate_movr_load(movr):
    users = movr.get_users()
    vehicles = movr.get_vehicles()

    if len(vehicles) == 0 or len(users) == 0:
        print "must have users and vehicles in the movr database to generte load. try running with the --load command."
        sys.exit(1)


    active_ride_ids = set(map(lambda x: x.id, movr.get_active_rides()))

    while True:
        try:
            if random.random() < .1:
                ride = movr.start_ride(random.choice(users).id, random.choice(vehicles).id)
                active_ride_ids.add(ride.id)
            else:
                if len(active_ride_ids):
                    ride_id = active_ride_ids.pop()  # pick arbitraty ride to end
                    movr.end_ride(ride_id)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    #@todo: add subparses for loadgen: https://stackoverflow.com/questions/10448200/how-to-parse-multiple-nested-sub-commands-using-python-argparse
    parser = argparse.ArgumentParser(description='CLI for MovR.')
    parser.add_argument('--url', dest='conn_string', default='cockroachdb://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database.")
    parser.add_argument('--num-users', dest='num_users', type=int, default=50)
    parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10)
    parser.add_argument('--num-rides', dest='num_rides', type=int, default=500)
    parser.add_argument('--city', dest='city', action='append',
                        help='The names of the cities to use with generating load.')
    parser.add_argument('--load', dest='load', action='store_true', help='Load data into the MovR database')
    parser.add_argument('--reload-tables', dest='reload_tables', action='store_true',
                        help='Drop and reload MovR tables. Use with --load')
    args = parser.parse_args()

    movr = MovR(args.conn_string, reload_tables=args.reload_tables)

    print "connected to movr database @ %s" % args.conn_string

    cities = ['new york'] if args.city == None else args.city

    if args.num_users <= 0 or args.num_rides <= 0 or args.num_vehicles <= 0:
        print "The number of objects to generate must be > 0"
        sys.exit(1)

    if args.reload_tables or args.load:
        print "loading movr data with %d cities, %d users, %d vehicles, and %d rides" % \
              (len(MOVR_CITIES), args.num_users, args.num_vehicles, args.num_rides)
        load_movr_data(movr, args.num_users, args.num_vehicles, args.num_rides, MOVR_CITIES)

    else:
        print "simulating movr load"
        simulate_movr_load(movr)



