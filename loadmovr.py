#!/usr/bin/python

import argparse
from movr import MovR
import random
import sys
import time

def load_movr_data(movr, num_users, num_vehicles, num_rides, cities):
    user_ids = []
    vehicle_ids = []

    # add users
    start_time = time.time()
    movr.add_users(num_users)
    print "added %d users in %f seconds (%f users/second)" % \
          (num_users,  time.time() - start_time, num_users / float(time.time() - start_time))

    # add vehicles
    start_time = time.time()
    movr.add_vehicles(num_vehicles, cities)
    print "added %d vehicles in %f seconds (%f vehicles/second)" % \
          (num_vehicles, time.time() - start_time, num_vehicles / float(time.time() - start_time))

    start_time = time.time()
    movr.add_rides(num_rides)
    print "added %d rides in %f seconds (%f rides/second)" % \
          (num_rides, time.time() - start_time, num_rides / float(time.time() - start_time))

    return



    # # add rides
    # for x in range(0, num_users_to_load * 10):
    #     if x % 25 == 0:
    #         print "added %d/%d rides" % (x, num_users_to_load * 10)
    #     rider = random.choice(user_ids)
    #     vehicle = random.choice(vehicle_ids)
    #     ride = movr.start_ride(rider, vehicle)
    #     if random.random() < .99:
    #         movr.end_ride(ride.id)
    #
    # print "added %d users, %d vehicles, and %d rides" % (num_users_to_load, len(vehicle_ids), num_users_to_load * 10)

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
    parser = argparse.ArgumentParser(description='CLI for MovR.')
    parser.add_argument('--url', dest='conn_string', default='cockroachdb://root@localhost:26257/movr?sslmode=disable',
                        help="connection string to movr database.")
    parser.add_argument('--num-users', dest='num_users', type=int, default=50)
    parser.add_argument('--num-vehicles', dest='num_vehicles', type=int, default=10)
    parser.add_argument('--num-rides', dest='num_rides', type=int, default=500)
    parser.add_argument('--city', dest='city', action='append',
                        help='The names of the cities in which to place vehicles')
    parser.add_argument('--load', dest='load', action='store_true', help='Load data into the MovR database')
    parser.add_argument('--reload-tables', dest='reload_tables', action='store_true',
                        help='Drop and reload MovR tables. Use with --load')
    args = parser.parse_args()

    movr = MovR(args.conn_string, reload_tables=args.reload_tables)

    print "connected to movr database @ %s" % args.conn_string

    cities = ['new york'] if args.city == None else args.city

    if args.reload_tables or args.load:
        print "loading movr data"
        load_movr_data(movr, args.num_users, args.num_vehicles, args.num_rides, cities)

    else:
        print "simulating movr load"
        simulate_movr_load(movr)



