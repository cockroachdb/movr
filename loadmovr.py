#!/usr/bin/python

import argparse
from movr import MovR
import random

parser = argparse.ArgumentParser(description='CLI for MovR.')
parser.add_argument('--url', dest='conn_string', default='cockroachdb://root@localhost:26257/movr?sslmode=disable',
                    help="connection string to movr database.")
parser.add_argument('--users', dest='users', type=int, default=50)
parser.add_argument('--city', dest='city', action='append', help='The names of the cities in which to place vehicles')
parser.add_argument('--load', dest='load', action='store_true', help='Load data into the MovR database')
parser.add_argument('--reload-tables', dest='reload_tables', action='store_true', help='Drop and reload MovR tables. Use with --load')
args = parser.parse_args()

movr = MovR(args.conn_string, reload_tables = args.reload_tables)
cities = ['new york'] if args.city == None else args.city

if args.reload_tables or args.load:
    print "loading movr database @ %s" % args.conn_string

    user_ids = []
    vehicle_ids = []

    #add users and inventory
    for x in range(0,args.users):
        user = movr.add_user()
        user_ids.append(user.id)
        if random.random() < .1: #10% of users are on the supply side
            owned_vehicles = random.randint(1,5)
            for i in range(owned_vehicles):
                vehicle = movr.add_vehicle(user.id, random.choice(cities))
                vehicle_ids.append(vehicle.id)


    #add rides
    for x in range(0,args.users*10):
        rider = random.choice(user_ids)
        vehicle = random.choice(vehicle_ids)
        ride = movr.start_ride(rider, vehicle)
        if random.random() < .99:
            movr.end_ride(ride.id)

    print "added %d users, %d vehicles, and %d rides" % (args.users, len(vehicle_ids), args.users*10)

else:
    print "simulating load for the movr database @ %s" % args.conn_string

    users = movr.get_users()
    vehicles = movr.get_vehicles()

    active_ride_ids = set(map(lambda x: x.id, movr.get_active_rides()))
    print active_ride_ids

    while True:
        if random.random() < .1:
            ride = movr.start_ride(random.choice(users).id, random.choice(vehicles).id)
            active_ride_ids.add(ride.id)
        else:
            if len(active_ride_ids):
                ride_id = active_ride_ids.pop() #pick arbitraty ride to end
                movr.end_ride(ride_id)





