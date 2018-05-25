#!/usr/bin/python

import argparse

from movr import MovR
import random
import functools

parser = argparse.ArgumentParser(description='Create some load for MovR.')
parser.add_argument('--url', dest='conn_string', default='cockroachdb://root@localhost:26257/movr?sslmode=disable',
                    help="must include database name in url.")
parser.add_argument('--iterations', dest='iterations', type=int, default=0)
parser.add_argument('--users', dest='users', type=int, default=50)
parser.add_argument('--city', dest='city', action='append', default=[])
parser.add_argument('--load', dest='load', action='store_true')
parser.add_argument('--drop', dest='drop', action='store_true', help="drop and reload MovR tables")

args = parser.parse_args()

movr = MovR(args.conn_string, drop = args.drop)


if args.load:
    #@todo: create database if it doesnt exist

    # create users and inventory
    user_ids = []
    vehicle_ids = []
    for x in range(0,args.users):
        user_id = movr.add_user()
        user_ids.append(user_id)
        if random.random() < .1: #10% of users are on the supply side
            owned_vehicles = random.randint(1,5)
            for i in range(owned_vehicles):
                vehicle_ids.append(movr.add_vehicle(user_id, random.choice(args.city)))


    # create rides
    for x in range(0,args.users*10):
        movr.add_ride(random.choice(user_ids), random.choice(vehicle_ids))



    print "added %d users" % args.iterations
else:
    users = movr.get_users()
    vehicles = movr.get_vehicles()

    if args.iterations == 0:
        while True:
            movr.start_ride(random.choice(users).id, random.choice(vehicles).id)



