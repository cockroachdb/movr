#!/usr/bin/python

import argparse

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, String, DateTime, Integer, Float, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import sessionmaker

import datetime
import uuid
import random
import functools
import traceback
from faker import Faker
fake = Faker()




parser = argparse.ArgumentParser(description='Create some load for MovR.')

parser.add_argument('--url', dest='conn_string', default='cockroachdb://root@localhost:26257/movr?sslmode=disable',
                    help="must include database name in url.")
parser.add_argument('--iterations', dest='iterations', type=int, default=100)
parser.add_argument('--city', dest='city', action='append', default=[])
parser.add_argument('--load', dest='load', action='store_true')
parser.add_argument('--kv-mode', dest='kv_mode', action='store_true', help="limit actions to kv lookups")


args = parser.parse_args()
print args.city
Base = declarative_base()


engine = create_engine(args.conn_string, use_batch_mode=True)
#engine = create_engine(args.conn_string, use_batch_mode=True, echo=True )


Session = sessionmaker(bind=engine)

#@todo: how to do this in the database?
def generate_uuid():
    return str(uuid.uuid4())

def generate_revenue():
    return random.uniform(1,100)


def weighted_choice(items):
    """items is a list of tuples in the form (item, weight)"""
    weight_total = sum((item[1] for item in items))
    n = random.uniform(0, weight_total)
    for item, weight in items:
        if n < weight:
            return item
        n = n - weight
    return item

#@todo: how to add inverted index from sql alchemy

class User(Base):
    __tablename__ = 'users'
    id = Column(UUID, default=generate_uuid)
    name = Column(String, default=fake.name)
    address = Column(String, default=fake.address)
    credit_card = Column(String, default=fake.credit_card_number)
    PrimaryKeyConstraint(id)


class Ride(Base):
    __tablename__ = 'rides'
    id = Column(UUID, default=generate_uuid)
    rider_id = Column(UUID)
    vehicle_id = Column(UUID)
    start_address = Column(String, default=fake.address)
    end_address = Column(String, default=fake.address)
    revenue = Column(Float, default=generate_revenue )
    PrimaryKeyConstraint(id)

class Vehicle(Base):
    __tablename__ = 'vehicles'
    id = Column(UUID, default=generate_uuid)
    type = Column(String)
    city = Column(String)
    owner_id = Column(UUID)
    creation_time = Column(DateTime, default=datetime.datetime.now)
    status = Column(String)
    ext = Column(JSONB) #this isnt decoding properly
    PrimaryKeyConstraint(city, id)
    __table_args__ = (Index('ix_vehicle_type', type),)
    #@todo: FK on owner ID on delete cascade
    #__table_args__ = (Index('ix_vehicle_ext', ext, postgresql_using="gin"), )

Base.metadata.create_all(engine)

def generate_random_vehicle():
    return random.choice(['skateboard', 'bike', 'scooter'])

def generate_random_color():
    return random.choice(['red', 'yellow', 'blue', 'green', 'black'])

def gen_bike_brand():
    return random.choice(['Merida','Fuji'
    'Cervelo', 'Pinarello',
    'Santa Cruz', 'Kona', 'Schwinn'])

def generate_vehicle_metadata(type):
    metadata = {}
    metadata['color'] = generate_random_color()
    if type == 'bike':
        metadata['brand'] = gen_bike_brand()
    return metadata

def get_vehicle_availability():
    return weighted_choice([("available", .4), ("in_use", .55), ("lost", .05)])

def find_vehicle_from_keys(keys):
    find_vehicle_by_id(random.choice(keys))

def find_vehicle_by_id(id):
    session = Session()
    try:
        return session.query(Vehicle).filter_by(city=id[0], id=id[1]).first()
    except:
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

def update_vehicle_from_keys(keys):
    update_vehicle_by_id(random.choice(keys))

def update_vehicle_by_id(id):
    session = Session()
    try:
        session.query(Vehicle).filter_by(city=id[0], id=id[1]).update({"status": random.choice(["available",
                                                                                   "in_use", "lost"])})
        session.commit()
    except:
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

def find_and_select_vehicle():
    #print "finding and selecting vehicle" #@todo: add log level
    session = Session()
    try:
        # @todo: grab a random user
        # @todo: create transactions where you find a ride and claim it if its available. Should be 10%
        v = session.query(Vehicle).filter_by(city=random.choice(args.city), type="bike", status="available").first()
        if v:
            v.status = 'in_use'
            session.commit()
        else:
            print "no vehicles were available to claim"

            # @todo: create transactions where people periodically return checked out items. Should be 10%

            # @todo: create load where people are just seeing whats available (ie home age load). Should be 80%
    except:
        session.rollback()
    finally:
        session.close()

def returning_vehicle():

    session = Session()
    try:
        v = session.query(Vehicle).filter_by(city=random.choice(args.city), status="in_use").first()
        if v:
            v.status = 'available'
            session.commit()
        else:
            print "no vehicles were available to claim"

            # @todo: create transactions where people periodically return checked out items. Should be 10%

            # @todo: create load where people are just seeing whats available (ie home age load). Should be 80%
    except:
        session.rollback()
    finally:
        session.close()

def browse_vehicles():

    session = Session()
    try:
        session.query(Vehicle).filter_by(city=random.choice(args.city), status="available").limit(25).all()
    except:
        session.rollback()
    finally:
        session.close()


def add_vehicle_helper(session, user):
    vehicle_type = generate_random_vehicle()
    ext = generate_vehicle_metadata(vehicle_type)
    vehicle = Vehicle(type=vehicle_type, city=random.choice(args.city), owner_id=user.id,
                        status=get_vehicle_availability(), ext=ext)
    session.add(vehicle)




def add_vehicle(session, user):
    try:
        add_vehicle_helper(session, user)
        session.commit()
    except:
        traceback.print_exc()
        session.rollback()


def get_ids_for_vehicles():
    session = Session()
    try:
        return session.query(Vehicle.id).all()
    except :
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

def get_keys_for_cities():
    session = Session()
    try:
        return session.query(Vehicle.city, Vehicle.id).filter(Vehicle.city.in_(args.city)).all()
    except :
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

def simulate_action(keys):
    if args.kv_mode:
        action = weighted_choice([(functools.partial(find_vehicle_from_keys, keys), .95),
                                  (functools.partial(update_vehicle_from_keys, keys), .05)])
    else:
        action = weighted_choice([(find_and_select_vehicle, .1),
                                  (browse_vehicles, .7), (returning_vehicle, .1),
                                  (add_vehicle, .1)])

    action()

if args.load:
    #@todo: create database if it doesnt exist
    session = Session()

    # create users and inventory
    user_ids = []
    for x in range(0,args.iterations):
        u = User()
        session.add(u)
        session.commit()
        user_ids.append(u.id)
        if random.random() < .1: #10% of users are on the supply side
            owned_vehicles = random.randint(1,5)
            for i in range(owned_vehicles):
                add_vehicle(session, u)
                #print v.id
                #vehicle_ids.append(v.id)



    # create rides
    vehicle_ids = get_ids_for_vehicles()

    for x in range(0,1000):
        session.add(Ride(rider_id=random.choice(user_ids), vehicle_id=random.choice(vehicle_ids)[0]))

    session.commit()
    session.close()



    # print "added %d users" % args.iterations
else:
    keys = get_keys_for_cities()

    if args.iterations == 0:
        while True:
            simulate_action(keys)

    else:
        for _ in range(args.iterations):
            simulate_action(keys)



