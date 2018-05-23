#!/usr/bin/python

# demo user stories
# I want to see the status of my app: select type, status, count(*) from vehicles group by type, status order by type, status;
# I want to find all bikes near me: select * from vehicles where city='new york' and type='bike' and status='available';
# Feature request: I want to search bikes by model;
# I want to find all schwinn bikes near me: select * from vehicles where city='new york' and type='bike' and status='available' and ext @> '{"model":"Schwinn"}'

import argparse

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, String, DateTime, Integer, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import sessionmaker

import datetime
import uuid
import random
import functools
import traceback


parser = argparse.ArgumentParser(description='Create some load for MovR.')

parser.add_argument('--url', dest='conn_string', required=True, help="must include database name in url.")
parser.add_argument('--version', dest='version', type=float, default=1.0, help="version of the ride sharing app.")
parser.add_argument('--iterations', dest='iterations', type=int, default=0)
parser.add_argument('--city', dest='city', action='append', required=True)
parser.add_argument('--load', dest='load', action='store_true')
parser.add_argument('--kv-mode', dest='kv_mode', action='store_true', help="limit actions to kv lookups")


args = parser.parse_args()
print "running version %f" % args.version
print args.city
Base = declarative_base()

# http://docs.sqlalchemy.org/en/rel_0_9/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSON
# @todo: NOT SURE WHY WE NEED THIS.
def jsonb_deserializer(doc):
    return doc

engine = create_engine(args.conn_string, use_batch_mode=True, json_deserializer=jsonb_deserializer)
#engine = create_engine(args.conn_string, use_batch_mode=True, echo=True )


Session = sessionmaker(bind=engine)

#@todo: how to do this in the database?
def generate_uuid():
    return str(uuid.uuid4())


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
    name = Column(String)
    credit_card = Column(Integer)
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


def add_vehicle_helper(session):
    vehicle_type = generate_random_vehicle()
    ext = generate_vehicle_metadata(vehicle_type) if args.version >= 1.1 else {}
    session.add(Vehicle(type=vehicle_type, city=random.choice(args.city), owner_id=str(uuid.uuid4()),
                        status=get_vehicle_availability(), ext=ext))


def add_user_helper(session):
    session.add(User(name="Nate Stewart", credit_card=1234))

def add_vehicles():

    session = Session()
    try:
        add_vehicle_helper(session)
        session.commit()
    except:
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
                                  (add_vehicles, .1)])

    action()

if args.load:
    #@todo: create database if it doesnt exist
    session = Session()
    add_user_helper(session)
    # for _ in range(args.iterations):
    #     add_vehicle_helper(session)

    session.commit()

    # print "added %d vehicles" % args.iterations
else:
    keys = get_keys_for_cities()

    if args.iterations == 0:
        while True:
            simulate_action(keys)

    else:
        for _ in range(args.iterations):
            simulate_action(keys)



