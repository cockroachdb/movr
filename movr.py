from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
import traceback
from faker import Faker
import psycopg2
from models import Base, User, Vehicle, Ride
from generators import MovRGenerator
import datetime
import random
import time
import math

class MovR:

    def __init__(self, conn_string, partition_map, enable_geo_partitioning = False, reload_tables = False,
                 echo = False, exponential_txn_backoff = False):

        engine = create_engine(conn_string, convert_unicode=True, echo=echo)

        if reload_tables:
            Base.metadata.drop_all(bind=engine)

        Base.metadata.create_all(bind=engine)

        self.session = sessionmaker(bind=engine)()
        self.exponential_txn_backoff = exponential_txn_backoff

        MovR.fake = Faker()

        #setup geo-partitioning if this is an enterprise cluster
        if enable_geo_partitioning:
            partition_string = ""
            first_region = True
            for region in partition_map:
                partition_string += "PARTITION " + region + " VALUES IN (" if first_region \
                    else ", PARTITION " + region + " VALUES IN ("
                first_region = False
                first_city = True
                for city in partition_map[region]:
                    partition_string+="'" + city + "' " if first_city else ", '" + city + "'"
                    first_city = False
                partition_string += ")"

            for table in ["vehicles", "users", "rides"]:
                partition_sql = "ALTER TABLE "+ table + " PARTITION BY LIST (city) (" + partition_string + ")"
                self.session.execute(partition_sql)
                #@todo: add error handling



    def run_transaction(self, transaction):
        # we can't use the savepoint approach due to the way sqlalchemy handles transactions

        attempt_number = 0
        while True:
            try:
                ret = transaction()
                self.session.commit() #@todo: without this I get sqlalchemy.exc.InternalError: (psycopg2.InternalError) current transaction is committed, commands ignored until end of transaction block
                return ret
            except exc.OperationalError as e:

                if type(e.orig) != psycopg2.extensions.TransactionRollbackError:
                    print "caught non-retryable error: %s" % type(e.orig)
                    raise e

                print "retrynig txn. attempt #%d" % attempt_number
                self.session.rollback()
                if self.exponential_txn_backoff:
                    time.sleep(.001 * math.pow(2,attempt_number))
                attempt_number+=1



    def start_ride_helper(self, city, rider_id, vehicle_id):
        #@todo: fake data should ideally be completely separated from MovR the class
        r = Ride(city=city, vehicle_city=city, id=MovRGenerator.generate_uuid(),
                 rider_id=rider_id, vehicle_id=vehicle_id,
                 start_address=MovR.fake.address())  # @todo: this should be the address of the vehicle
        self.session.add(r)
        v = self.session.query(Vehicle).filter_by(city=city, id=vehicle_id).first()
        v.status = "in_use"

        return r

    def start_ride(self, city, rider_id, vehicle_id):
        return self.run_transaction(lambda: self.start_ride_helper(city, rider_id, vehicle_id)) #@todo: ask ben how this works. seems like a closure in js


    def end_ride_helper(self, city, ride_id):
        ride = self.session.query(Ride).filter_by(city=city, id=ride_id).first()
        ride.end_address = MovR.fake.address()  # @todo: this should update the address of the vehicle
        ride.revenue = MovRGenerator.generate_revenue()
        ride.end_time = datetime.datetime.now()
        v = self.session.query(Vehicle).filter_by(city=city, id=ride.vehicle_id).first()
        v.status = "available"

    def end_ride(self, city, ride_id):
        self.run_transaction(lambda: self.end_ride_helper(city, ride_id))


    def add_user_helper(self, city):
        u = User(city=city, id=MovRGenerator.generate_uuid(), name=MovR.fake.name(),
                 address=MovR.fake.address(), credit_card=MovR.fake.credit_card_number())
        self.session.add(u)
        return u

    def add_user(self, city):
        return self.run_transaction(lambda: self.add_user_helper(city))


    def add_rides(self, num_rides, city):
        chunk_size = 100

        users = self.session.query(User).filter_by(city=city).all()
        vehicles = self.session.query(Vehicle).filter_by(city=city).all()

        for chunk in range(0, num_rides, chunk_size):
            rides = []
            for i in range(chunk, min(chunk + chunk_size, num_rides)):
                start_time = datetime.datetime.now() - datetime.timedelta(days = random.randint(0,30))
                rides.append(Ride(id = MovRGenerator.generate_uuid(), city = city, vehicle_city = city,
                 rider_id=random.choice(users).id, vehicle_id=random.choice(vehicles).id,
                 start_time = start_time,
                 start_address = MovR.fake.address(),
                 end_address = MovR.fake.address(),
                 revenue = MovRGenerator.generate_revenue(),
                 end_time=start_time + datetime.timedelta(minutes = random.randint(0,60))))
            self.session.bulk_save_objects(rides)
            self.session.commit()

    def add_users(self, num_users, city):
        chunk_size = 1000
        for chunk in range(0, num_users, chunk_size):
            users = []
            for i in range(chunk, min(chunk + chunk_size, num_users)):
                users.append(User(id = MovRGenerator.generate_uuid(), city = city,
                                  name = MovR.fake.name(),
                      address = MovR.fake.address(), credit_card = MovR.fake.credit_card_number()))
            self.session.bulk_save_objects(users)
            self.session.commit()

    def add_vehicles(self, num_vehicles, city):
        owners = self.session.query(User).filter_by(city=city).all()
        chunk_size = 1000
        for chunk in range(0, num_vehicles, chunk_size):
            vehicles = []
            for i in range(chunk, min(chunk + chunk_size, num_vehicles)):
                vehicle_type = MovRGenerator.generate_random_vehicle()
                vehicles.append(Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                          city=city, owner_id=(random.choice(owners)).id, status=MovRGenerator.get_vehicle_availability(),
                          ext=MovRGenerator.generate_vehicle_metadata(vehicle_type)))
            self.session.bulk_save_objects(vehicles)
            self.session.commit()

    def get_users(self, city, limit=None):
        return self.session.query(User).filter_by(city=city).limit(limit).all()

    def get_vehicles(self, city, limit=None):
        return self.session.query(Vehicle).filter_by(city=city).limit(limit).all()

    def get_active_rides(self, limit=None):
        return self.session.query(Ride).filter_by(end_time = None).limit(limit).all()

    def add_vehicle_helper(self, city, user_id):
        vehicle_type = MovRGenerator.generate_random_vehicle()

        vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                          city=city, owner_id=user_id, status=MovRGenerator.get_vehicle_availability(),
                          ext=MovRGenerator.generate_vehicle_metadata(vehicle_type))

        self.session.add(vehicle)

        return vehicle

    def add_vehicle(self, city, user_id):
        return self.run_transaction(lambda: self.add_vehicle_helper(city, user_id))





