from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import traceback
from faker import Faker
from models import Base, User, Vehicle, Ride
from generators import MovRGenerator
import datetime
import random

#@todo: add query retries

class MovR:

    def __init__(self, conn_string, partition_map, is_enterprise = False, reload_tables = False):
        engine = create_engine(conn_string, convert_unicode=True)

        if reload_tables:
            Base.metadata.drop_all(bind=engine)

        Base.metadata.create_all(bind=engine)

        self.session = sessionmaker(bind=engine)()

        MovR.fake = Faker()

        #setup geo-partitioning if this is an enterprise cluster
        if is_enterprise:
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



    def start_ride(self, city, rider_id, vehicle_id):
        r = Ride(city = city, vehicle_city = city, id = MovRGenerator.generate_uuid(),
                 rider_id=rider_id, vehicle_id=vehicle_id,
                 start_address = MovR.fake.address()) #@todo: this should be the address of the vehicle
        self.session.add(r)
        self.session.query(Vehicle).filter_by(city=city, id=vehicle_id).update({"status": "in_use"})
        self.session.commit()
        return r

    def end_ride(self, city, ride_id):
        ride = self.session.query(Ride).filter_by(city = city, id=ride_id).first()
        ride.end_address = MovR.fake.address() #@todo: this should update the address of the vehicle
        ride.revenue = MovRGenerator.generate_revenue()
        ride.end_time = datetime.datetime.now()
        self.session.query(Vehicle).filter_by(city = city, id = ride.vehicle_id).update({"status": "available"})
        self.session.commit()

    def add_user(self, city):
        u = User(city=city, id = MovRGenerator.generate_uuid(), name = MovR.fake.name(),
                      address = MovR.fake.address(), credit_card = MovR.fake.credit_card_number())
        self.session.add(u)
        self.session.commit()
        return u


    def add_rides(self, num_rides, city):
        chunk_size = 10000

        users = self.session.query(User).filter_by(city=city).all()
        vehicles = self.session.query(Vehicle).filter_by(city=city).all()

        for chunk in range(0, num_rides, chunk_size):
            rides = []
            for i in range(chunk, min(chunk + chunk_size, num_rides)):
                rides.append(Ride(id = MovRGenerator.generate_uuid(), city = city, vehicle_city = city,
                 rider_id=random.choice(users).id, vehicle_id=random.choice(vehicles).id,
                 start_address = MovR.fake.address(),
                 end_address = MovR.fake.address(),
                 revenue = MovRGenerator.generate_revenue(),
                 end_time=datetime.datetime.now()))
            self.session.bulk_save_objects(rides)
            self.session.commit()

    def add_users(self, num_users, city):
        chunk_size = 50000
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
        chunk_size = 50000
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

    def add_vehicle(self, city, user_id):
        vehicle_type = MovRGenerator.generate_random_vehicle()

        vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                          city=city, owner_id=user_id, status=MovRGenerator.get_vehicle_availability(),
                          ext=MovRGenerator.generate_vehicle_metadata(vehicle_type))

        self.session.add(vehicle)
        self.session.commit()

        return vehicle





