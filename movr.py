from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cockroachdb.sqlalchemy import run_transaction
from faker import Faker
from models import Base, User, Vehicle, Ride
from generators import MovRGenerator
import datetime
import random

# @todo: fake data should ideally be separated from MovR the class

class MovR:

    def __init__(self, conn_string, partition_map, enable_geo_partitioning = False, reload_tables = False,
                 echo = False):

        self.engine = create_engine(conn_string, convert_unicode=True, echo=echo)

        if reload_tables:
            Base.metadata.drop_all(bind=self.engine)

        Base.metadata.create_all(bind=self.engine)

        self.session = sessionmaker(bind=self.engine)()

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

    ##################
    # MAIN MOVR API
    #################
    def start_ride(self, city, rider_id, vehicle_id):
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: self.start_ride_helper(session, city, rider_id, vehicle_id))

    def end_ride(self, city, ride_id):
        run_transaction(sessionmaker(bind=self.engine), lambda session: self.end_ride_helper(session, city, ride_id))

    def add_user(self, city):
        return run_transaction(sessionmaker(bind=self.engine), lambda session: self.add_user_helper(session, city))

    def add_vehicle(self, city, user_id):
        return run_transaction(sessionmaker(bind=self.engine), lambda session: self.add_vehicle_helper(session, city, user_id))

    def get_users(self, city, limit=None):
        users = self.session.query(User).filter_by(city=city).limit(limit).all()
        return map(lambda user: {'city': user.city, 'id': user.id}, users)

    def get_vehicles(self, city, limit=None):
        vehicles = self.session.query(Vehicle).filter_by(city=city).limit(limit).all()
        return map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles)

    def get_active_rides(self, limit=None):
        rides = self.session.query(Ride).filter_by(end_time = None).limit(limit).all()
        return map(lambda ride: {'city': ride.city, 'id': ride.id}, rides)


    ############
    # UTILITIES AND HELPERS
    ############

    def add_vehicle_helper(self, session, city, user_id):
        vehicle_type = MovRGenerator.generate_random_vehicle()

        vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                          city=city, owner_id=user_id, status=MovRGenerator.get_vehicle_availability(),
                          ext=MovRGenerator.generate_vehicle_metadata(vehicle_type))

        session.add(vehicle)
        return {'city': vehicle.city, 'id': vehicle.id}

    def add_user_helper(self, session, city):
        u = User(city=city, id=MovRGenerator.generate_uuid(), name=MovR.fake.name(),
                 address=MovR.fake.address(), credit_card=MovR.fake.credit_card_number())
        session.add(u)
        return {'city': u.city, 'id': u.id}

    def end_ride_helper(self, session, city, ride_id):
        ride = session.query(Ride).filter_by(city=city, id=ride_id).first()
        ride.end_address = MovR.fake.address()
        ride.revenue = MovRGenerator.generate_revenue()
        ride.end_time = datetime.datetime.now()
        v = session.query(Vehicle).filter_by(city=city, id=ride.vehicle_id).first()
        v.status = "available"

    def start_ride_helper(self, session, city, rider_id, vehicle_id):
        r = Ride(city=city, vehicle_city=city, id=MovRGenerator.generate_uuid(),
                 rider_id=rider_id, vehicle_id=vehicle_id,
                 start_address=MovR.fake.address())  # @todo: this should be the address of the vehicle
        session.add(r)
        v = session.query(Vehicle).filter_by(city=city, id=vehicle_id).first()
        v.status = "in_use"
        return {'city': r.city, 'id': r.id}

    ##############
    # BULK DATA LOADING
    ##############
    #@todo: how does this work with transaction retires? bulk_save_objects produces `SAVEPOINT not supported except for COCKROACH_RESTART`
    def add_rides(self, num_rides, city):
        chunk_size = 50000

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








