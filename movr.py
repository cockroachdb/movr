from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cockroachdb.sqlalchemy import run_transaction
from faker import Faker
from models import Base, User, Vehicle, Ride
from generators import MovRGenerator
import datetime
import random
import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

# @todo: fake data should ideally be separated from MovR the class

class MovR:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    def __init__(self, conn_string, init_tables = False, echo = False):


        self.engine = create_engine(conn_string, convert_unicode=True, echo=echo)


        if init_tables:
            logging.info("initializing tables")
            Base.metadata.drop_all(bind=self.engine)
            Base.metadata.create_all(bind=self.engine)
            logging.debug("tables dropped and created")

        self.session = sessionmaker(bind=self.engine)()

        MovR.fake = Faker()


    ##################
    # MAIN MOVR API
    #################

    # setup geo-partitioning if this is an enterprise cluster
    def add_geo_partitioning(self, partition_map):
        logging.debug("Partitioning database with : %s", partition_map)
        partition_string = ""
        first_region = True
        for region in partition_map:
            partition_string += "PARTITION " + region + " VALUES IN (" if first_region \
                else ", PARTITION " + region + " VALUES IN ("
            first_region = False
            first_city = True
            for city in partition_map[region]:
                partition_string += "'" + city + "' " if first_city else ", '" + city + "'"
                first_city = False
            partition_string += ")"

        for table in ["vehicles", "users", "rides"]:
            logging.debug("Partitioning table: %s", table)
            partition_sql = "ALTER TABLE " + table + " PARTITION BY LIST (city) (" + partition_string + ")"
            self.session.execute(partition_sql)

        self.session.commit()

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
        return run_transaction(sessionmaker(bind=self.engine), lambda session: self.get_users_helper(session, city, limit))

    def get_vehicles(self, city, limit=None):
        return run_transaction(sessionmaker(bind=self.engine), lambda session: self.get_vehicles_helper(session, city, limit))

    def get_active_rides(self, limit=None):
        return run_transaction(sessionmaker(bind=self.engine), lambda session: self.get_active_rides_helper(session, limit))


    ############
    # UTILITIES AND HELPERS
    ############

    # @todo: get by city
    def get_active_rides_helper(self, session, limit=None):
        rides = self.session.query(Ride).filter_by(end_time=None).limit(limit).all()
        return map(lambda ride: {'city': ride.city, 'id': ride.id}, rides)

    def get_users_helper(self, session, city, limit):
        users = session.query(User).filter_by(city=city).limit(limit).all()
        return map(lambda user: {'city': user.city, 'id': user.id}, users)

    def get_vehicles_helper(self, session, city, limit=None):
        vehicles = self.session.query(Vehicle).filter_by(city=city).limit(limit).all()
        return map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles)

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

    def add_rides(self, num_rides, city):
        chunk_size = 100

        def add_rides_helper(sess, chunk, n):
            users = sess.query(User).filter_by(city=city).all()
            vehicles = sess.query(Vehicle).filter_by(city=city).all()

            rides = []
            for i in range(chunk, min(chunk + chunk_size, num_rides)):
                start_time = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
                rides.append(Ride(id=MovRGenerator.generate_uuid(),
                                  city=city,
                                  vehicle_city=city,
                                  rider_id=random.choice(users).id,
                                  vehicle_id=random.choice(vehicles).id,
                                  start_time=start_time,
                                  start_address=MovR.fake.address(),
                                  end_address=MovR.fake.address(),
                                  revenue=MovRGenerator.generate_revenue(),
                                  end_time=start_time + datetime.timedelta(minutes=random.randint(0, 60))))
            sess.bulk_save_objects(rides)

        for chunk in range(0, num_rides, chunk_size):
            run_transaction(sessionmaker(bind=self.engine),
                            lambda s: add_rides_helper(s, chunk, min(chunk + chunk_size, num_rides)))

    def add_users(self, num_users, city):
        chunk_size = 1000

        def add_users_helper(sess, chunk, n):
            users = []
            for i in range(chunk, n):
                users.append(User(id=MovRGenerator.generate_uuid(),
                                  city=city,
                                  name=MovR.fake.name(),
                                  address=MovR.fake.address(),
                                  credit_card=MovR.fake.credit_card_number()))
            sess.bulk_save_objects(users)

        for chunk in range(0, num_users, chunk_size):
            run_transaction(sessionmaker(bind=self.engine),
                            lambda s: add_users_helper(s, chunk, min(chunk + chunk_size, num_users)))

    def add_vehicles(self, num_vehicles, city):
        chunk_size = 1000

        def add_vehicles_helper(sess, chunk, n):
            owners = sess.query(User).filter_by(city=city).all()
            vehicles = []
            for i in range(chunk, n):
                vehicle_type = MovRGenerator.generate_random_vehicle()
                vehicles.append(Vehicle(id=MovRGenerator.generate_uuid(),
                                        type=vehicle_type,
                                        city=city,
                                        owner_id=(random.choice(owners)).id,
                                        status=MovRGenerator.get_vehicle_availability(),
                                        ext=MovRGenerator.generate_vehicle_metadata(vehicle_type)))
            sess.bulk_save_objects(vehicles)

        for chunk in range(0, num_vehicles, chunk_size):
            run_transaction(sessionmaker(bind=self.engine),
                            lambda s: add_vehicles_helper(s, chunk, min(chunk + chunk_size, num_vehicles)))
