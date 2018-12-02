from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, User, Vehicle, Ride

from cockroachdb.sqlalchemy import run_transaction
from generators import MovRGenerator

import datetime, logging

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

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

    ##################
    # MAIN MOVR API
    #################

    def start_ride(self, city, rider_id, vehicle_id, address):

        def start_ride_helper(session, city, rider_id, vehicle_id, address):
            r = Ride(city=city, vehicle_city=city, id=MovRGenerator.generate_uuid(),
                     rider_id=rider_id, vehicle_id=vehicle_id,
                     start_address=address)  # @todo: this should be the address of the vehicle
            session.add(r)
            v = session.query(Vehicle).filter_by(city=city, id=vehicle_id).first()
            v.status = "in_use"
            return {'city': r.city, 'id': r.id}

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: start_ride_helper(session, city, rider_id, vehicle_id, address))

    def end_ride(self, city, ride_id, address):
        def end_ride_helper(session, city, ride_id, address):
            ride = session.query(Ride).filter_by(city=city, id=ride_id).first()
            ride.end_address = address
            ride.revenue = MovRGenerator.generate_revenue()
            ride.end_time = datetime.datetime.now()
            v = session.query(Vehicle).filter_by(city=city, id=ride.vehicle_id).first()
            v.status = "available"

        run_transaction(sessionmaker(bind=self.engine), lambda session: end_ride_helper(session, city, ride_id, address))

    def add_user(self, city, name, address, credit_card_number):
        def add_user_helper(session, city, name, address, credit_card_number):
            u = User(city=city, id=MovRGenerator.generate_uuid(), name=name,
                     address=address, credit_card=credit_card_number)
            session.add(u)
            return {'city': u.city, 'id': u.id}
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_user_helper(session, city, name, address, credit_card_number))

    def add_vehicle(self, city, user_id):
        def add_vehicle_helper(session, city, user_id):
            vehicle_type = MovRGenerator.generate_random_vehicle()

            vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                              city=city, owner_id=user_id, status=MovRGenerator.get_vehicle_availability(),
                              ext=MovRGenerator.generate_vehicle_metadata(vehicle_type))

            session.add(vehicle)
            return {'city': vehicle.city, 'id': vehicle.id}
        return run_transaction(sessionmaker(bind=self.engine), lambda session: add_vehicle_helper(session, city, user_id))

    def get_users(self, city, limit=None):
        def get_users_helper(session, city, limit=None):
            users = session.query(User).filter_by(city=city).limit(limit).all()
            return list(map(lambda user: {'city': user.city, 'id': user.id}, users))
        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_users_helper(session, city, limit))

    def get_vehicles(self, city, limit=None):
        def get_vehicles_helper(session, city, limit=None):
            vehicles = session.query(Vehicle).filter_by(city=city).limit(limit).all()
            return list(map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles))

        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_vehicles_helper(session, city, limit))

    def get_active_rides(self, limit=None):

        def get_active_rides_helper(session, limit=None):
            rides = session.query(Ride).filter_by(end_time=None).limit(limit).all()
            return list(map(lambda ride: {'city': ride.city, 'id': ride.id}, rides))

        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_active_rides_helper(session, limit))


    ############
    # UTILITIES AND HELPERS
    ############


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






