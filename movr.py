from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, User, Vehicle, Ride, VehicleLocationHistory, PromoCode, UserPromoCode

from cockroachdb.sqlalchemy import run_transaction
from generators import MovRGenerator

import datetime, logging

class MovR:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    def __init__(self, conn_string, init_tables = False, multi_region = False, echo = False):

        self.multi_region = multi_region
        self.engine = create_engine(conn_string, convert_unicode=True, echo=echo)


        if init_tables:
            logging.info("initializing tables")
            Base.metadata.drop_all(bind=self.engine)
            Base.metadata.create_all(bind=self.engine)
            if multi_region:
                self.run_multi_region_transformations()


            logging.debug("tables dropped and created")

        self.session = sessionmaker(bind=self.engine)()

    ##################
    # MAIN MOVR API
    #################

    def start_ride(self, city, rider_id, vehicle_id):

        def start_ride_helper(session, city, rider_id, vehicle_id):
            vehicle = session.query(Vehicle).filter_by(city=city, id=vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=vehicle_id)
            vehicle.update({'status': 'in_use' })
            v = vehicle.first()
            # get promo codes associated with this user's account
            upcs = session.query(UserPromoCode).filter_by(city=city, user_id=rider_id).all() if self.multi_region else session.query(UserPromoCode).filter_by(user_id=rider_id).all()

            # determine which codes are valid
            for upc in upcs:
                promo_code = session.query(PromoCode).filter_by(code = upc.code).first()
                if promo_code and promo_code.expiration_time > datetime.datetime.now():
                    code_to_update = session.query(UserPromoCode).filter_by(city=city,
                                                                  user_id=rider_id, code=upc.code) if self.multi_region else session.query(
                        UserPromoCode).filter_by(user_id=rider_id,code=upc.code)
                    code_to_update.update({'usage_count': upc.usage_count+1})

            r = Ride(city=city, id=MovRGenerator.generate_uuid(),
                     rider_id=rider_id, vehicle_id=vehicle_id,
                     start_address=v.current_location)

            session.add(r)
            return {'city': r.city, 'id': r.id}

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: start_ride_helper(session, city, rider_id, vehicle_id))


    def end_ride(self, city, ride_id):
        def end_ride_helper(session, city, ride_id):
            ride = session.query(Ride).filter_by(city=city, id=ride_id) if self.multi_region else session.query(Ride).filter_by(id=ride_id)
            r = ride.first()
            vehicle = session.query(Vehicle).filter_by(city=city, id=r.vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=r.vehicle_id)
            vehicle.update({'status': 'available'})
            v = vehicle.first()
            ride.update({'end_address':v.current_location, 'revenue': MovRGenerator.generate_revenue(),
                         'end_time': datetime.datetime.now()})


        run_transaction(sessionmaker(bind=self.engine), lambda session: end_ride_helper(session, city, ride_id))

    def update_ride_location(self, city, ride_id, lat, long):
        def update_ride_location_helper(session, city, ride_id, lat, long):
            h = VehicleLocationHistory(city = city, ride_id = ride_id, lat = lat, long = long)
            session.add(h)

        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: update_ride_location_helper(session, city, ride_id, lat, long))

    def add_user(self, city, name, address, credit_card_number):
        def add_user_helper(session, city, name, address, credit_card_number):
            u = User(city=city, id=MovRGenerator.generate_uuid(), name=name,
                     address=address, credit_card=credit_card_number)
            session.add(u)
            return {'city': u.city, 'id': u.id}
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_user_helper(session, city, name, address, credit_card_number))

    def add_vehicle(self, city, owner_id, current_location, type, vehicle_metadata, status):
        def add_vehicle_helper(session, city, owner_id, current_location, type, vehicle_metadata, status):
            vehicle_type = type

            vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                              city=city, owner_id=owner_id, current_location = current_location,
                              status=status,
                              ext=vehicle_metadata)

            session.add(vehicle)
            return {'city': vehicle.city, 'id': vehicle.id}
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_vehicle_helper(session,
                                                                  city, owner_id, current_location, type,
                                                                  vehicle_metadata, status))

    def get_users(self, city, follower_reads=False, limit=None):
        def get_users_helper(session, city, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            users = session.query(User).filter_by(city=city).limit(limit).all()
            return list(map(lambda user: {'city': user.city, 'id': user.id}, users))
        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_users_helper(session, city, follower_reads, limit))

    def get_vehicles(self, city, follower_reads=False, limit=None):

            def get_vehicles_helper(session, city, follower_reads, limit=None):
                if follower_reads:
                    session.execute(text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
                vehicles = session.query(Vehicle).filter_by(city=city).limit(limit).all()
                return list(map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles))

            return run_transaction(sessionmaker(bind=self.engine), lambda session: get_vehicles_helper(session, city, follower_reads, limit))

    def get_active_rides(self, city, follower_reads=False, limit=None):
        def get_active_rides_helper(session, city, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            rides = session.query(Ride).filter_by(city=city, end_time=None).limit(limit).all()
            return list(map(lambda ride: {'city': city, 'id': ride.id}, rides))

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: get_active_rides_helper(session, city, follower_reads, limit))

    def get_promo_codes(self, follower_reads=False, limit=None):
        def get_promo_codes_helper(session, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            pcs = session.query(PromoCode).limit(limit).all()
            return list(map(lambda pc: pc.code, pcs))

        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_promo_codes_helper(session, follower_reads, limit))


    def create_promo_code(self, code, description, expiration_time, rules):
        def add_promo_code_helper(session, code, description, expiration_time, rules):
            pc = PromoCode(code = code, description = description, expiration_time = expiration_time, rules = rules)
            session.add(pc)
            return pc.code

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_promo_code_helper(session, code, description, expiration_time, rules))


    def apply_promo_code(self, user_city, user_id, promo_code):
        def apply_promo_code_helper(session, user_city, user_id, code):
            pc = session.query(PromoCode).filter_by(code=code).one_or_none()
            if pc:
                # see if it has already been applied
                upc = session.query(UserPromoCode).\
                    filter_by(city = user_city, user_id = user_id, code = code).one_or_none() if self.multi_region else session.query(UserPromoCode).\
                    filter_by(user_id = user_id, code = code).one_or_none()
                if not upc:
                    upc = UserPromoCode(city = user_city, user_id = user_id, code = code)
                    session.add(upc)

        run_transaction(sessionmaker(bind=self.engine),
                               lambda session: apply_promo_code_helper(session, user_city, user_id, promo_code))

    def multi_query_helper(session, queries):
        for query in queries:
            session.execute(query)

    def run_queries_in_separate_transactions(self, queries):
        for query in queries:
            run_transaction(sessionmaker(bind=self.engine),
                            lambda session: session.execute(query))

    ##############
    # MULTI REGION TRANSFORMATIONS
    ################

    def get_multi_region_transformations(self):
        queries_to_run = {"database_regions": [],"table_localities": []}
        queries_to_run["database_regions"].append("ALTER DATABASE movr PRIMARY REGION "us-east1";")
        queries_to_run["database_regions"].append("ALTER DATABASE movr ADD REGION "us-west1";")
        queries_to_run["database_regions"].append("ALTER DATABASE movr ADD REGION "europe-west1";")
        queries_to_run["table_localities"].append("ALTER TABLE users SET LOCALITY REGIONAL BY ROW;")
        queries_to_run["table_localities"].append("ALTER TABLE rides SET LOCALITY REGIONAL BY ROW")
        queries_to_run["table_localities"].append("ALTER TABLE vehicle_location_histories SET LOCALITY REGIONAL BY ROW;")
        queries_to_run["table_localities"].append("ALTER TABLE vehicles SET LOCALITY REGIONAL BY ROW;")
        queries_to_run["table_localities"].append("ALTER TABLE user_promo_codes SET LOCALITY REGIONAL BY ROW;")
        queries_to_run["table_localities"].append("ALTER TABLE promo_codes SET LOCALITY GLOBAL;")

        return queries_to_run

    def run_multi_region_transformations(self):
        logging.info("applying schema changes to make this database multi-region (this may take up to a minute).")
        queries_to_run = self.get_multi_region_transformations()

        logging.info("altering database regions...")
        self.run_queries_in_separate_transactions(queries_to_run["database_regions"])
        logging.info("altering table localities...")
        self.run_queries_in_separate_transactions(queries_to_run["table_localities"])
        logging.info("done.")
