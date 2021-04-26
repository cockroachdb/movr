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

    def start_ride(self, rider_id, vehicle_id):

        def start_ride_helper(session, rider_id, vehicle_id):
            vehicle = session.query(Vehicle).filter_by(id=vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=vehicle_id)
            vehicle.update({'status': 'in_use' })
            v = vehicle.first()
            # get promo codes associated with this user's account
            upcs = session.query(UserPromoCode).filter_by(user_id=rider_id).all() if self.multi_region else session.query(UserPromoCode).filter_by(user_id=rider_id).all()

            # determine which codes are valid
            for upc in upcs:
                promo_code = session.query(PromoCode).filter_by(code = upc.code).first()
                if promo_code and promo_code.expiration_time > datetime.datetime.now():
                    code_to_update = session.query(UserPromoCode).filter_by(city=city,
                                                                  user_id=rider_id, code=upc.code) if self.multi_region else session.query(
                        UserPromoCode).filter_by(user_id=rider_id,code=upc.code)
                    code_to_update.update({'usage_count': upc.usage_count+1})

            r = Ride(id=MovRGenerator.generate_uuid(),
                     rider_id=rider_id, vehicle_id=vehicle_id,
                     start_address=v.current_location)

            session.add(r)
            return {'id': r.id}

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: start_ride_helper(session, city, rider_id, vehicle_id))


    def end_ride(self, ride_id):
        def end_ride_helper(session, ride_id):
            ride = session.query(Ride).filter_by(id=ride_id) if self.multi_region else session.query(Ride).filter_by(id=ride_id)
            r = ride.first()
            vehicle = session.query(Vehicle).filter_by(id=r.vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=r.vehicle_id)
            vehicle.update({'status': 'available'})
            v = vehicle.first()
            ride.update({'end_address':v.current_location, 'revenue': MovRGenerator.generate_revenue(),
                         'end_time': datetime.datetime.now()})


        run_transaction(sessionmaker(bind=self.engine), lambda session: end_ride_helper(session, ride_id))

    def update_ride_location(self, ride_id, lat, long):
        def update_ride_location_helper(session, ride_id, lat, long):
            h = VehicleLocationHistory(ride_id = ride_id, lat = lat, long = long)
            session.add(h)

        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: update_ride_location_helper(session, city, ride_id, lat, long))

    def add_user(self, name, address, credit_card_number):
        def add_user_helper(session, name, address, credit_card_number):
            u = User(id=MovRGenerator.generate_uuid(), name=name,
                     address=address, credit_card=credit_card_number)
            session.add(u)
            return {'id': u.id}
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_user_helper(session, name, address, credit_card_number))

    def add_vehicle(self, owner_id, current_location, type, vehicle_metadata, status):
        def add_vehicle_helper(session, owner_id, current_location, type, vehicle_metadata, status):
            vehicle_type = type

            vehicle = Vehicle(id=MovRGenerator.generate_uuid(), type=vehicle_type,
                              owner_id=owner_id, current_location = current_location,
                              status=status,
                              ext=vehicle_metadata)

            session.add(vehicle)
            return {'id': vehicle.id}
        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_vehicle_helper(session,
                                                                  owner_id, current_location, type,
                                                                  vehicle_metadata, status))

    def get_users(self, follower_reads=False, limit=None):
        def get_users_helper(session, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
            users = session.query(User).limit(limit).all()
            return list(map(lambda user: {'id': user.id}, users))
        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_users_helper(session, follower_reads, limit))

    def get_vehicles(self, follower_reads=False, limit=None):

            def get_vehicles_helper(session, follower_reads, limit=None):
                if follower_reads:
                    session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
                vehicles = session.query(Vehicle).limit(limit).all()
                return list(map(lambda vehicle: {'id': vehicle.id}, vehicles))

            return run_transaction(sessionmaker(bind=self.engine), lambda session: get_vehicles_helper(session, follower_reads, limit))

    def get_active_rides(self, follower_reads=False, limit=None):
        def get_active_rides_helper(session, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            rides = session.query(Ride).filter_by(end_time=None).limit(limit).all()
            return list(map(lambda ride: {'id': ride.id}, rides))

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: get_active_rides_helper(session, follower_reads, limit))

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


    def apply_promo_code(self, user_id, promo_code):
        def apply_promo_code_helper(session, user_id, code):
            pc = session.query(PromoCode).filter_by(code=code).one_or_none()
            if pc:
                # see if it has already been applied
                upc = session.query(UserPromoCode).filter_by(user_id = user_id, code = code).one_or_none()
                if not upc:
                    upc = UserPromoCode(user_id = user_id, code = code)
                    session.add(upc)

        run_transaction(sessionmaker(bind=self.engine),
                               lambda session: apply_promo_code_helper(session, user_id, promo_code))

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
        queries_to_run = {"database_regions": [], "table_localities": []}
        #@todo: need to add the right database name
        queries_to_run["database_regions"].append('ALTER DATABASE movr PRIMARY REGION "us-east1";') #@todo: add IF NOT EXISTS
        queries_to_run["database_regions"].append('ALTER DATABASE movr ADD REGION "us-west1";')
        queries_to_run["database_regions"].append('ALTER DATABASE movr ADD REGION "europe-west1";')
        queries_to_run["table_localities"].append('ALTER TABLE users SET LOCALITY REGIONAL BY ROW;')
        queries_to_run["table_localities"].append('ALTER TABLE rides SET LOCALITY REGIONAL BY ROW;')
        queries_to_run["table_localities"].append('ALTER TABLE vehicle_location_histories SET LOCALITY REGIONAL BY ROW;')
        queries_to_run["table_localities"].append('ALTER TABLE vehicles SET LOCALITY REGIONAL BY ROW;')
        queries_to_run["table_localities"].append('ALTER TABLE user_promo_codes SET LOCALITY REGIONAL BY ROW;')
        queries_to_run["table_localities"].append('ALTER TABLE promo_codes SET LOCALITY GLOBAL;')
        return queries_to_run

    def run_multi_region_transformations(self):
        logging.info("applying schema changes to make this database multi-region (this may take up to a minute).")
        queries_to_run = self.get_multi_region_transformations()

        logging.info("altering database regions...")
        self.run_queries_in_separate_transactions(queries_to_run["database_regions"])
        logging.info("altering table localities...")
        self.run_queries_in_separate_transactions(queries_to_run["table_localities"])
        logging.info("done.")










