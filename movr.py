from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from models import Base, User, Vehicle, Ride, VehicleLocationHistory, PromoCode, UserPromoCode
from cockroachdb.sqlalchemy import run_transaction
from generators import MovRGenerator

import datetime
import logging


class MovR:

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

    def __init__(self, conn_string, reset_tables=False, multi_region=False, primary_region=None, echo=False):

        self.engine = create_engine(
            conn_string, convert_unicode=True, echo=echo)
        self.session = sessionmaker(bind=self.engine)()
        self.primary_region = primary_region
        self.multi_region = multi_region

        if reset_tables:
            logging.info("reseting movr schema")
            logging.info("initializing tables")
            Base.metadata.drop_all(bind=self.engine)
            Base.metadata.create_all(bind=self.engine)
            logging.debug("tables dropped and created")
            if multi_region:
                self.run_multi_region_transformations()

    ##################
    # MAIN MOVR API
    #################

    def start_ride(self, city, rider_id, vehicle_id):

        def start_ride_helper(session, city, rider_id, vehicle_id):
            vehicle = session.query(Vehicle).filter_by(
                city=city, id=vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=vehicle_id)
            vehicle.update({'status': 'in_use'})
            v = vehicle.first()
            # get promo codes associated with this user's account
            upcs = session.query(UserPromoCode).filter_by(city=city, user_id=rider_id).all(
            ) if self.multi_region else session.query(UserPromoCode).filter_by(user_id=rider_id).all()

            # determine which codes are valid
            for upc in upcs:
                promo_code = session.query(
                    PromoCode).filter_by(code=upc.code).first()
                if promo_code and promo_code.expiration_time > datetime.datetime.now():
                    code_to_update = session.query(UserPromoCode).filter_by(city=city,
                                                                            user_id=rider_id, code=upc.code) if self.multi_region else session.query(
                        UserPromoCode).filter_by(user_id=rider_id, code=upc.code)
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
            ride = session.query(Ride).filter_by(
                city=city, id=ride_id) if self.multi_region else session.query(Ride).filter_by(id=ride_id)
            r = ride.first()
            vehicle = session.query(Vehicle).filter_by(
                city=city, id=r.vehicle_id) if self.multi_region else session.query(Vehicle).filter_by(id=r.vehicle_id)
            vehicle.update({'status': 'available'})
            v = vehicle.first()
            ride.update({'end_address': v.current_location, 'revenue': MovRGenerator.generate_revenue(),
                         'end_time': datetime.datetime.now()})

        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: end_ride_helper(session, city, ride_id))

    def update_ride_location(self, city, ride_id, lat, long):

        def update_ride_location_helper(session, city, ride_id, lat, long):
            h = VehicleLocationHistory(
                city=city, ride_id=ride_id, lat=lat, long=long)
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
                              city=city, owner_id=owner_id, current_location=current_location,
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
                session.execute(
                    text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            users = session.query(User).filter_by(city=city).limit(limit).all()
            return list(map(lambda user: {'city': user.city, 'id': user.id}, users))
        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_users_helper(session, city, follower_reads, limit))

    def get_vehicles(self, city, follower_reads=False, limit=None):

        def get_vehicles_helper(session, city, follower_reads, limit=None):
            if follower_reads:
                session.execute(
                    text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            vehicles = session.query(Vehicle).filter_by(
                city=city).limit(limit).all()
            return list(map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles))

        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_vehicles_helper(session, city, follower_reads, limit))

    def get_active_rides(self, city, follower_reads=False, limit=None):

        def get_active_rides_helper(session, city, follower_reads, limit=None):
            if follower_reads:
                session.execute(
                    text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            rides = session.query(Ride).filter_by(
                city=city, end_time=None).limit(limit).all()
            return list(map(lambda ride: {'city': city, 'id': ride.id}, rides))

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: get_active_rides_helper(session, city, follower_reads, limit))

    def get_promo_codes(self, follower_reads=False, limit=None):

        def get_promo_codes_helper(session, follower_reads, limit=None):
            if follower_reads:
                session.execute(
                    text('SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()'))
            pcs = session.query(PromoCode).limit(limit).all()
            return list(map(lambda pc: pc.code, pcs))

        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_promo_codes_helper(session, follower_reads, limit))

    def create_promo_code(self, code, description, expiration_time, rules):

        def add_promo_code_helper(session, code, description, expiration_time, rules):
            pc = PromoCode(code=code, description=description,
                           expiration_time=expiration_time, rules=rules)
            session.add(pc)
            return pc.code

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: add_promo_code_helper(session, code, description, expiration_time, rules))

    def apply_promo_code(self, user_city, user_id, promo_code):

        def apply_promo_code_helper(session, user_city, user_id, code):
            pc = session.query(PromoCode).filter_by(code=code).one_or_none()
            if pc:
                # see if it has already been applied
                upc = session.query(UserPromoCode).filter_by(city=user_city, user_id=user_id, code=code).one_or_none(
                ) if self.multi_region else session.query(UserPromoCode).filter_by(user_id=user_id, code=code).one_or_none()
                if not upc:
                    upc = UserPromoCode(
                        city=user_city, user_id=user_id, code=code)
                    session.add(upc)

        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: apply_promo_code_helper(session, user_city, user_id, promo_code))

    def run_queries_in_separate_transactions(self, queries):
        for query in queries:
            try:
                run_transaction(sessionmaker(bind=self.engine),
                                lambda session: session.execute(query))
            except ProgrammingError as err:
                if 'Duplicate' in str(err):
                    logging.info(
                        "The following query will be skipped, due to a duplicate object error:")
                    logging.info(str(query))
                    continue
                else:
                    raise err

    ##############
    # MULTI REGION TRANSFORMATIONS
    ################

    def assign_regions(self, regions):
        DEFAULT_REGION_MAP = {
            'us_east': ['new york', 'boston', 'washington dc'],
            'us_west': ['san francisco', 'seattle', 'los angeles'],
            'us_central': ['chicago', 'detroit', 'minneapolis'],
            'eu_west': ['amsterdam', 'paris', 'rome']
        }
        region_map = {}

        for region in regions:
            if 'east' in region:
                region_map[region] = DEFAULT_REGION_MAP['us_east']
            elif 'central' in region:
                region_map[region] = DEFAULT_REGION_MAP['us_central']
            elif 'west' in region:
                if 'eu' in region:
                    region_map[region] = DEFAULT_REGION_MAP['eu_west']
                else:
                    region_map[region] = DEFAULT_REGION_MAP['us_west']
            else:
                region_map = None

        if len(region_map) < len(DEFAULT_REGION_MAP):
            default_cities = []
            region_cities = []
            for region_default in DEFAULT_REGION_MAP:
                default_cities.extend(DEFAULT_REGION_MAP[region_default])
            for region in region_map:
                region_cities.extend(region_map[region])
            for city in default_cities:
                if city not in region_cities:
                    region_map[self.primary_region].append(city)

        return region_map

    def get_multi_region_transformations(self):
        # This function uses raw SQL, as there is no functional mapping to multi-region ALTER statements, in any ORM/tool
        # We should really execute all schema changes with Alembic (the migration tool built for SQLAlchemy), but this should work for now
        # See https://docs.sqlalchemy.org/en/14/core/metadata.html#altering-database-objects-through-migrations

        # Alter database statements
        region_tups = self.session.execute(
            text('SELECT region FROM [SHOW REGIONS]')).fetchall()
        regions = []
        for tup in region_tups:
            regions.append(tup[0])

        if self.primary_region is None:
            self.primary_region = regions[0]

        add_primary_region_query = 'ALTER DATABASE movr PRIMARY REGION "{0}"'.format(
            self.primary_region)
        add_primary_region_query = text(add_primary_region_query)

        add_region_queries = []
        for region in regions:
            if region != self.primary_region:
                add_region_query = 'ALTER DATABASE movr ADD REGION "{0}"'.format(
                    region)
                add_region_query = text(add_region_query)
                add_region_queries.append(add_region_query)

        # Alter table statements
        tables = []
        table_tups = self.session.execute(
            text('SELECT table_name FROM [SHOW TABLES]')).fetchall()
        for tup in table_tups:
            tables.append(tup[0])

        region_map = self.assign_regions(regions)
        add_region_column_queries = []
        not_null_region_column_queries = []
        set_locality_regional_queries = []
        for table in tables:
            if table != 'promo_codes':
                cases_when = ''
                for region in regions:
                    cities = tuple(region_map[region])
                    case_when_city = 'WHEN city IN {0} THEN \'{1}\' '.format(
                        cities, region)
                    cases_when = cases_when + case_when_city
                region_query = 'ALTER TABLE {0} ADD COLUMN region crdb_internal_region AS (CASE {1} END) STORED'.format(
                    table, cases_when)
                region_query = text(region_query)
                add_region_column_queries.append(region_query)
                not_null_query = 'ALTER TABLE {0} ALTER COLUMN region SET NOT NULL'.format(
                    table)
                not_null_query = text(not_null_query)
                not_null_region_column_queries.append(not_null_query)
                locality_query = 'ALTER TABLE {0} SET LOCALITY REGIONAL BY ROW AS "region"'.format(
                    table)
                locality_query = text(locality_query)
                set_locality_regional_queries.append(locality_query)

        set_locality_global_query = text(
            'ALTER TABLE promo_codes SET LOCALITY GLOBAL')

        queries = [add_primary_region_query, set_locality_global_query]
        queries.extend(add_region_queries)
        queries.extend(add_region_column_queries)
        queries.extend(not_null_region_column_queries)
        queries.extend(set_locality_regional_queries)

        return queries

    def run_multi_region_transformations(self):
        logging.info("constructing multi-region schema change statements.")
        queries_to_run = self.get_multi_region_transformations()
        logging.info(
            "applying multi-region schema changes (this may take a few minutes).")
        self.run_queries_in_separate_transactions(queries_to_run)
        logging.info("done.")
