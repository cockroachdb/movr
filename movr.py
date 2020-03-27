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

            r = Ride(city=city, vehicle_city=city, id=MovRGenerator.generate_uuid(),
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
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
            users = session.query(User).filter_by(city=city).limit(limit).all()
            return list(map(lambda user: {'city': user.city, 'id': user.id}, users))
        return run_transaction(sessionmaker(bind=self.engine), lambda session: get_users_helper(session, city, follower_reads, limit))

    def get_vehicles(self, city, follower_reads=False, limit=None):

            def get_vehicles_helper(session, city, follower_reads, limit=None):
                if follower_reads:
                    session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
                vehicles = session.query(Vehicle).filter_by(city=city).limit(limit).all()
                return list(map(lambda vehicle: {'city': vehicle.city, 'id': vehicle.id}, vehicles))

            return run_transaction(sessionmaker(bind=self.engine), lambda session: get_vehicles_helper(session, city, follower_reads, limit))

    def get_active_rides(self, city, follower_reads=False, limit=None):
        def get_active_rides_helper(session, city, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
            rides = session.query(Ride).filter_by(city=city, end_time=None).limit(limit).all()
            return list(map(lambda ride: {'city': city, 'id': ride.id}, rides))

        return run_transaction(sessionmaker(bind=self.engine),
                               lambda session: get_active_rides_helper(session, city, follower_reads, limit))

    def get_promo_codes(self, follower_reads=False, limit=None):
        def get_promo_codes_helper(session, follower_reads, limit=None):
            if follower_reads:
                session.execute(text('SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()'))
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

    ##############
    # MULTI REGION TRANSFORMATIONS
    ################

    def get_multi_region_transformations(self):
        queries_to_run = {"pk_alters": [], "fk_alters": []}
        queries_to_run["pk_alters"].append("ALTER TABLE users ALTER PRIMARY KEY USING COLUMNS (city, id);")
        queries_to_run["pk_alters"].append("ALTER TABLE rides ALTER PRIMARY KEY USING COLUMNS (city, id);")
        queries_to_run["pk_alters"].append(
            "ALTER TABLE vehicle_location_histories ALTER PRIMARY KEY USING COLUMNS (city, ride_id, timestamp);")
        queries_to_run["pk_alters"].append("ALTER TABLE vehicles ALTER PRIMARY KEY USING COLUMNS (city, id);")
        queries_to_run["pk_alters"].append("ALTER TABLE user_promo_codes ALTER PRIMARY KEY USING COLUMNS (city, user_id, code);")

        # vehicles
        queries_to_run["fk_alters"].append("ALTER TABLE vehicles DROP CONSTRAINT fk_owner_id_ref_users;")
        #foreign key requires an existing index on columns
        queries_to_run["fk_alters"].append("CREATE INDEX ON vehicles (city, owner_id);")
        queries_to_run["fk_alters"].append("DROP INDEX vehicles_auto_index_fk_owner_id_ref_users;")
        queries_to_run["fk_alters"].append(
            "ALTER TABLE vehicles ADD CONSTRAINT fk_owner_id_ref_users_mr FOREIGN KEY (city, owner_id) REFERENCES users (city,id);")

        # rides
        queries_to_run["fk_alters"].append("ALTER TABLE rides DROP CONSTRAINT fk_rider_id_ref_users;")
        queries_to_run["fk_alters"].append("CREATE INDEX ON rides (city, rider_id);")
        queries_to_run["fk_alters"].append(
            "ALTER TABLE rides ADD CONSTRAINT fk_rider_id_ref_users_mr FOREIGN KEY (city, rider_id) REFERENCES users (city,id);")
        queries_to_run["fk_alters"].append("ALTER TABLE rides DROP CONSTRAINT fk_vehicle_id_ref_vehicles;")
        queries_to_run["fk_alters"].append("CREATE INDEX ON rides (vehicle_city, vehicle_id);")
        queries_to_run["fk_alters"].append(
            "ALTER TABLE rides ADD CONSTRAINT fk_vehicle_id_ref_vehicles_mr FOREIGN KEY (vehicle_city, vehicle_id) REFERENCES vehicles (city,id);")
        queries_to_run["fk_alters"].append("DROP INDEX rides_auto_index_fk_rider_id_ref_users;")
        queries_to_run["fk_alters"].append("DROP INDEX rides_auto_index_fk_vehicle_id_ref_vehicles;")

        # @todo: remove single region index
        # user_promo_codes
        queries_to_run["fk_alters"].append("ALTER TABLE user_promo_codes DROP CONSTRAINT fk_user_id_ref_users;")
        queries_to_run["fk_alters"].append(
            "ALTER TABLE user_promo_codes ADD CONSTRAINT fk_user_id_ref_users_mr FOREIGN KEY (city, user_id) REFERENCES users (city,id);")

        return queries_to_run

    def run_multi_region_transformations(self):
        logging.info("applying schema changes to make this database multi-region (this may take up to a minute).")
        queries_to_run = self.get_multi_region_transformations()


        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries_to_run["pk_alters"]))

        # #@todo: this causes an FK issue for some reason
        # run_transaction(sessionmaker(bind=self.engine),
        #                 lambda session: MovR.multi_query_helper(session, queries_to_run["fk_alters"]))

        for query in queries_to_run["fk_alters"]:
            run_transaction(sessionmaker(bind=self.engine),
                        lambda session: session.execute(query))


    ############
    # GEO PARTITIONING
    ############

    def get_geo_partitioning_queries(self, partition_map, zone_map):


        def get_index_partition_name(region, index_name):
            return region + "_" + index_name

        def create_partition_string(index_name=""):
            partition_string = ""
            first_region = True
            for region in partition_map:
                region_name = get_index_partition_name(region, index_name) if index_name else region
                partition_string += "PARTITION " + region_name + " VALUES IN (" if first_region \
                    else ", PARTITION " + region_name + " VALUES IN ("
                first_region = False
                first_city = True
                for city in partition_map[region]:
                    partition_string += "'" + city + "' " if first_city else ", '" + city + "'"
                    first_city = False
                partition_string += ")"
            return partition_string

        queries_to_run = {}

        partition_string = create_partition_string()
        for table in ["vehicles", "users", "rides", "vehicle_location_histories", "user_promo_codes"]:
            partition_sql = "ALTER TABLE " + table + " PARTITION BY LIST (city) (" + partition_string + ");"
            queries_to_run.setdefault("table_partitions",[]).append(partition_sql)

            for partition_name in partition_map:
                if not partition_name in zone_map:
                    logging.info("partition_name %s not found in zone map. Skipping", partition_name)
                    continue

                zone_sql = "ALTER PARTITION " + partition_name + " OF TABLE " + table + " CONFIGURE ZONE USING constraints='[+region=" + \
                           zone_map[partition_name] + "]';"
                queries_to_run.setdefault("table_zones",[]).append(zone_sql)

        for index in [{"index_name": "rides_city_rider_id_idx", "prefix_name": "city", "table": "rides"},
                      {"index_name": "rides_vehicle_city_vehicle_id_idx", "prefix_name": "vehicle_city",
                       "table": "rides"},
                      {"index_name": "vehicles_city_owner_id_idx", "prefix_name": "city",
                       "table": "vehicles"}]:
            partition_string = create_partition_string(index_name=index["index_name"])
            partition_sql = "ALTER INDEX " + index["index_name"] + " PARTITION BY LIST (" + index[
                "prefix_name"] + ") (" + partition_string + ");"
            queries_to_run.setdefault("index_partitions",[]).append(partition_sql)

            for partition_name in partition_map:
                if not partition_name in zone_map:
                    logging.info("partition_name %s not found in zone map. Skipping", partition_name)
                    continue
                zone_sql = "ALTER PARTITION " + get_index_partition_name(partition_name,
                                                                         index["index_name"]) + " OF TABLE " + \
                           index["table"] + " CONFIGURE ZONE USING constraints='[+region=" + zone_map[
                               partition_name] + "]';"
                queries_to_run.setdefault("index_zones",[]).append(zone_sql)


        # create an index in each region so we can use the zone-config aware CBO
        for partition_name in partition_map:
            if not partition_name in zone_map:
                logging.info("partition_name %s not found in zone map. Skipping index creation for promo codes",
                             partition_name)
                continue

            sql = "CREATE INDEX promo_codes_" + partition_name + "_idx on promo_codes (code) STORING (description, creation_time, expiration_time, rules);"
            queries_to_run.setdefault("promo_code_indices",[]).append(sql)

            sql = "ALTER INDEX promo_codes@promo_codes_" + partition_name + "_idx CONFIGURE ZONE USING constraints='[+region=" + \
                  zone_map[partition_name] + "]';";
            queries_to_run.setdefault("promo_code_zones",[]).append(sql)

        return queries_to_run


    # setup geo-partitioning if this is an enterprise cluster
    def add_geo_partitioning(self, partition_map, zone_map):
        queries = self.get_geo_partitioning_queries(partition_map, zone_map)



        logging.info("partitioned tables...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["table_partitions"]))

        logging.info("partitioned indices...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["index_partitions"]))

        logging.info("applying table zone configs...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["table_zones"]))

        logging.info("applying index zone configs...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["index_zones"]))

        logging.info("adding indexes for promo code reference tables...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["promo_code_indices"]))

        logging.info("applying zone configs for reference table indices...")
        run_transaction(sessionmaker(bind=self.engine),
                        lambda session: MovR.multi_query_helper(session, queries["promo_code_zones"]))









