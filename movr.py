from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

import traceback
import random
from faker import Faker
from models import Base
from models import User, Vehicle, Ride
from generators import MovRGenerator


#@todo: remove hard coded connection string

class MovR:

    def __init__(self, conn_string, drop = False):
        engine = create_engine(conn_string, convert_unicode=True)

        if drop:
            Base.metadata.drop_all(bind=engine)
            Base.metadata.create_all(bind=engine)

        # @todo: make this a singleton
        MovR.gen = MovRGenerator()


        Session = sessionmaker(bind=engine)
        self.session = Session()

        #@todo: is this a singleton?
        self.User = User

        # @todo: need to close

        MovR.fake = Faker()

        # @todo: where should this live?





    def add_ride(self, rider_id, vehicle_id):
        self.session.add(Ride(rider_id=rider_id, vehicle_id=vehicle_id,
                              start_address = MovR.fake.address(), end_address = MovR.fake.address(),
                              revenue = MovR.gen.generate_revenue()))
        self.session.commit()

    def add_user(self):
        u = self.User(id = self.gen.generate_uuid(), name = MovR.fake.name(),
                      address = MovR.fake.address(), credit_card = MovR.fake.credit_card_number())
        self.session.add(u)
        self.session.commit()
        return u.id

    def get_vehicle_availability(self):
        return self.gen.weighted_choice([("available", .4), ("in_use", .55), ("lost", .05)])

    def find_vehicle_from_keys(self, keys):
        self.find_vehicle_by_id(random.choice(keys))

    def find_vehicle_by_id(self, id):
        try:
            return self.session.query(Vehicle).filter_by(city=id[0], id=id[1]).first()
        except:
            traceback.print_exc()
            self.session.rollback()
        finally:
            self.session.close()

    def update_vehicle_from_keys(self, keys):
        self.update_vehicle_by_id(random.choice(keys))

    def update_vehicle_by_id(self, id):
        try:
            self.session.query(Vehicle).filter_by(city=id[0], id=id[1]).update({"status": random.choice(["available",
                                                                                                    "in_use", "lost"])})
            self.session.commit()
        except:
            traceback.print_exc()
            self.session.rollback()
        finally:
            self.session.close()

    def find_and_select_vehicle(self):
        # print "finding and selecting vehicle" #@todo: add log level
        try:
            # @todo: grab a random user
            # @todo: create transactions where you find a ride and claim it if its available. Should be 10%
            v = self.session.query(Vehicle).filter_by(city=random.choice(args.city), type="bike", status="available").first()
            if v:
                v.status = 'in_use'
                self.session.commit()
            else:
                print "no vehicles were available to claim"

                # @todo: create transactions where people periodically return checked out items. Should be 10%

                # @todo: create load where people are just seeing whats available (ie home age load). Should be 80%
        except:
            self.session.rollback()
        finally:
            self.session.close()

    def returning_vehicle(self):


        try:
            v = self.session.query(Vehicle).filter_by(city=random.choice(args.city), status="in_use").first()
            if v:
                v.status = 'available'
                self.session.commit()
            else:
                print "no vehicles were available to claim"

                # @todo: create transactions where people periodically return checked out items. Should be 10%

                # @todo: create load where people are just seeing whats available (ie home age load). Should be 80%
        except:
            self.session.rollback()
        finally:
            self.session.close()

    def browse_vehicles(self, cities):

        try:
            self.session.query(Vehicle).filter_by(city=random.choice(cities), status="available").limit(25).all()
        except:
            self.session.rollback()
        finally:
            self.session.close()



    def add_vehicle(self, user_id, cities):
        try:
            vehicle_type = self.gen.generate_random_vehicle()
            ext = self.gen.generate_vehicle_metadata(vehicle_type)
            vehicle = Vehicle(id=self.gen.generate_uuid(), type=vehicle_type, city=cities,
                              owner_id=user_id,
                              status=self.get_vehicle_availability(), ext=ext)


            self.session.add(vehicle)
            self.session.commit()

            return vehicle.id
        except:
            print "GOT EXCEPTION!"
            traceback.print_exc()
            self.session.rollback()
            raise


    def get_keys_for_cities(self):
        try:
            return self.session.query(Vehicle.city, Vehicle.id).filter(Vehicle.city.in_(args.city)).all()
        except:
            traceback.print_exc()
            self.session.rollback()
        finally:
            self.session.close()



