from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

import traceback
import random
from faker import Faker
from models import Base
from models import User, Vehicle, Ride
from generators import MovRGenerator
import datetime


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


    def start_ride(self, rider_id, vehicle_id):
        ride_id = self.gen.generate_uuid()
        self.session.add(Ride(id = ride_id,
                              rider_id=rider_id, vehicle_id=vehicle_id,
                              start_address = MovR.fake.address()))
        self.session.query(Vehicle).filter_by(id=vehicle_id).update({"status": "in_use"})
        self.session.commit()
        return ride_id

    def end_ride(self, ride_id):
        ride = self.session.query(Ride).filter_by(id=ride_id).first()
        ride.end_address = MovR.fake.address()
        ride.revenue = MovR.gen.generate_revenue()
        ride.end_time = datetime.datetime.now()
        self.session.query(Vehicle).filter_by(id = ride.vehicle_id).update({"status": "available"})
        self.session.commit()

    #@todo: add fake ride

    def add_user(self):
        u = self.User(id = self.gen.generate_uuid(), name = MovR.fake.name(),
                      address = MovR.fake.address(), credit_card = MovR.fake.credit_card_number())
        self.session.add(u)
        self.session.commit()
        return u.id

    def get_users(self):
        return self.session.query(User).all()

    def get_vehicles(self):
        return self.session.query(Vehicle).all()


    def add_vehicle(self, user_id, cities):
        try:
            vehicle_type = self.gen.generate_random_vehicle()
            ext = self.gen.generate_vehicle_metadata(vehicle_type)
            vehicle = Vehicle(id=self.gen.generate_uuid(), type=vehicle_type, city=cities,
                              owner_id=user_id,
                              status=MovRGenerator.get_vehicle_availability(), ext=ext)

            self.session.add(vehicle)
            self.session.commit()

            return vehicle.id
        except:
            traceback.print_exc()
            self.session.rollback()
            raise




