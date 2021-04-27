
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, String, DateTime, Integer, Float, \
    PrimaryKeyConstraint, ForeignKeyConstraint, CheckConstraint
from sqlalchemy.types import DECIMAL
from sqlalchemy.dialects.postgresql import UUID, JSONB

import datetime

from generators import MovRGenerator


#default to the single region schema and dynamically make multi-region based on command line args.

Base = declarative_base()

#@todo: how to override region
class User(Base):
    __tablename__ = 'users'
    id = Column(UUID, primary_key=True, default=MovRGenerator.generate_uuid)
    name = Column(String)
    address = Column(String)
    credit_card = Column(String)

    def __repr__(self):
        return "<User(id='%s', name='%s')>" % (self.id, self.name)

class Ride(Base):
    __tablename__ = 'rides'
    id = Column(UUID, primary_key=True, default=MovRGenerator.generate_uuid)
    rider_id = Column(UUID)
    vehicle_id = Column(UUID)
    start_address = Column(String)
    end_address = Column(String)
    start_time = Column(DateTime, default=datetime.datetime.now)
    end_time = Column(DateTime)
    revenue = Column(DECIMAL(10,2))
    __table_args__ = (ForeignKeyConstraint([rider_id], ["users.id"], name='fk_rider_id_ref_users'),)  #@todo: may not need to name these with new mr work
    __table_args__ = (ForeignKeyConstraint([vehicle_id], ["vehicles.id"], name='fk_vehicle_id_ref_vehicles'),)


    def __repr__(self):
        return "<Ride(id='%s', rider_id='%s', vehicle_id='%s')>" % (self.id, self.rider_id, self.vehicle_id)

class VehicleLocationHistory(Base):
    __tablename__ = 'vehicle_location_histories'
    ride_id = Column(UUID)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    lat = Column(Float)
    long = Column(Float)
    PrimaryKeyConstraint(ride_id, timestamp)
    __table_args__ = (ForeignKeyConstraint([ride_id], ["rides.id"]),)

    def __repr__(self):
        return "<VehicleLocationHistory(ride_id='%s', timestamp='%s', lat='%s', long='%s')>" % \
               (self.ride_id, self.timestamp, self.lat, self.long)

class Vehicle(Base):
    __tablename__ = 'vehicles'
    id = Column(UUID, primary_key=True, default=MovRGenerator.generate_uuid)
    type = Column(String)
    owner_id = Column(UUID)
    creation_time = Column(DateTime, default=datetime.datetime.now)
    status = Column(String)
    current_location = Column(String)
    ext = Column(JSONB)
    __table_args__ = (ForeignKeyConstraint([owner_id], ["users.id"], name='fk_owner_id_ref_users'),)

    def __repr__(self):
        return "<Vehicle(id='%s', type='%s', status='%s', ext='%s')>" % (self.id, self.type, self.status, self.ext)

class PromoCode(Base):
    __tablename__ = 'promo_codes'
    code = Column(String, primary_key=True)
    description = Column(String)
    creation_time = Column(DateTime, default=datetime.datetime.now)
    expiration_time = Column(DateTime)
    rules = Column(JSONB)

    def __repr__(self):
        return "<PromoCode(code='%s', description='%s', creation_time='%s', expiration_time='%s', rules='%s')>" % \
               (self.code, self.description, self.creation_time, self.expiration_time, self.rules)


class UserPromoCode(Base):
    __tablename__ = 'user_promo_codes'
    user_id = Column(UUID)
    code = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    usage_count = Column(Integer, default=0)

    PrimaryKeyConstraint(user_id, code)

    __table_args__ = (ForeignKeyConstraint([code], ["promo_codes.code"]),)
    __table_args__ = (ForeignKeyConstraint([user_id], ["users.id"], name='fk_user_id_ref_users'),)

    def __repr__(self):
        return "<UserPromoCode(user_id='%s', code='%s', timestamp='%s')>" % \
               (self.user_id, self.code, self.timestamp)
