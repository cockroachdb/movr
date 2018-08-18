
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, String, DateTime, Float, \
    PrimaryKeyConstraint, ForeignKeyConstraint, CheckConstraint
from sqlalchemy.types import DECIMAL
from sqlalchemy.dialects.postgresql import UUID, JSONB

import datetime

from generators import MovRGenerator

#@todo: add interleaving

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(UUID, default=MovRGenerator.generate_uuid)
    city = Column(String)
    name = Column(String)
    address = Column(String)
    credit_card = Column(String)
    PrimaryKeyConstraint(city, id)

    def __repr__(self):
        return "<User(city='%s', id='%s', name='%s')>" % (self.city, self.id, self.name)

#@todo: sqlalchemy fails silently if compound fks are in the wrong order.
class Ride(Base):
    __tablename__ = 'rides'
    id = Column(UUID, default=MovRGenerator.generate_uuid)
    city = Column(String)
    vehicle_city = Column(String, CheckConstraint('vehicle_city=city')) #annoying workaround for https://github.com/cockroachdb/cockroach/issues/23580
    rider_id = Column(UUID)
    vehicle_id = Column(UUID)
    start_address = Column(String)
    end_address = Column(String)
    start_time = Column(DateTime, default=datetime.datetime.now)
    end_time = Column(DateTime)
    revenue = Column(DECIMAL(10,2))
    PrimaryKeyConstraint(city, id)
    __table_args__ = (ForeignKeyConstraint([city, rider_id], ["users.city", "users.id"]),) #this requires an index or it fails silently:  https://github.com/cockroachdb/cockroach/issues/22253
    __table_args__ = (ForeignKeyConstraint([vehicle_city, vehicle_id], ["vehicles.city", "vehicles.id"]),)


    def __repr__(self):
        return "<Ride(city='%s', id='%s', rider_id='%s', vehicle_id='%s')>" % (self.city, self.id, self.rider_id, self.vehicle_id)


class Vehicle(Base):
    __tablename__ = 'vehicles'
    id = Column(UUID, default=MovRGenerator.generate_uuid)
    city = Column(String)
    type = Column(String)
    owner_id = Column(UUID)
    creation_time = Column(DateTime, default=datetime.datetime.now)
    status = Column(String)
    ext = Column(JSONB)
    PrimaryKeyConstraint(city, id)
    #ForeignKeyConstraint(["city", "owner_id"], ["users.city", "users.id"]) #this requires an index or it fails silently: https://github.com/cockroachdb/cockroach/issues/22253
    __table_args__ = (ForeignKeyConstraint([city, owner_id], ["users.city", "users.id"]),)
    #__table_args__ = (Index('ix_vehicle_ext', ext, postgresql_using="gin"), )
    def __repr__(self):
        return "<Vehicle(city='%s', id='%s', type='%s', status='%s', ext='%s')>" % (self.city, self.id, self.type, self.status, self.ext)


