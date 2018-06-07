
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, String, DateTime, Integer, Float, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB

import datetime

from generators import MovRGenerator

#@todo: add interleaving

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(UUID, default=MovRGenerator.generate_uuid)
    name = Column(String)
    address = Column(String)
    credit_card = Column(String)
    PrimaryKeyConstraint(id)

    def __repr__(self):
        return "<User(id='%s', name='%s')>" % (self.id, self.name)


class Ride(Base):
    __tablename__ = 'rides'
    id = Column(UUID, default=MovRGenerator.generate_uuid)
    rider_id = Column(UUID, ForeignKey("users.id"))
    vehicle_id = Column(UUID, ForeignKey("vehicles.id"))
    start_address = Column(String)
    end_address = Column(String)
    start_time = Column(DateTime, default=datetime.datetime.now)
    end_time = Column(DateTime)
    revenue = Column(Float)
    PrimaryKeyConstraint(id)

    def __repr__(self):
        return "<Ride(id='%s', start_address='%s', end_address='%s')>" % (self.id, self.start_address, self.end_address)


class Vehicle(Base):
    __tablename__ = 'vehicles'
    id = Column(UUID, default=MovRGenerator.generate_uuid, unique=True)
    type = Column(String)
    city = Column(String)
    owner_id = Column(UUID, ForeignKey("users.id"))
    creation_time = Column(DateTime, default=datetime.datetime.now)
    status = Column(String)
    ext = Column(JSONB)
    PrimaryKeyConstraint(city, id)
    __table_args__ = (Index('ix_vehicle_type', type),)
    #__table_args__ = (Index('ix_vehicle_ext', ext, postgresql_using="gin"), )
    def __repr__(self):
        return "<Vehicle(id='%s', type='%s', status='%s', ext='%s')>" % (self.id, self.type, self.status, self.ext)


