CREATE DATABASE movr_new;
USE movr_new:

--No changes to promo_codes
CREATE TABLE public.promo_codes (
  code VARCHAR NOT NULL,
  description VARCHAR NULL,
  creation_time TIMESTAMP NULL,
  expiration_time TIMESTAMP NULL,
  rules JSONB NULL,
  CONSTRAINT "primary" PRIMARY KEY (code ASC)
)
--New users table
CREATE TABLE public.users (
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  name VARCHAR NULL,
  address VARCHAR NULL,
  credit_card VARCHAR NULL,
  CONSTRAINT "primary" PRIMARY KEY (id ASC)
)

--New user_promo_codes
CREATE TABLE public.user_promo_codes (
  city VARCHAR NOT NULL,
  user_id UUID NOT NULL REFERENCES users (id),
  code VARCHAR NOT NULL,
  "timestamp" TIMESTAMP NULL,
  usage_count INT8 NULL,
  CONSTRAINT "primary" PRIMARY KEY (user_id ASC, code ASC)
)

--New vehicles Table
--need to check query performance
CREATE TABLE public.vehicles (
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  type VARCHAR NULL,
  owner_id UUID NULL REFERENCES users (id),
  creation_time TIMESTAMP NULL,
  status VARCHAR NULL,
  current_location VARCHAR NULL,
  ext JSONB NULL,
  CONSTRAINT "primary" PRIMARY KEY (id ASC)
)
--New rides table
--check query performance after removing indexes
CREATE TABLE public.rides (
  id UUID NOT NULL,
  city VARCHAR NOT NULL,
  vehicle_city VARCHAR NULL,
  rider_id UUID NULL REFERENCES users (id),
  vehicle_id UUID NULL REFERENCES vehicles (id),
  start_address VARCHAR NULL,
  end_address VARCHAR NULL,
  start_time TIMESTAMP NULL,
  end_time TIMESTAMP NULL,
  revenue DECIMAL(10,2) NULL,
  CONSTRAINT "primary" PRIMARY KEY (id ASC),
  CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city)
)
--New vehicle_location_histories table
CREATE TABLE public.vehicle_location_histories (
  city VARCHAR NOT NULL,
  ride_id UUID NOT NULL REFERENCES rides (id),
  "timestamp" TIMESTAMP NOT NULL,
  lat FLOAT8 NULL,
  long FLOAT8 NULL,
  CONSTRAINT "primary" PRIMARY KEY (ride_id ASC, "timestamp" ASC)
)
