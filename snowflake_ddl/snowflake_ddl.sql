create database AIRBNB_PRO;
create schema staging;

use AIRBNB_PRO;
use schema staging;

-- DDL For My Tables
CREATE OR REPLACE TABLE HOSTS (
    host_id NUMBER,
    host_name STRING,
    host_since DATE,
    is_superhost BOOLEAN,
    response_rate NUMBER,
    created_at TIMESTAMP,
    PRIMARY KEY (host_id)
);

CREATE OR REPLACE TABLE LISTINGS (
    listing_id NUMBER,
    host_id NUMBER,
    property_type STRING,
    room_type STRING,
    city STRING,
    country STRING,
    accommodates NUMBER,
    bedrooms NUMBER,
    bathrooms NUMBER,
    price_per_night NUMBER,
    created_at TIMESTAMP,
    PRIMARY KEY (listing_id)
);

CREATE OR REPLACE TABLE BOOKINGS (
    booking_id STRING,
    listing_id NUMBER,
    booking_date TIMESTAMP,
    nights_booked NUMBER,
    booking_amount NUMBER,
    cleaning_fee NUMBER,
    service_fee NUMBER,
    booking_status STRING,
    created_at TIMESTAMP,
    PRIMARY KEY (booking_id)
);

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV' 
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

show FILE FORMATS;

-- till folder level we have the URI path..
CREATE OR REPLACE STAGE snowstage
FILE_FORMAT = csv_format
URL='s3://snowbucket-raw-aadil/source/';

show stages;

-- we should Ideally create storage integration and not expose the creds like this...
COPY INTO BOOKINGS
FRoM @snowstage
FILES=('bookings.csv')
CREDENTIALS=(aws_key_id = '', aws_secret_key = '');

COPY INTO LISTINGS
FRoM @snowstage
FILES=('listings.csv')
CREDENTIALS=(aws_key_id = '', aws_secret_key = '');

COPY INTO HOSTS
FRoM @snowstage
FILES=('hosts.csv')
CREDENTIALS=(aws_key_id = '', aws_secret_key = '');

select * from bookings;
select * from listings;
select * from hosts;


select * from airbnb_pro.bronze.bookings
