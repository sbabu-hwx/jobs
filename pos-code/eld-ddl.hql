set hivevar:GEO_ENV=qa;
create database if not exists ${hivevar:GEO_ENV};

drop table ${hivevar:GEO_ENV}.eld_geo_raw_location;
CREATE EXTERNAL TABLE ${hivevar:GEO_ENV}.eld_geo_raw_location(
RAW_RECORD string,
DEVICE_SERIAL_NUMBER STRING,
DEVICE_VIN string,
DEVICE_TYPE string,
DEVICE_LICENSE_STATE string,
DEVICE_LICENSE_PLATE string,
DEVICE_WORKTIME string,
DEVICE_NAME string,
DEVICE_ID string,
DEVICE_PLANS string,
LOCATION_BEARING int,
LOCATION_LONGITUDE double,
LOCATION_LATITUDE double,
LOCATION_SPEED double,
LOCATION_DRIVER String,
LOCATION_DATETIME String
)
COMMENT 'Real Time GEO eld -Raw'
PARTITIONED BY (
  date_part string,
  hour_part string)
row format DELIMITED
fields terminated by '|'
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location "/${hivevar:GEO_ENV}/landing/eld/raw/location";
  
  
  
  
drop table ${hivevar:GEO_ENV}.eld_geo_staging_location;
CREATE EXTERNAL TABLE ${hivevar:GEO_ENV}.eld_geo_staging_location(
DEVICE_SERIAL_NUMBER STRING,
DEVICE_VIN string,
DEVICE_TYPE string,
DEVICE_LICENSE_STATE string,
DEVICE_LICENSE_PLATE string,
DEVICE_WORKTIME string,
DEVICE_NAME string,
DEVICE_ID string,
DEVICE_PLANS string,
LOCATION_BEARING int,
LOCATION_LONGITUDE double,
LOCATION_LATITUDE double,
LOCATION_SPEED double,
LOCATION_DRIVER String,
LOCATION_DATETIME String
)
COMMENT 'GEO eld=Staging'
PARTITIONED BY (
  date_part string,
  hour_part string)
STORED AS ORC
LOCATION '/${hivevar:GEO_ENV}/landing/eld/staging/location'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

drop table ${hivevar:GEO_ENV}.eld_geo_location;
CREATE EXTERNAL TABLE ${hivevar:GEO_ENV}.eld_geo_location(
DEVICE_SERIAL_NUMBER STRING,
DEVICE_VIN string,
DEVICE_TYPE string,
DEVICE_LICENSE_STATE string,
DEVICE_LICENSE_PLATE string,
DEVICE_WORKTIME string,
DEVICE_NAME string,
DEVICE_ID string,
DEVICE_PLANS string,
LOCATION_BEARING int,
LOCATION_LONGITUDE double,
LOCATION_LATITUDE double,
LOCATION_SPEED double,
LOCATION_DRIVER String,
LOCATION_DATETIME String
)
COMMENT 'GEO Transaction- Reporting'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:GEO_ENV}/reporting/eld/location'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');
