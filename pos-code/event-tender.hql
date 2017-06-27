-- Setup Environment
set hivevar:POS_ENV=qa;
create database if not exists ${hivevar:POS_ENV};

-- MSMQ Tender Raw Table
drop table ${hivevar:POS_ENV}.event_pos_raw_tender;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_raw_tender(
RAW_RECORD string,
Amount double,
EntryMethod string,
Approval int,
AuthNumber int,
FullCardName string,
AccountNumber int,
TruckCompanyName string,
OLATenderKey string
)
COMMENT 'Real Time POS Tender -Raw'
PARTITIONED BY (
  date_part string,
  hour_part string)
row format DELIMITED
fields terminated by '|'
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location "/${hivevar:POS_ENV}/landing/event/raw/Tender";

-- MSMQ Tender Staging Table
drop table ${hivevar:POS_ENV}.event_pos_staging_tender;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_staging_tender(
LOYALTY_NUM string,
OPERATOR_ID string,
STORE_ID int,
TERMINAL_ID string,
TRANSACTION_ID int,
TRANSACTION_TOTAL double,
TRANS_DATE timestamp,
TRANS_TYPE string,
Loyalty_Bonus_Points int,
Loyalty_Message string,
Loyalty_Points int,
Loyalty_TransAmt  double,
AMOUNT double,
ENTRY_METHOD string,
BASE_UNIT_PRICE double,
ITEM_CODE string,
LINE_ID int,
ITEM_NAME string,
OLA_PRODUCT_CODE string,  --BARCODE
PUMP_ID int,
PUMP_SERVER_REF string,
QUANTITY double,
TAX_AMOUNT double,
UNIT_PRICE double,
WEIGHT double,
BASE_AMOUNT double,
DeptId int,
PromoPrice double,
PromoType string,
SubDeptId int
)
COMMENT 'Real Time POS event=Staging'
PARTITIONED BY (
  date_part string,
  hour_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/landing/event/staging/trans'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

--MSMQ Transaction Reporting Table
drop table ${hivevar:POS_ENV}.event_pos_trans;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_trans(
LOYALTY_NUM string,
OPERATOR_ID string,
STORE_ID int,
TERMINAL_ID string,
TRANSACTION_ID int,
TRANSACTION_TOTAL double,
TRANS_DATE timestamp,
TRANS_TYPE string,
Loyalty_Bonus_Points int,
Loyalty_Message string,
Loyalty_Points int,
Loyalty_TransAmt  double,
AMOUNT double,
ENTRY_METHOD string,
BASE_UNIT_PRICE double,
ITEM_CODE string,
LINE_ID int,
ITEM_NAME string,
OLA_PRODUCT_CODE string,  --BARCODE
PUMP_ID int,
PUMP_SERVER_REF string,
QUANTITY double,
TAX_AMOUNT double,
UNIT_PRICE double,
WEIGHT double,
BASE_AMOUNT double,
DeptId int,
PromoPrice double,
PromoType string,
SubDeptId int,
STORE_DESCRIPTION string,
STORE_TYPE string,
FLG_FRANCHISE string,
CONCEPT_NAME string,
DESCRIPTIONLEVEL1 string
)
COMMENT 'POS Transaction- Reporting'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/event/trans'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

--MSMQ Transaction Aggregation Sales Hourly
drop table if exists ${hivevar:POS_ENV}.event_pos_store_sales_hourly;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_store_sales_hourly(
STORE_ID int,
TRANSACTION_TOTAL double,
SALE_DATE date,
SALE_HOUR string
)
COMMENT 'Real Time events- SALES Aggregate table'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/event/storesaleshourly'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

--MSMQ Transaction Aggregation Sales Hourly
drop table if exists ${hivevar:POS_ENV}.event_pos_product_sales_hourly;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_product_sales_hourly(
STORE_ID int,
ITEM_CODE string,
ITEM_COUNT int,
ITEM_TOTAL double,
SALE_DATE date
)
COMMENT 'Real Time events- PRODUCTS Aggregate table'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/event/productsaleshourly'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');
