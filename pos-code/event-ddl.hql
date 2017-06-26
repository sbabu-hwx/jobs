set hivevar:POS_ENV=qa;
create database if not exists ${hivevar:POS_ENV};

drop table ${hivevar:POS_ENV}.event_pos_raw_trans;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_raw_trans(
RAW_RECORD string,
LOYALTY_NUM string,
OPERATOR_ID string,
STORE_ID int,
TERMINAL_ID string,
TRANSACTION_ID int,
TRANSACTION_TOTAL double,
TRANS_DATE string, 
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
COMMENT 'Real Time POS event -Raw'
PARTITIONED BY (
  date_part string,
  hour_part string)
row format DELIMITED
fields terminated by '|'
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location "/${hivevar:POS_ENV}/landing/event/raw/trans";
  
  
  
  
drop table ${hivevar:POS_ENV}.event_pos_staging_trans;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.event_pos_staging_trans(
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

