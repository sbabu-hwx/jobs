set hivevar:POS_ENV=qa;

drop table ${hivevar:POS_ENV}.dte_pos_raw_header;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_raw_header(
RAW_RECORD string,
FILE_NAME string,
STORE_ID int,
DAY_BATCH_ID int,
TRANS_DATE string,
POS_ID int,
TILL_NUM int,
POS_BATCH_ID int,
SALE_DATE string,
TIME_OPEN string,
TIME_CLOSE string,
OPERATOR_ID string,
TICKET_TAX_AMOUNT double,
TRANSACTION_TOTAL double,
LOYALTY_NUM string,
TRANS_TYPE string,
STORE_LD_NUMBER string
)
COMMENT 'DTE POS header'
PARTITIONED BY (
  date_part string,
  hour_part string)
row format DELIMITED
fields terminated by '|'
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location "/${hivevar:POS_ENV}/landing/dte/raw/header";

  
drop table ${hivevar:POS_ENV}.dte_pos_raw_lineitem;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_raw_lineitem(
RAW_RECORD string,
FILE_NAME string,
STORE_ID int,
DAY_BATCH_ID int,
TRANS_DATE string,
POS_ID int,
TILL_NUM int,
POS_BATCH_ID int,
ITEM_CODE string,
ITEM_NAME string,
ITEM_ACTUAL_PRICE double,
ITEM_QUANTITY double,
ITEM_WEIGHT double,
ITEM_TAX_VAL double,
ITEM_ADJ_AMOUNT double,
PUMP_ID int,
OLA_PRODUCT_CODE string,  --BARCODE
ITEM_CODE_EX string,
PUMP_SERVER_REF string,
LINE_NUM int
)
COMMENT 'POS Line Item'
PARTITIONED BY (
  date_part string,
  hour_part string)
row format DELIMITED
fields terminated by '|'
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location "/${hivevar:POS_ENV}/landing/dte/raw/lineitem";
  
drop table ${hivevar:POS_ENV}.dte_pos_staging_trans;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_staging_trans(
H_FILE_NAME string,
H_STORE_ID int,
H_DAY_BATCH_ID int,
H_TRANS_DATE date,
H_POS_ID int,
H_TILL_NUM int,
H_POS_BATCH_ID int,
H_SALE_DATE date,
H_TIME_OPEN string,
H_TIME_CLOSE string,
H_OPERATOR_ID string,
H_TICKET_TAX_AMOUNT double,
H_TRANSACTION_TOTAL double,
H_LOYALTY_NUM string,
H_TRANS_TYPE string,
H_STORE_LD_NUMBER string,
L_FILE_NAME string,
L_STORE_ID int,
L_DAY_BATCH_ID int,
L_TRANS_DATE date,
L_POS_ID int,
L_TILL_NUM int,
L_POS_BATCH_ID int,
L_ITEM_CODE string,
L_ITEM_NAME string,
L_ITEM_ACTUAL_PRICE double,
L_ITEM_QUANTITY double,
L_ITEM_WEIGHT double,
L_ITEM_TAX_VAL double,
L_ITEM_ADJ_AMOUNT double,
L_PUMP_ID int,
L_OLA_PRODUCT_CODE string,  --BARCODE
L_ITEM_CODE_EX string,
L_PUMP_SERVER_REF string,
L_LINE_NUM int
)
COMMENT 'DTE OS Transaction Staging'
PARTITIONED BY (
  date_part string,
  hour_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/landing/dte/staging/trans'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

drop table ${hivevar:POS_ENV}.dte_pos_trans;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_trans(
H_FILE_NAME string,
H_STORE_ID int,
H_DAY_BATCH_ID int,
H_TRANS_DATE date,
H_POS_ID int,
H_TILL_NUM int,
H_POS_BATCH_ID int,
H_SALE_DATE date,
H_TIME_OPEN string,
H_TIME_CLOSE string,
H_OPERATOR_ID string,
H_TICKET_TAX_AMOUNT double,
H_TRANSACTION_TOTAL double,
H_LOYALTY_NUM string,
H_TRANS_TYPE string,
H_STORE_LD_NUMBER string,
L_FILE_NAME string,
L_STORE_ID int,
L_DAY_BATCH_ID int,
L_TRANS_DATE date,
L_POS_ID int,
L_TILL_NUM int,
L_POS_BATCH_ID int,
L_ITEM_CODE string,
L_ITEM_NAME string,
L_ITEM_ACTUAL_PRICE double,
L_ITEM_QUANTITY double,
L_ITEM_WEIGHT double,
L_ITEM_TAX_VAL double,
L_ITEM_ADJ_AMOUNT double,
L_PUMP_ID int,
L_OLA_PRODUCT_CODE string,  --BARCODE
L_ITEM_CODE_EX string,
L_PUMP_SERVER_REF string,
L_LINE_NUM int,
STORE_DESCRIPTION string,
STORE_TYPE string,
FLG_FRANCHISE string,
CONCEPT_NAME string,
DESCRIPTIONLEVEL1 string
)
COMMENT 'DTE OS Transaction Staging'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/dte/trans'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

drop table if exists ${hivevar:POS_ENV}.dte_pos_store_sales_hourly;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_store_sales_hourly(
STORE_ID int,
TRANSACTION_TOTAL double,
SALE_DATE date,
SALE_HOUR string
)
COMMENT 'SALES Aggregate table'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/dte/storesaleshourly'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

drop table if exists ${hivevar:POS_ENV}.dte_pos_product_sales_hourly;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.dte_pos_product_sales_hourly(
STORE_ID int,
ITEM_CODE string,
ITEM_COUNT int,
ITEM_TOTAL double,
SALE_DATE date
)
COMMENT 'PRODUCTS Aggregate table'
PARTITIONED BY (
  date_part string)
STORED AS ORC
LOCATION '/${hivevar:POS_ENV}/reporting/dte/productsaleshourly'
TBLPROPERTIES ("orc.compress"="SNAPPY", "orc.stripe.size"='67108864', "orc.row.index.stride"='5000');

