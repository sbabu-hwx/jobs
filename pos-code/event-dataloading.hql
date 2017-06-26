!echo '${hivevar:POS_ENV}';
!echo '${hivevar:NEXT_DATE}';
!echo '${hivevar:NEXT_HOUR}';
!echo '${hivevar:PREVIOUS_DATE}';

alter table ${hivevar:POS_ENV}.event_pos_raw_trans add IF NOT EXISTS partition (date_part='${hivevar:NEXT_DATE}',hour_part='${hivevar:NEXT_HOUR}');

--msck repair table ${hivevar:POS_ENV}.event_pos_raw_trans;

with newdata as
(select  
distinct(concat(RAW_RECORD,ITEM_CODE,LINE_ID)),
LOYALTY_NUM,
OPERATOR_ID,
STORE_ID div 10000 AS STORE_ID,
TERMINAL_ID,
TRANSACTION_ID,
TRANSACTION_TOTAL,
cast(from_unixtime(unix_timestamp(TRANS_DATE, 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss') as timestamp) AS TRANS_DATE, 
TRANS_TYPE,
Loyalty_Bonus_Points,
Loyalty_Message,
Loyalty_Points,
Loyalty_TransAmt,
AMOUNT,
ENTRY_METHOD,
BASE_UNIT_PRICE,
ITEM_CODE,
LINE_ID,
ITEM_NAME,
OLA_PRODUCT_CODE,  
PUMP_ID,
PUMP_SERVER_REF,
QUANTITY,
TAX_AMOUNT,
UNIT_PRICE,
WEIGHT,
BASE_AMOUNT,
DeptId,
PromoPrice,
PromoType,
SubDeptId
 from ${hivevar:POS_ENV}.event_pos_raw_trans where date_part='${hivevar:NEXT_DATE}' and hour_part='${hivevar:NEXT_HOUR}'),
 olddata as 
 (select  
STORE_ID,
TRANSACTION_ID,
TRANS_DATE,
TRANS_TYPE,
ITEM_CODE,
LINE_ID
 from ${hivevar:POS_ENV}.event_pos_staging_trans where date_part in ('${hivevar:PREVIOUS_DATE}', '${hivevar:NEXT_DATE}')),
 composite as 
 ( select
 n.LOYALTY_NUM as LOYALTY_NUM,
n.OPERATOR_ID as OPERATOR_ID,
n.STORE_ID as STORE_ID,
n.TERMINAL_ID TERMINAL_ID,
n.TRANSACTION_ID as TRANSACTION_ID,
n.TRANSACTION_TOTAL as TRANSACTION_TOTAL,
n.TRANS_DATE as TRANS_DATE, 
n.TRANS_TYPE as TRANS_TYPE,
n.Loyalty_Bonus_Points as Loyalty_Bonus_Points,
n.Loyalty_Message as Loyalty_Message,
n.Loyalty_Points as Loyalty_Points,
n.Loyalty_TransAmt as Loyalty_TransAmt,
n.AMOUNT as AMOUNT,
n.ENTRY_METHOD as ENTRY_METHOD,
n.BASE_UNIT_PRICE as BASE_UNIT_PRICE,
n.ITEM_CODE as ITEM_CODE,
n.LINE_ID as LINE_ID,
n.ITEM_NAME as ITEM_NAME,
n.OLA_PRODUCT_CODE as OLA_PRODUCT_CODE,  --BARCODE
n.PUMP_ID as PUMP_ID,
n.PUMP_SERVER_REF as PUMP_SERVER_REF,
n.QUANTITY as QUANTITY,
n.TAX_AMOUNT as TAX_AMOUNT,
n.UNIT_PRICE as UNIT_PRICE,
n.WEIGHT as WEIGHT,
n.BASE_AMOUNT as BASE_AMOUNT,
n.DeptId as DeptId,
n.PromoPrice as PromoPrice,
n.PromoType as PromoType,
n.SubDeptId as SubDeptId,
o.STORE_ID as O_STORE_ID,
o.TRANSACTION_ID as O_TRANSACTION_ID,
o.TRANS_DATE as O_TRANS_DATE,
o.TRANS_TYPE as O_TRANS_TYPE,
o.ITEM_CODE as O_ITEM_CODE,
o.LINE_ID as O_LINE_ID
from newdata n
 LEFT OUTER JOIN olddata o on (n.TRANS_DATE = o.TRANS_DATE and n.STORE_ID=o.STORE_ID and n.TRANSACTION_ID= o.TRANSACTION_ID and n.TRANS_TYPE
  = o.TRANS_TYPE and n.ITEM_CODE = o.ITEM_CODE and n.LINE_ID = o.LINE_ID))
  INSERT INTO TABLE ${hivevar:POS_ENV}.event_pos_staging_trans PARTITION (date_part=${hivevar:NEXT_DATE},hour_part='${hivevar:NEXT_HOUR}')
   select 
 LOYALTY_NUM,
OPERATOR_ID,
STORE_ID,
TERMINAL_ID,
TRANSACTION_ID,
TRANSACTION_TOTAL,
TRANS_DATE,
TRANS_TYPE,
Loyalty_Bonus_Points,
Loyalty_Message,
Loyalty_Points,
Loyalty_TransAmt,
AMOUNT,
ENTRY_METHOD,
BASE_UNIT_PRICE,
ITEM_CODE,
LINE_ID,
ITEM_NAME,
OLA_PRODUCT_CODE,
PUMP_ID,
PUMP_SERVER_REF,
QUANTITY,
TAX_AMOUNT,
UNIT_PRICE,
WEIGHT,
BASE_AMOUNT,
DeptId,
PromoPrice,
PromoType,
SubDeptId
 from composite c
where c.O_TRANS_DATE is null and c.O_STORE_ID is null and c.O_TRANSACTION_ID is null and c.O_TRANS_TYPE is null and c.O_ITEM_CODE is null and c.O_LINE_ID  is null;



INSERT INTO TABLE ${hivevar:POS_ENV}.event_pos_trans PARTITION (date_part=${hivevar:NEXT_DATE})
Select 
LOYALTY_NUM,
OPERATOR_ID,
STORE_ID,
TERMINAL_ID,
TRANSACTION_ID,
TRANSACTION_TOTAL,
TRANS_DATE,
TRANS_TYPE,
Loyalty_Bonus_Points,
Loyalty_Message,
Loyalty_Points,
Loyalty_TransAmt,
AMOUNT,
ENTRY_METHOD,
BASE_UNIT_PRICE,
ITEM_CODE,
LINE_ID,
ITEM_NAME,
OLA_PRODUCT_CODE,  --BARCODE
PUMP_ID,
PUMP_SERVER_REF,
QUANTITY,
TAX_AMOUNT,
UNIT_PRICE,
WEIGHT,
BASE_AMOUNT,
DeptId,
PromoPrice,
PromoType,
SubDeptId,
s.store_description AS STORE_DESCRIPTION,
s.STORE_TYPE AS STORE_TYPE,
s.FLG_FRANCHISE AS FLG_FRANCHISE, 
s.CONCEPT_NAME AS CONCEPT_NAME,
s.DESCRIPTIONLEVEL1 AS DESCRIPTIONLEVEL1 
from ${hivevar:POS_ENV}.event_pos_staging_trans t
LEFT OUTER JOIN ${hivevar:POS_ENV}.dim_store s  on (t.store_id = s.store_code) 
where date_part='${hivevar:NEXT_DATE}' and hour_part='${hivevar:NEXT_HOUR}' order by STORE_ID, TRANSACTION_ID, ITEM_CODE;



with h as
( select distinct(TRANSACTION_ID) AS TILL_NUM, STORE_ID, TRANSACTION_TOTAL as TOTAL,cast( TRANS_DATE as date) as SALE_DATE, hour(TRANS_DATE) as hour from  ${hivevar:POS_ENV}.event_pos_staging_trans t where t.date_part='${hivevar:NEXT_DATE}' and t.hour_part='${hivevar:NEXT_HOUR}' )
INSERT INTO TABLE ${hivevar:POS_ENV}.event_pos_store_sales_hourly PARTITION (date_part=${hivevar:NEXT_DATE})
select store_id, sum( total) as TRANSACTION_TOTAL, SALE_DATE, hour as SALE_HOUR from h group by SALE_DATE, store_id, hour order by store_id;



INSERT INTO TABLE ${hivevar:POS_ENV}.event_pos_product_sales_hourly PARTITION (date_part=${hivevar:NEXT_DATE})
select STORE_ID, ITEM_CODE, sum(QUANTITY) as ITEM_COUNT, sum(UNIT_PRICE * QUANTITY) as ITEM_TOTAL,cast(TRANS_DATE as date) as SALE_DATE from  ${hivevar:POS_ENV}.event_pos_staging_trans t where t.date_part='${hivevar:NEXT_DATE}' and t.hour_part='${hivevar:NEXT_HOUR}' group by TRANS_DATE, STORE_ID, ITEM_CODE order by store_id;
