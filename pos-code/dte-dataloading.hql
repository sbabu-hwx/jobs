!echo '${hivevar:POS_ENV}';
!echo '${hivevar:NEXT_DATE}';
!echo '${hivevar:NEXT_HOUR}';

alter table ${hivevar:POS_ENV}.dte_pos_raw_header add IF NOT EXISTS partition (date_part='${hivevar:NEXT_DATE}',hour_part='${hivevar:NEXT_HOUR}');
alter table ${hivevar:POS_ENV}.dte_pos_raw_lineitem add IF NOT EXISTS partition (date_part='${hivevar:NEXT_DATE}',hour_part='${hivevar:NEXT_HOUR}');
--msck repair table ${hivevar:POS_ENV}.dte_pos_raw_header;
--msck repair table ${hivevar:POS_ENV}.dte_pos_raw_lineitem;  
  
  
INSERT INTO TABLE ${hivevar:POS_ENV}.dte_pos_staging_trans PARTITION (date_part=${hivevar:NEXT_DATE},hour_part='${hivevar:NEXT_HOUR}')
Select 
h.FILE_NAME as H_FILE_NAME,
h.STORE_ID div 10000 AS H_STORE_ID,
h.DAY_BATCH_ID AS H_DAY_BATCH_ID,
cast(from_unixtime(unix_timestamp(h.TRANS_DATE, 'MM/dd/yyyy'), 'yyyy-MM-dd') as date) AS H_TRANS_DATE,
h.POS_ID AS H_POS_ID,
h.TILL_NUM AS H_TILL_NUM,
h.POS_BATCH_ID AS H_POS_BATCH_ID,
cast(from_unixtime(unix_timestamp(h.SALE_DATE, 'MM/dd/yyyy'), 'yyyy-MM-dd') as date) AS H_SALE_DATE, 
h.TIME_OPEN as H_TIME_OPEN,
h.TIME_CLOSE as H_TIME_CLOSE,
h.OPERATOR_ID AS H_OPERATOR_ID,
h.TICKET_TAX_AMOUNT AS H_TICKET_TAX_AMOUNT,
h.TRANSACTION_TOTAL AS H_TRANSACTION_TOTAL, 
h.LOYALTY_NUM AS H_LOYALTY_NUM,
h.TRANS_TYPE AS H_TRANS_TYPE,
h.STORE_LD_NUMBER AS H_STORE_LD_NUMBER,
l.FILE_NAME as L_FILE_NAME,
l.STORE_ID AS L_STORE_ID,
l.DAY_BATCH_ID AS L_DAY_BATCH_ID,
cast(from_unixtime(unix_timestamp(l.TRANS_DATE, 'MM/dd/yyyy'), 'yyyy-MM-dd') as date) AS L_TRANS_DATE,
l.POS_ID AS L_POS_ID,
l.TILL_NUM AS L_TILL_NUM,
l.POS_BATCH_ID AS L_POS_BATCH_ID,
l.ITEM_CODE AS L_ITEM_CODE,
l.ITEM_NAME AS L_ITEM_NAME,
l.ITEM_ACTUAL_PRICE AS L_ITEM_ACTUAL_PRICE,
l.ITEM_QUANTITY AS L_ITEM_QUANTITY,
l.ITEM_WEIGHT AS L_ITEM_WEIGHT,
l.ITEM_TAX_VAL AS L_ITEM_TAX_VAL,
l.ITEM_ADJ_AMOUNT AS L_ITEM_ADJ_AMOUNT,
l.PUMP_ID AS L_PUMP_ID,
l.OLA_PRODUCT_CODE AS L_OLA_PRODUCT_CODE,
l.ITEM_CODE_EX AS L_ITEM_CODE_EX,
l.PUMP_SERVER_REF AS L_PUMP_SERVER_REF,
l.LINE_NUM as L_LINE_NUM
from ${hivevar:POS_ENV}.dte_pos_raw_header h
LEFT OUTER JOIN ${hivevar:POS_ENV}.dte_pos_raw_lineitem l on (h.TRANS_DATE=l.TRANS_DATE and h.store_id = l.store_id and h.TILL_NUM=l.TILL_NUM  and h.date_part='${hivevar:NEXT_DATE}' and h.hour_part='${hivevar:NEXT_HOUR}')
where h.date_part='${hivevar:NEXT_DATE}' and h.hour_part='${hivevar:NEXT_HOUR}';



INSERT INTO TABLE ${hivevar:POS_ENV}.dte_pos_trans PARTITION (date_part=${hivevar:NEXT_DATE})
Select 
t.H_FILE_NAME as H_FILE_NAME,
t.H_STORE_ID AS H_STORE_ID, 
t.H_DAY_BATCH_ID AS H_DAY_BATCH_ID,
t.H_TRANS_DATE AS H_TRANS_DATE,
t.H_POS_ID AS H_POS_ID,
t.H_TILL_NUM AS H_TILL_NUM,
t.H_POS_BATCH_ID AS H_POS_BATCH_ID,
t.H_SALE_DATE AS H_SALE_DATE,
t.H_TIME_OPEN as H_TIME_OPEN,
t.H_TIME_CLOSE as H_TIME_CLOSE,
t.H_OPERATOR_ID AS H_OPERATOR_ID,
t.H_TICKET_TAX_AMOUNT AS H_TICKET_TAX_AMOUNT,
t.H_TRANSACTION_TOTAL AS H_TRANSACTION_TOTAL, 
t.H_LOYALTY_NUM AS H_LOYALTY_NUM,
t.H_TRANS_TYPE AS H_TRANS_TYPE,
t.H_STORE_LD_NUMBER AS H_STORE_LD_NUMBER,
t.L_FILE_NAME as L_FILE_NAME,
t.L_STORE_ID AS L_STORE_ID,
t.L_DAY_BATCH_ID AS L_DAY_BATCH_ID,
t.L_TRANS_DATE AS L_TRANS_DATE,
t.L_POS_ID AS L_POS_ID,
t.L_TILL_NUM AS L_TILL_NUM,
t.L_POS_BATCH_ID AS L_POS_BATCH_ID,
t.L_ITEM_CODE AS L_ITEM_CODE,
t.L_ITEM_NAME AS L_ITEM_NAME,
t.L_ITEM_ACTUAL_PRICE AS L_ITEM_ACTUAL_PRICE,
t.L_ITEM_QUANTITY AS L_ITEM_QUANTITY,
t.L_ITEM_WEIGHT AS L_ITEM_WEIGHT,
t.L_ITEM_TAX_VAL AS L_ITEM_TAX_VAL,
t.L_ITEM_ADJ_AMOUNT AS L_ITEM_ADJ_AMOUNT,
t.L_PUMP_ID AS L_PUMP_ID,
t.L_OLA_PRODUCT_CODE AS L_OLA_PRODUCT_CODE,
t.L_ITEM_CODE_EX AS L_ITEM_CODE_EX,
t.L_PUMP_SERVER_REF AS L_PUMP_SERVER_REF,
t.L_LINE_NUM as L_LINE_NUM,
s.store_description AS STORE_DESCRIPTION,
s.STORE_TYPE AS STORE_TYPE,
s.FLG_FRANCHISE AS FLG_FRANCHISE, 
s.CONCEPT_NAME AS CONCEPT_NAME,
s.DESCRIPTIONLEVEL1 AS DESCRIPTIONLEVEL1 
from ${hivevar:POS_ENV}.dte_pos_staging_trans t
LEFT OUTER JOIN ${hivevar:POS_ENV}.dim_store s on (t.H_store_id = s.store_code)
where t.date_part='${hivevar:NEXT_DATE}' and t.hour_part='${hivevar:NEXT_HOUR}' order by H_STORE_ID, H_TILL_NUM, L_ITEM_CODE;

with h as
( select distinct(H_TILL_NUM) AS TILL_NUM, H_STORE_ID AS STORE_ID, H_TRANSACTION_TOTAL as TOTAL, H_SALE_DATE as SALE_DATE, substr(H_TIME_CLOSE,1,2) as hour from  ${hivevar:POS_ENV}.dte_pos_staging_trans t where t.date_part='${hivevar:NEXT_DATE}' and t.hour_part='${hivevar:NEXT_HOUR}' )
INSERT INTO TABLE ${hivevar:POS_ENV}.dte_pos_store_sales_hourly PARTITION (date_part=${hivevar:NEXT_DATE})
select store_id, sum( total) as TRANSACTION_TOTAL, SALE_DATE, hour as PROCESS_HOUR from h group by SALE_DATE, STORE_ID, hour order by store_id;

INSERT INTO TABLE ${hivevar:POS_ENV}.dte_pos_product_sales_hourly PARTITION (date_part=${hivevar:NEXT_DATE})
select H_store_id as STORE_ID, L_ITEM_CODE as ITEM_CODE, count(L_ITEM_CODE) as ITEM_COUNT, sum(L_ITEM_ACTUAL_PRICE) as ITEM_TOTAL,H_SALE_DATE as SALE_DATE from  ${hivevar:POS_ENV}.dte_pos_staging_trans t where t.date_part='${hivevar:NEXT_DATE}' and t.hour_part='${hivevar:NEXT_HOUR}' group by H_STORE_ID,L_ITEM_CODE,H_SALE_DATE order by H_store_id;
