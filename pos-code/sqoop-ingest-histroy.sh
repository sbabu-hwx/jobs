#/bin/sh
kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa-hdp01prod@HDP01PROD.LOVES.COM
function pause(){
   read -p "$*"
}

file=$1
declare -A props
if [ -f "$file" ]
then
while IFS='=' read -r key value
  do
    key=$(echo $key | tr '.' '_')
    eval "props[${key}]='${value}'"
    echo $key =${props[${key}]}
  done < "$file"

else
  echo "$file not found."
fi

start_date=${props[start_date]}
days= ${props[days]}

while read NEXT_DATE
do
#   NEXT_DATE=$(date +%Y%m%d -d "$start_date + 1 day")
   echo "$NEXT_DATE"
sql='select * from ARCMADW.dbo.fct_sale_header where $CONDITIONS and date_key=$NEXT_DATE' 

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect jdbc:sqlserver://172.23.8.158 --username loyaltyTest --password-file /user/skotha/password --query "select * from ARCMADW.dbo.fct_sale_header where \$CONDITIONS and date_key="$NEXT_DATE --split-by STORE_CODE --target-dir  /${props["pos_env"]}/landing/history/raw/header/date_part=$NEXT_DATE

hive -e "ALTER TABLE ${props["pos_env"]}.fct_sale_raw_header ADD IF NOT EXISTS PARTITION (date_part='$NEXT_DATE');"
hive -e "MSCK repair table ${props["pos_env"]}.fct_sale_raw_header;"
hive -e "show partitions ${props["pos_env"]}.fct_sale_raw_header;"
 

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect jdbc:sqlserver://172.23.8.158 --username loyaltyTest --password-file /user/skotha/password --query 'select * from ARCMADW.dbo.fct_sale_line where $CONDITIONS and date_key='$NEXT_DATE --split-by STORE_CODE --target-dir  /${props["pos_env"]}/landing/history/raw/lineitem/date_part=$NEXT_DATE

hive -e "ALTER TABLE ${props["pos_env"]}.fct_sale_raw_line ADD IF NOT EXISTS PARTITION (date_part='$NEXT_DATE');"
hive -e "MSCK repair table ${props["pos_env"]}.fct_sale_raw_line;"
hive -e "show partitions ${props["pos_env"]}.fct_sale_raw_line;"
hive --hivevar NEXT_DATE=$NEXT_DATE --hivevar POS_ENV=${props["pos_env"]} -f staging-history.hql
done <${props["datesfile"]}
