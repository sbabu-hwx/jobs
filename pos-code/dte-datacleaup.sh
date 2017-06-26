#/bin/sh
posenv=$1
if [ -z "$posenv" ]
then
  echo "Environment value is required(qa/prod)."
  exit -1
fi
deletedate=$2
if [ -z "$deletedate" ]
then
  echo "Delete date value is required"
  exit -1
fi
#hadoop fs -rmr /$posenv/landing/dte/raw/header/date_part=$deletedate/*
#hadoop fs -rmr /$posenv/landing/dte/raw/lineitem/date_part=$deletedate/*
hadoop fs -rmr /$posenv/landing/dte/staging/trans/date_part=$deletedate/*
hadoop fs -rm /$posenv/reporting/dte/reporting/trans/date_part=$deletedate/*
hadoop fs -rm /$posenv/reporting/dte/storesaleshourly/date_part=$deletedate/*
hadoop fs -rm /$posenv/reporting/dte/productsaleshourly/date_part=$deletedate/*
#hive --hivevar NEXT_DATE=20170615 --hivevar PREVIOUS_DATE=20170614 --hivevar NEXT_HOUR=09 --hivevar POS_ENV=qa -f event-dataloading.hql
