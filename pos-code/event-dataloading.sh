#/bin/sh
kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa-hdp01prod@HDP01PROD.LOVES.COM
posenv=$1
if [ -z "$posenv" ]
then
  echo "Environment value is required(qa/prod)."
  exit -1
fi
nextdate=`date '+%Y%m%d' --date='1 hour ago'`
echo $nextdate
nexthour=`date '+%H' --date='1 hour ago'`
echo $nexthour

previousday=`date '+%Y%m%d' --date='1 day ago'`
echo $previousday

#sudo su -l hdfs -c 'hadoop fs -chmod -R 777 /$posenv/landing/event/raw'


echo "hadoop fs -test -d /$posenv/landing/event/raw/trans/date_part=$nextdate/hour_part=$nexthour"
hadoop fs -test -d /$posenv/landing/dte/event/trans/date_part=$nextdate/hour_part=$nexthour
result=$?
echo $result
if [[ $result -eq 0 ]] 
then 
echo "hive --hivevar NEXT_DATE=$nextdate --hivevar PREVIOUS_DATE=$previousday  --hivevar NEXT_HOUR=$nexthour --hivevar POS_ENV=$posenv -f event-dataloading.hql"
hive --hivevar NEXT_DATE=$nextdate --hivevar PREVIOUS_DATE=$previousday  --hivevar NEXT_HOUR=$nexthour --hivevar POS_ENV=$posenv -f event-dataloading.hql
else
	echo "/$posenv/landing/event/raw/header/date_part=$nextdate/hour_part=$nexthour does not exists "
fi
