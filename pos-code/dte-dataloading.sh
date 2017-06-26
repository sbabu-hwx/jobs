#/bin/sh
posenv=$1
if [ -z "$posenv" ]
then
  echo "Environment value is required(qa/prod)."
  exit -1
fi
kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa-hdp01prod@HDP01PROD.LOVES.COM
nextdate=`date '+%Y%m%d' --date='1 hour ago'`
echo $nextdate
nexthour=`date '+%H' --date='1 hour ago'`
echo $nexthour

echo "hadoop fs -test -d /$1/landing/dte/raw/header/date_part=$nextdate/hour_part=$nexthour"
echo "hadoop fs -test -d /$1/landing/dte/raw/lineitem/date_part=$nextdate/hour_part=$nexthour"

#sudo su -l hdfs -c 'hadoop fs -chmod -R 777 /qa/landing/dte/'

hadoop fs -test -d /$posenv/landing/dte/raw/header/date_part=$nextdate/hour_part=$nexthour
result = $?
echo $result
if [[ $result -eq 0 ]];
then 
hive --hivevar NEXT_DATE=$nextdate --hivevar NEXT_HOUR=$nexthour --hivevar POS_ENV=$posenv -f dte-dataloading.hql
else
	echo "/$posenv/landing/dte/raw/header/date_part=$nextdate/hour_part=$nexthour does not exists "
fi
 
