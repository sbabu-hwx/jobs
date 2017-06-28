set hivevar:POS_ENV=dev;


CREATE DATABASE IF NOT EXISTS ${hivevar:POS_ENV};
use ${hivevar:POS_ENV};

drop table ${hivevar:POS_ENV}.sitereporting_raw_header;
CREATE EXTERNAL TABLE ${hivevar:POS_ENV}.sitereporting_raw_header(
  FacilityDimensionId int,
  FacilityId int,
  SiteId int,
  Name string,
  PreferredName string,
  Number int,
  FacilityStatusId int,
  FacilityStatusDescription string,
  FacilitySubtypeId int,
  FacilitySubtypeName string,
  FacilityTypeId int,
  FacilityTypeName string,
  OpenDate date,
  ClosedDate date,
  MatureDate date,
  PhysicalAddressId int,
  PhysicalAddress1 string,
  PhysicalAddress2 string,
  PhysicalAddressCity string,
  PhysicalAddressState string,
  PhysicalAddressZip string,
  PhysicalAddressCounty string,
  MailingAddressId int,
  MailingAddress1 string,
  MailingAddress2 string,
  MailingAddressCity string,
  MailingAddressState string,
  MailingAddressZip string,
  MailingAddressCounty string,
  MainPhoneNumberContactMethodId int,
  MainPhoneNumber string,
  MainEmailAddressContactMethodId int,
  MainEmailAddress string,
  FaxContactMethodId int,
  FaxNumber string,
  TireCareCellPhoneNumberContactMethodId int,
  TireCareCellPhoneNumber string,
  RoadsidePhoneNumberContactMethodId int,
  RoadsidePhoneNumber string,
  FacilityManagerId int,
  FacilityManagerFirstName string,
  FacilityManagerLastName string,
  RecruiterId int,
  RecruiterFirstName string,
  RecruiterLastName string,
  AuditorId int,
  AuditorFirstName string,
  AuditorLastName string,
  FacilityAreaId int,
  FacilityAreaNumber string,
  FacilityAreaManagerId int,
  FacilityAreaManagerFirstName string,
  FacilityAreaManagerLastName string,
  FacilityMaintenanceManagerId int,
  FacilityMaintenanceManagerFirstName string,
  FacilityMaintenanceManagerLastName string,
  HasDrinkBar BOOLEAN,
  HasFountain BOOLEAN,
  ValidFrom date,
  ValidTo date,
  IsCurrent BOOLEAN,
  InsertedDateTimeUtc date,
  UpdatedDateTimeUtc date,
  IsLegacyInline BOOLEAN
)
COMMENT 'SiteReporting Detail Raw'
PARTITIONED BY (
FacilityAreaId string)
row format DELIMITED
fields terminated by ','
lines terminated by '\n'
null DEFINED as ''
  STORED as TEXTFILE
  location '/${hivevar:POS_ENV}/landing/sitereporting/raw/header';
