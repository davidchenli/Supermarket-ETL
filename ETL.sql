-- create the server connection

 drop server if exists GBA424;
 
 create SERVER GBA424
FOREIGN DATA WRAPPER mysql
OPTIONS (USER 'gba424_student',password "Student!2020", HOST 'gba424.simon.rochester.edu', DATABASE 'mobilevisits');

-- create the federated table

drop database if exists mobile;

create database mobile;
use mobile;

drop table  if exists chains;

create TABLE chains (  `chain` varchar(20) NOT NULL,  PRIMARY KEY (`chain`)) 
ENGINE=FEDERATED 
CONNECTION='GBA424/chains'
COLLATE=utf8mb4_0900_ai_ci;

drop table  if exists venues;

create TABLE venues (`venueID` int(11) NOT NULL, 
`chain` varchar(20) NOT NULL,
`latitude` decimal(16,13) NOT NULL, 
`longitude` decimal(16,13) NOT NULL, 
PRIMARY KEY (`venueID`)) 
ENGINE=FEDERATED 
CONNECTION='GBA424/venues'
COLLATE=utf8mb4_0900_ai_ci;

drop table  if exists visits;

 CREATE TABLE visits (
  `userID` int(11) NOT NULL,
  `venueID` int(11) NOT NULL
) 
ENGINE=FEDERATED 
CONNECTION='GBA424/visits'
COLLATE=utf8mb4_0900_ai_ci;

drop table  if exists users;

CREATE TABLE users (
  `userID` int(11) NOT NULL,
  `latitude` decimal(16,13) NOT NULL,
  `longitude` decimal(16,13) NOT NULL,
  UNIQUE KEY `idx_users_userID` (`userID`)
) ENGINE=FEDERATED 
CONNECTION='GBA424/users'
 COLLATE=utf8mb4_0900_ai_ci;

 drop table if exists temp,temp2,temp3,temp4;

 create table temp as (select * from users);
 
 create table temp2 as (select * from visits);
 
 create table temp3 as (select* from venues);
 
 create table temp4 as (
 select temp.userID,temp3.chain,
 temp.latitude lat1,temp.longitude lon1,
 temp3.latitude lat2,temp3.longitude lon2,
 count( temp3.venueID) as visits 
 from temp2 join temp on temp.userID= temp2.userID
  join temp3 on temp2.venueID = temp3.venueID
  group by temp.userID , temp3.venueID);
  
drop table if exists warehouse;

create table warehouse as(
 select userID,chain,
 case when
 if(sqrt(test)>0 &sqrt(1-test)>0, 2*atan(sqrt(test)/sqrt(1 - test)), 2*atan2(sqrt(test), sqrt(1 - test)))*6371/1.6 >=5
 then "long"
 else "short" 
 end as distance_type,sum(visits) as visits
 from (
select userID,chain,visits,
(sin(((lat2- lat1)*(pi()/180))/2))*(sin(((lat2 - lat1)*(pi()/180))/2)) 
+ cos(lat1*(pi()/180))*cos(lat2*(pi()/180))*(sin(((lon2- lon1)*(pi()/180))/2))*(sin(((lon2- lon1)*(pi()/180))/2))
  as test from temp4) as o 
group by userID,chain,distance_type);

drop table temp,temp2,temp3,temp4;

-- total customer visits -- 
select chain, count(customers) as customers, sum(visits) as visits from(
  select chain,userID as customers, sum(visits) as visits from warehouse group by userID,chain ) as i group by chain with rollup ;
  
-- distance
select chain,distance_type ,sum(visits) as visits from warehouse group by chain,distance_type with rollup;

-- loyalty
select chain, count(customers) as loyal_customer from(
select chain, userID as customers  from warehouse  group by userID having count(distinct chain)=1) as i  group by chain with rollup;

-- frequency 
 select count(userID) as total,frequency from(
select userID  , case when sum(visits) >2 then "frequent" else "non_frequent" end as frequency from warehouse group by userID)as i 
group by frequency;


-- frequency by chain
select chain,frequency,count(userID) as total from(
select userID,chain  , case when sum(visits) >2 then "frequent" else "non_frequent" end as frequency from warehouse group by userID,chain)as i
 group by chain,frequency with rollup;

  
  -- cross shopper
  select chain,count(distinct i.userID) as cross_shopper from(select userID,chain from warehouse group by userID, chain)as a join (select userID from warehouse where chain ="AbleWare") as i on a.userID = i.userID group by chain;
  select chain,count(distinct i.userID) as cross_shopper from(select userID,chain from warehouse group by userID, chain)as a join (select userID from warehouse where chain ="BuildInc") as i on a.userID = i.userID group by chain;
  select chain,count(distinct i.userID) as cross_shopper from(select userID,chain from warehouse group by userID, chain)as a join (select userID from warehouse where chain ="Collards") as i on a.userID = i.userID group by chain;
  select chain,count(distinct i.userID) as cross_shopper from(select userID,chain from warehouse group by userID, chain)as a join (select userID from warehouse where chain ="DepotInc") as i on a.userID = i.userID group by chain;
  select chain,count(distinct i.userID) as cross_shopper from(select userID,chain from warehouse group by userID, chain)as a join (select userID from warehouse where chain ="ExcelInc") as i on a.userID = i.userID group by chain;

 select chain,sum( a.visits) as cross_visit from(select userID,chain,sum(visits) as visits from warehouse group by userID, chain)as a where userID in (select distinct userID from warehouse where chain ="AbleWare") group by chain;
  select chain,sum( a.visits) as cross_visit from(select userID,chain,sum(visits) as visits from warehouse group by userID, chain)as a where userID in (select distinct userID from warehouse where chain ="BuildInc") group by chain;
  select chain,sum( a.visits) as cross_visit from(select userID,chain,sum(visits) as visits from warehouse group by userID, chain)as a where userID in (select distinct userID from warehouse where chain ="Collards") group by chain;
  select chain,sum( a.visits) as cross_visit from(select userID,chain,sum(visits) as visits from warehouse group by userID, chain)as a where userID in (select distinct userID from warehouse where chain ="DepotInc") group by chain;
  select chain,sum( a.visits) as cross_visit from(select userID,chain,sum(visits) as visits from warehouse group by userID, chain)as a where userID in (select distinct userID from warehouse where chain ="ExcelInc") group by chain;
  
  
  
  
  
  
  -- Segmentation by loyalty & frequency
  select type,visits,count(userID) as number from(
select userID,case when sum(visits)>2 then "freq" else "nonfreq" end as visits, 
case when count(distinct chain)>1 then "cross" else "loyal" end as type from  warehouse group by userID) as  i 
group by type,visits with rollup;

select type,"loyal",count(userID) as total from(
select if(visits>2,"freq","non_freq") as type,userID from(
select userID, chain, sum(visits) as visits from warehouse where userID in  (select userID from warehouse  group by userID having count(distinct chain)=1)group by userID,chain) as i) as ii group by type
union
select type,"cross",count(userID) as total from(
select if(visits>2,"freq","non_freq") as type,userID from(
select userID, chain, sum(visits) as visits from warehouse where userID not in  (select userID from warehouse  group by userID having count(distinct chain)=1)group by userID,chain) as i) as ii group by type ;


  select "loyal",chain, frequency,count(customers) total from(
select chain, userID as customers ,case when sum(visits) >2 then "frequent" else "non_frequent" end as frequency from warehouse  group by userID having count(distinct chain)=1) as i group by chain,frequency with rollup
union
select "cross",chain, frequency,count(customers) as total from(
select chain,userID as customers,case when visits >2 then "frequent" else "non_frequent" end as frequency from warehouse where userID not in  (select userID from warehouse  group by userID having count(distinct chain)=1)group by userID,chain
) as i group by chain,frequency with rollup; 

