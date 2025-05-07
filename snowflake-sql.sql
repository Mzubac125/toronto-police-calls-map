CREATE DATABASE TPS_ANAlYTICS;

CREATE SCHEMA TPS;

USE DATABASE TPS_ANALYTICS;
USE SCHEMA TPS;

CREATE STAGE IF NOT EXISTS TPS_STAGE;

CREATE TABLE tps_calls("Datetime" TIMESTAMP_NTZ,
    "Division" STRING,
    "EventType" STRING, 
    "Street" STRING,          
    "Intersection" STRING      
);

LIST @TPS_ANALYTICS.TPS.TPS_STAGE;

TRUNCATE table tps_calls;

select * from tps_calls
order by "Datetime" desc;