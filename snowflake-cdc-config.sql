/******** INITIALIZATION ********/

create database payments;
use payments;

-- create a named stage for our local cdc files
create stage cdc_stage
    encryption = (TYPE = 'SNOWFLAKE_SSE')
    file_format = (type = json);


-- before we explore the data we need to upload our files
-- from the databases menu in the left-hand navigation bar
-- navigate to PAYMENTS > PUBLIC > Stages > CDC_STAGE 
-- click the "+ Files" button in the upper-right hand corner
-- and upload the files from your local storage bucket



/******** INVESTIGATION ********/

list @cdc_stage pattern = '.*\.RESOLVED$';

select $1 as data,
       metadata$filename as filename,
       metadata$file_row_number as file_row_number,
       metadata$file_content_key as file_content_key,
       metadata$file_last_modified as file_last_modified,
       metadata$start_scan_time as start_scan_time,
from @cdc_stage
where metadata$filename like '%.RESOLVED'
limit 10;

-- we're using logic embedded in the filename to determine the resolve time for a record
-- by extracting the data from staging with this field we can engineer bitemporal records
-- using the mvcc timestamp in the file (single table) and the resolve time (entire change feed)
-- however, an individual record could be updated multiple times in a single file
-- and without further information the best we can accomplish is to apply a seq order for those cases
-- which we'll do AFTER we auto-ingest the data into internal tables with a pipe
-- BTW this is not an efficient query and will NOT run with more than a few thousand transactions
-- but it resolves each record against the appropriate changefeed checkpoint
-- and also captures the mvcc timestamp on the file for multiple file updates between checkpoints
-- however, this is just to understand the bitemporal nature of the change feed
-- after we ingest the data we only care about latest snapshot for each resolved timestamp
-- and we can make the resolve time as large or as small as necessary for our specific purposes

-- DO NOT RUN BELOW QUERY IF THERE ARE MORE THAN A FEW THOUSAND TRANSACTION RECORDS
with checkpoint as (
    select $1:resolved::number(29, 10) as resolved,
           split_part(metadata$filename, '.', 1) as timestamp
    from @cdc_stage
    where metadata$filename like '%.RESOLVED'
), address as (
    select addr.$1:after:id::varchar(64) as addr_id,
           addr.$1:after:acct_num::number(19, 0) as acct_num,
           addr.$1:after:street::varchar(64) as street,
           addr.$1:after:zip::number(9, 0) as zip,
           addr.$1:after:lat::number(11, 8) as lat,
           addr.$1:after:lng::number(11, 8) as lng,
           cp.resolved,
           cp.timestamp,
           split_part(metadata$filename, '.', 1) as addr_timestamp,
           addr.$1:mvcc_timestamp::number(29, 10) as mvcc_timestamp,
           addr.$1:updated::number(29, 10) as updated
    from @cdc_stage addr, checkpoint cp
    where metadata$filename like '%-address-%'
      and cp.timestamp = (select min(timestamp)
                          from checkpoint
                          where timestamp >= split_part(metadata$filename, '.', 1))
      and addr_timestamp <= cp.timestamp
      and addr_timestamp > (select max(timestamp)
                            from checkpoint
                            where timestamp < split_part(metadata$filename, '.', 1))
), city_loc as (
    select city.$1:after:id::varchar(64) as city_id,
           city.$1:after:zip::number(9, 0) as zip,
           city.$1:after:city::varchar(32) as city,
           city.$1:after:state::varchar(2) as state,
           city.$1:after:city_pop::number(9, 0) as city_pop,
           cp.resolved,
           cp.timestamp,
           split_part(metadata$filename, '.', 1) as city_timestamp,
           city.$1:mvcc_timestamp::number(29, 10) as mvcc_timestamp,
           city.$1:updated::number(29, 10) as updated
    from @cdc_stage city, checkpoint cp
    where metadata$filename like '%-city_loc-%'
      and cp.timestamp = (select min(timestamp)
                          from checkpoint
                          where timestamp >= split_part(metadata$filename, '.', 1))
      and city_timestamp <= cp.timestamp
      and city_timestamp > (select max(timestamp)
                            from checkpoint
                            where timestamp < split_part(metadata$filename, '.', 1))
), customer as (
    select cust.$1:after:id::varchar(64) as cust_id,
           cust.$1:after:ssn::varchar(16) as ssn,
           cust.$1:after:cc_num::number(19, 0) as cc_num,
           cust.$1:after:first::varchar(16) as first,
           cust.$1:after:last::varchar(16) as last,
           cust.$1:after:gender::varchar(1) as gender,
           cust.$1:after:job::varchar(64) as job,
           cust.$1:after:dob::date as dob,
           cust.$1:after:acct_num::number(19, 0) as acct_num,
           cust.$1:after:profile::varchar(32) as profile,
           cp.resolved,
           cp.timestamp,
           split_part(metadata$filename, '.', 1) as cust_timestamp,
           cust.$1:mvcc_timestamp::number(29, 10) as mvcc_timestamp,
           cust.$1:updated::number(29, 10) as updated
    from @cdc_stage cust, checkpoint cp
    where metadata$filename like '%-customer-%'
      and cp.timestamp = (select min(timestamp)
                          from checkpoint
                          where timestamp >= split_part(metadata$filename, '.', 1))
      and cust_timestamp <= cp.timestamp
      and cust_timestamp > (select max(timestamp)
                            from checkpoint
                            where timestamp < split_part(metadata$filename, '.', 1))
), merchant as (
    select merc.$1:after:id::varchar(64) as merch_id,
           merc.$1:after:merchant::varchar(64) as merchant,
           merc.$1:after:merch_lat::number(11, 8) as merch_lat,
           merc.$1:after:merch_lng::number(11, 8) as merch_lng,
           cp.resolved,
           cp.timestamp,
           split_part(metadata$filename, '.', 1) as merc_timestamp,
           merc.$1:mvcc_timestamp::number(29, 10) as mvcc_timestamp,
           merc.$1:updated::number(29, 10) as updated
    from @cdc_stage merc, checkpoint cp
    where metadata$filename like '%-merchant-%'
      and cp.timestamp = (select min(timestamp)
                          from checkpoint
                          where timestamp >= split_part(metadata$filename, '.', 1))
      and merc_timestamp <= cp.timestamp
      and merc_timestamp > (select max(timestamp)
                            from checkpoint
                            where timestamp < split_part(metadata$filename, '.', 1))
), transaction as (
    select tran.$1:after:id::varchar(64) as tran_id,
           tran.$1:after:cc_num::number(19, 0) as cc_num,
           tran.$1:after:merch_id::varchar(64) as merch_id,
           tran.$1:after:trans_num::varchar(32) as trans_num,
           tran.$1:after:trans_date::date as trans_date,
           tran.$1:after:trans_time::time as trans_time,
           tran.$1:after:unix_time::number(10, 0) as unix_time,
           tran.$1:after:category::varchar(16) as category,
           tran.$1:after:amt::number(12, 2) as amt,
           tran.$1:after:is_fraud::boolean as is_fraud,
           cp.resolved,
           cp.timestamp,
           split_part(metadata$filename, '.', 1) as tran_timestamp,
           tran.$1:mvcc_timestamp::number(29, 10) as mvcc_timestamp,
           tran.$1:updated::number(29, 10) as updated
    from @cdc_stage tran, checkpoint cp
    where metadata$filename like '%-transaction-%'
      and cp.timestamp = (select min(timestamp)
                          from checkpoint
                          where timestamp >= split_part(metadata$filename, '.', 1))
      and tran_timestamp <= cp.timestamp
      and tran_timestamp > (select max(timestamp)
                            from checkpoint
                            where timestamp < split_part(metadata$filename, '.', 1))
)
select cp.resolved, cp.timestamp, addr.addr_id, addr.street, addr.lat, addr.lng,
       city.city_id, city.city, city.state, city.zip, city.city_pop,
       cust.cust_id, cust.ssn, cust.cc_num, cust.first, cust.last,
       cust.gender, cust.job, cust.dob, cust.acct_num, cust.profile,
       merc.merch_id, merc.merchant, merc.merch_lat, merc.merch_lng,
       tran.tran_id, tran.trans_num, tran.trans_date, tran.trans_time,
       tran.unix_time, tran.category, tran.amt, tran.is_fraud
from checkpoint cp, address addr, city_loc city, customer cust, merchant merc, transaction tran
where addr.resolved = cp.resolved
  and city.zip = addr.zip
  and city.resolved = cp.resolved
  and cust.acct_num = addr.acct_num
  and cust.resolved = cp.resolved
  and merc.merch_id = tran.merch_id
  and merc.resolved = cp.resolved
  and tran.cc_num = cust.cc_num
  and tran.resolved = cp.resolved
limit 10;



/******** INGESTION ********/

-- we'll create a single table to ingest all the data from our change feed
-- and then we'll apply the logic from our investigation above in python
create table cdc_data (
    data variant not null,
    filename string not null,
    file_row_number int not null,
    file_content_key string(32) not null,
    file_last_modified date not null,
    start_scan_time date not null
);

-- and then create a pipe publish data from staging into our table
create pipe cdc_pipe as
copy into cdc_data
from (
    select $1 as data,
           metadata$filename as filename,
           metadata$file_row_number as file_row_number,
           metadata$file_content_key as file_content_key,
           metadata$file_last_modified as file_last_modified,
           metadata$start_scan_time as start_scan_time,
    from @cdc_stage
);

-- then we can refresh the pipe to start ingesting data
alter pipe cdc_pipe refresh;

-- wait a few minutes and query the cdc_data table for results
select count(*) from cdc_data;
select * from cdc_data limit 10;
