create EXTERNAL TABLE lineorder_opt( 
    lo_orderkey INT, 
    lo_linenumber INT, 
    lo_custkey INT, 
    lo_partkey INT, 
    lo_suppkey INT, 
    lo_orderdate INT, 
    lo_orderpriority STRING, 
    lo_shippriority STRING, 
    lo_quantity INT, 
    lo_extendedprice INT, 
    lo_ordertotalprice INT, 
    lo_discount INT, 
    lo_revenue INT, 
    lo_supplycost INT, 
    lo_tax INT, 
    lo_commitdate INT, 
    lo_shipmode STRING) 
     partitioned by (q1 INT, q2 INT, q3 INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
create EXTERNAL TABLE dwdate_opt( 
    d_datekey INT, 
    d_date STRING, 
    d_dayofweek STRING, 
    d_month STRING, 
    d_year INT, 
    d_yearmonthnum INT, 
    d_yearmonth STRING, 
    d_daynuminweek INT, 
    d_daynuminmonth INT, 
    d_daynuminyear INT, 
    d_monthnuminyear INT, 
    d_weeknuminyear INT, 
    d_sellingseason STRING, 
    d_lastdayinweekfl STRING, 
    d_lastdayinmonthfl STRING, 
    d_holidayfl STRING, 
    d_weekdayfl STRING) 
    partitioned by(q1 INT, q2 INT, q3 INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
    CREATE EXTERNAL TABLE part_opt (
        p_partkey INT,
        p_name STRING,
        p_mfgr STRING,
        p_category STRING,
        p_brand1 STRING,
        p_color STRING,
        p_type STRING,
        p_size INT,
        p_container STRING)
        partitioned by (q1 INT, q2 INT, q3 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
CREATE EXTERNAL TABLE supplier_opt (
        s_suppkey   INT,
        s_name STRING,
        s_address STRING,
        s_city STRING,
        s_nation STRING,
        s_region STRING,
        s_phone STRING)
        partitioned by (q1 INT, q2 INT, q3 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
CREATE EXTERNAL TABLE customer_opt(
        c_custkey INT,
        c_name STRING,
        c_address  STRING,
        c_city STRING,
        c_nation STRING,
        c_region STRING,
        c_phone STRING,
        c_mktsegment STRING)
        partitioned by (q1 INT, q2 INT, q3 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';


1.
select sum(lo_extendedprice*lo_discount) as revenue
from lineorder, dwdate
where lo_orderdate=d_datekey
and d_year=1997
and lo_discount between 1 and 3
and lo_quantity < 24;

insert into lineorder_opt partition (q1=1, q2=0, q3=0) select * from lineorder where lo_discount >= 1 and lo_discount <= 3 and lo_quantity < 24;
insert into dwdate_opt partition (q1=1, q2=0, q3=0) select * from dwdate where d_year = 1997;

select sum(t1.lo_extendedprice*t1.lo_discount) as revenue
from (select lo_extendedprice, lo_discount, lo_orderdate from lineorder_opt where q1=1 and q2=0 and q3=0) as t1,
(select d_datekey from dwdate_opt where q1=1 and q2=0 and q3=0) as t2
where t1.lo_orderdate=t2.d_datekey;

2.
select sum(lo_revenue), d_year, p_brand1 
from lineorder, dwdate, part, supplier 
where lo_orderdate = d_datekey 
and lo_partkey = p_partkey 
and lo_suppkey = s_suppkey 
and p_category = 'MFGR#12' 
and s_region = 'AMERICA' 
group by d_year, p_brand1 
order by d_year, p_brand1 
limit 500;

# insert into dwdate_opt partition (q1=0, q2=1, q3=0) select * from dwdate;
insert into part_opt partition (q1=0, q2=1, q3=0) select * from part where p_category = 'MFGR#12';
insert into supplier_opt partition (q1=0, q2=1, q3=0) select * from supplier where s_region = 'AMERICA';
insert into lineorder_opt partition (q1=0, q2=1, q3=0) 
    select lo_orderkey, 
    lo_linenumber, 
    lo_custkey, 
    lo_partkey, 
    lo_suppkey, 
    lo_orderdate, 
    lo_orderpriority, 
    lo_shippriority, 
    lo_quantity, 
    lo_extendedprice, 
    lo_ordertotalprice, 
    lo_discount, 
    lo_revenue, 
    lo_supplycost, 
    lo_tax, 
    lo_commitdate, 
    lo_shipmode from lineorder, (select p_partkey from part where p_category = 'MFGR#12') as t1,
                    (select s_suppkey from supplier where s_region = 'AMERICA') as t2 
    where lineorder.lo_partkey = t1.p_partkey
    and lineorder.lo_suppkey = t2.s_suppkey;
    

-- CREATE EXTERNAL TABLE q2_opt(
--         lo_revenue INT,
--         lo_orderdate INT,
--         lo_partkey INT,
--         lo_suppkey INT,
--         d_year INT,
--         d_datekey INT,
--         p_brand1 STRING,
--         p_partkey INT,
--         p_category STRING,
--         s_suppkey INT,
--         c_region STRING)
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';


select sum(t1.lo_revenue), t2.d_year, t3.p_brand1 
from (select * from lineorder_opt where q1=0 and q2=1 and q3=0) as t1, 
dwdate as t2, part as t3
where t1.lo_orderdate = t2.d_datekey
and t1.lo_partkey = t3.p_partkey
group by t2.d_year, t3.p_brand1 
order by t2.d_year, t3.p_brand1
limit 500;

3.
select c_city, s_city, d_year, sum(lo_revenue) as revenue 
from customer, lineorder, supplier, dwdate 
where lo_custkey = c_custkey 
and lo_suppkey = s_suppkey 
and lo_orderdate = d_datekey 
and (c_city='UNITED KI1' or c_city='UNITED KI5') 
and (s_city='UNITED KI1' or s_city='UNITED KI5') 
and d_yearmonth = 'Dec1997' 
group by c_city, s_city, d_year 
order by d_year asc, revenue desc 
limit 5;

insert into lineorder_opt partition (q1=0, q2=0, q3=1) 
    select lo_orderkey, 
    lo_linenumber, 
    lo_custkey, 
    lo_partkey, 
    lo_suppkey, 
    lo_orderdate, 
    lo_orderpriority, 
    lo_shippriority, 
    lo_quantity, 
    lo_extendedprice, 
    lo_ordertotalprice, 
    lo_discount, 
    lo_revenue, 
    lo_supplycost, 
    lo_tax, 
    lo_commitdate, 
    lo_shipmode from lineorder, (select c_custkey from customer where c_city='UNITED KI1' or c_city='UNITED KI5') as t1,
                    (select s_suppkey from supplier where s_city='UNITED KI1' or s_city='UNITED KI5') as t2, 
                    (select d_datekey from dwdate where d_yearmonth = 'Dec1997') as t3
    where lineorder.lo_custkey = t1.c_custkey
    and lineorder.lo_suppkey = t2.s_suppkey
    and  lineorder.lo_orderdate = t3.d_datekey;
    
select c.c_city, s.s_city, d.d_year, sum(l.lo_revenue) as revenue 
from (select * from lineorder_opt where q1=0 and q2=0 and q3=1) as l, customer as c, supplier as s, dwdate as d 
where l.lo_custkey = c.c_custkey 
and l.lo_suppkey = s.s_suppkey 
and l.lo_orderdate = d.d_datekey 
group by c.c_city, s.s_city, d.d_year 
order by d.d_year asc, revenue desc 
limit 5;



Part3:

CREATE TABLE part (
  p_partkey             integer         not null sortkey,
  p_name                varchar(22)     not null,
  p_mfgr                varchar(6)      not null,
  p_category            varchar(7)      not null,
  p_brand1              varchar(9)      not null,
  p_color               varchar(11)     not null,
  p_type                varchar(25)     not null,
  p_size                integer         not null,
  p_container           varchar(10)     not null
) diststyle all;
CREATE TABLE supplier (
  s_suppkey             integer        not null sortkey distkey,
  s_name                varchar(25)    not null,
  s_address             varchar(25)    not null,
  s_city                varchar(10)    not null distkey,
  s_nation              varchar(15)    not null,
  s_region              varchar(12)    not null distkey,
  s_phone               varchar(15)    not null
);
CREATE TABLE customer (
  c_custkey             integer        not null sortkey distkey,
  c_name                varchar(25)    not null,
  c_address             varchar(25)    not null,
  c_city                varchar(10)    not null distkey,
  c_nation              varchar(15)    not null,
  c_region              varchar(12)    not null,
  c_phone               varchar(15)    not null,
  c_mktsegment      varchar(10)    not null
);
CREATE TABLE dwdate (
  d_datekey            integer       not null sortkey distkey,
  d_date               varchar(19)   not null,
  d_dayofweek         varchar(10)   not null,
  d_month           varchar(10)   not null,
  d_year               integer       not null distkey,
  d_yearmonthnum       integer           not null,
  d_yearmonth          varchar(8)       not null distkey,
  d_daynuminweek       integer       not null,
  d_daynuminmonth      integer       not null,
  d_daynuminyear       integer       not null,
  d_monthnuminyear     integer       not null,
  d_weeknuminyear      integer       not null,
  d_sellingseason      varchar(13)    not null,
  d_lastdayinweekfl    varchar(1)    not null,
  d_lastdayinmonthfl   varchar(1)    not null,
  d_holidayfl          varchar(1)    not null,
  d_weekdayfl          varchar(1)    not null
);
CREATE TABLE lineorder (
  lo_orderkey           integer         not null,
  lo_linenumber         integer         not null,
  lo_custkey            integer         not null distkey,
  lo_partkey            integer         not null distkey,
  lo_suppkey            integer         not null distkey,
  lo_orderdate          integer         not null distkey,
  lo_orderpriority      varchar(15)     not null,
  lo_shippriority       varchar(1)      not null,
  lo_quantity           integer         not null sortkey,
  lo_extendedprice      integer         not null,
  lo_ordertotalprice    integer         not null,
  lo_discount           integer         not null sortkey,
  lo_revenue            integer         not null,
  lo_supplycost         integer         not null,
  lo_tax                integer         not null,
  lo_commitdate         integer         not null,
  lo_shipmode           varchar(10)     not null
);


