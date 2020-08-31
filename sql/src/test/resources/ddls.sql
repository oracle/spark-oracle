-- tables created in sparktest schema
-- using connection sparktest/Performance_1234@mammoth_medium

CREATE TABLE sales_range_partition (
                                       product_id       NUMBER(6),
                                       customer_id      NUMBER,
                                       channel_id       CHAR(1),
                                       promo_id         NUMBER(6),
                                       sale_date        DATE,
                                       quantity_sold    INTEGER,
                                       amount_sold      NUMBER(10,2)
)
    PARTITION BY RANGE (sale_date)
(
    PARTITION sales_q1_2014 VALUES LESS THAN (TO_DATE('01-APR-2014','dd-MON-yyyy')),
    PARTITION sales_q2_2014 VALUES LESS THAN (TO_DATE('01-JUL-2014','dd-MON-yyyy')),
    PARTITION sales_q3_2014 VALUES LESS THAN (TO_DATE('01-OCT-2014','dd-MON-yyyy')),
    PARTITION sales_q4_2014 VALUES LESS THAN (TO_DATE('01-JAN-2015','dd-MON-yyyy'))
);

grant SELECT on sales_range_partition to public;

-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'SALES_RANGE_PARTITION', USER) from dual;

-- interval partitioned
create table interval_par_demo (
                          start_date        DATE,
                          store_id          NUMBER,
                          inventory_id      NUMBER(6),
                          qty_sold          NUMBER(3)
)
    PARTITION BY RANGE (start_date)
    INTERVAL(NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION interval_par_demo_p2 VALUES LESS THAN (TO_DATE('1-7-2007', 'DD-MM-YYYY')),
    PARTITION interval_par_demo_p3 VALUES LESS THAN (TO_DATE('1-8-2007', 'DD-MM-YYYY'))
);
grant SELECT on interval_par_demo to public;
-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'INTERVAL_PAR_DEMO', USER) from dual;

-- list partitioned
CREATE TABLE sales_by_region (
                                 item_id INTEGER,
                                 item_quantity INTEGER,
                                 store_name VARCHAR(30),
                                 state_code VARCHAR(2),
                                 sale_date DATE)
    PARTITION BY LIST (state_code)
(
    PARTITION region_east
        VALUES ('MA','NY','CT','NH','ME','MD','VA','PA','NJ'),
    PARTITION region_west
        VALUES ('CA','AZ','NM','OR','WA','UT','NV','CO'),
    PARTITION region_south
        VALUES ('TX','KY','TN','LA','MS','AR','AL','GA'),
    PARTITION region_central
        VALUES ('OH','ND','SD','MO','IL','MI','IA')
);
grant SELECT on sales_by_region to public;

-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'SALES_BY_REGION', USER) from dual;

-- multi-column list partition
CREATE TABLE sales_by_region_and_channel
(dept_number      NUMBER NOT NULL,
 dept_name        VARCHAR2(20),
 quarterly_sales  NUMBER(10,2),
 state            VARCHAR2(2),
 channel          VARCHAR2(1)
)
    PARTITION BY LIST (state, channel)
(
    PARTITION yearly_west_direct VALUES (('OR','D'),('UT','D'),('WA','D')),
    PARTITION yearly_west_indirect VALUES (('OR','I'),('UT','I'),('WA','I')),
    PARTITION yearly_south_direct VALUES (('AZ','D'),('TX','D'),('GA','D')),
    PARTITION yearly_south_indirect VALUES (('AZ','I'),('TX','I'),('GA','I')),
    PARTITION yearly_east_direct VALUES (('PA','D'), ('NC','D'), ('MA','D')),
    PARTITION yearly_east_indirect VALUES (('PA','I'), ('NC','I'), ('MA','I')),
    PARTITION yearly_north_direct VALUES (('MN','D'),('WI','D'),('MI','D')),
    PARTITION yearly_north_indirect VALUES (('MN','I'),('WI','I'),('MI','I')),
    PARTITION yearly_ny_direct VALUES ('NY','D'),
    PARTITION yearly_ny_indirect VALUES ('NY','I'),
    PARTITION yearly_ca_direct VALUES ('CA','D'),
    PARTITION yearly_ca_indirect VALUES ('CA','I'),
    PARTITION rest VALUES (DEFAULT)
    )
;
grant SELECT on sales_by_region_and_channel to public;

-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'SALES_BY_REGION_AND_CHANNEL', USER) from dual;

-- hash partitioned
create table hash_partition_table
( x number(10) )
    partition by hash ( x )
    partitions 8
;
grant SELECT on hash_partition_table to public;

-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'T', USER) from dual;

-- range-list
create table COMP
( tstamp       timestamp(6) not null,
  empno        number(10)   not null,
  ename        varchar2(10) not null,
  deptno       varchar2(10) not null
)
    PARTITION BY RANGE (TSTAMP)
    SUBPARTITION BY LIST (deptno)
(
    PARTITION p01 VALUES LESS THAN
        (TIMESTAMP '2010-01-01 00:00:00')
        (SUBPARTITION p01_sp1 VALUES (1),
                SUBPARTITION p01_sp2 VALUES (2),
                SUBPARTITION p01_sp3 VALUES (3),
                SUBPARTITION p01_sp4 VALUES (4)),
    PARTITION p02 VALUES LESS THAN
        (TIMESTAMP '2010-02-01 00:00:00')
        (SUBPARTITION p02_sp1 VALUES (1),
                SUBPARTITION p02_sp2 VALUES (2),
                SUBPARTITION p02_sp3 VALUES (3),
                SUBPARTITION p02_sp4 VALUES (4)),
    PARTITION p11 VALUES LESS THAN
        (TIMESTAMP '2010-11-01 00:00:00')
        (SUBPARTITION p11_sp1 VALUES (1,2),
                SUBPARTITION p11_sp2 VALUES (3,4)),
    PARTITION p12 VALUES LESS THAN
        (TIMESTAMP '2010-12-01 00:00:00'),
    PARTITION p13 VALUES LESS THAN
        (TIMESTAMP' 2011-01-01 00:00:00')
)
;
grant SELECT on COMP to public;

-- SELECT DBMS_METADATA.get_sxml ('TABLE', 'COMP', USER) from dual;

-- list partition with unknown values
CREATE TABLE sales_by_region_unknown_values
(dept_number         NUMBER NOT NULL,
 dept_name           VARCHAR2(20),
 quarterly_sales     NUMBER(10,2),
 state               VARCHAR2(2)
)
    PARTITION BY LIST (state)
(
    PARTITION yearly_north VALUES ('MN','WI','MI'),
    PARTITION yearly_south VALUES ('NM','TX','GA'),
    PARTITION yearly_east VALUES  ('MA','NY','NC'),
    PARTITION yearly_west VALUES  ('CA','OR','WA'),
    PARTITION unknown VALUES (DEFAULT)
)
;
grant SELECT on sales_by_region_unknown_values to public;


-- range list
CREATE TABLE quarterly_regional_sales (
                                          product_id       NUMBER(6),
                                          customer_id      NUMBER,
                                          channel_id       CHAR(1),
                                          promo_id         NUMBER(6),
                                          sale_date        DATE,
                                          quantity_sold    INTEGER,
                                          amount_sold      NUMBER(10,2),
                                          store_name       VARCHAR(30),
                                          state_code       VARCHAR(2)
)
    PARTITION BY RANGE (sale_date)
    SUBPARTITION BY LIST (state_code)
(PARTITION sales_q1_2014 VALUES LESS THAN (TO_DATE('01-APR-2014','dd-MON-yyyy'))
     (SUBPARTITION sales_q1_region_east_2014
             VALUES ('CT','MA','MD','ME','NH','NJ','NY','PA','VA'),
           SUBPARTITION sales_q1_region_west_2014
             VALUES ('AZ','CA','CO','NM','NV','OR','UT','WA'),
           SUBPARTITION sales_q1_region_south_2014
             VALUES ('AL','AR','GA','KY','LA','MS','TN','TX'),
           SUBPARTITION sales_q1_region_central_2014
             VALUES ('IA','IL','KS','MI','MO','ND','OH','OK','SD'),
           SUBPARTITION sales_q1_region_other_2014
             VALUES ('HI','PR'),
           SUBPARTITION sales_q1_region_null_2014
             VALUES (NULL),
           SUBPARTITION
             VALUES (DEFAULT)
          ),
 PARTITION sales_q2_2014 VALUES LESS THAN (TO_DATE('01-JUL-2014','dd-MON-yyyy'))
     (SUBPARTITION sales_q2_region_east_2014
             VALUES ('CT','MA','MD','ME','NH','NJ','NY','PA','VA'),
           SUBPARTITION sales_q2_region_west_2014
             VALUES ('AZ','CA','CO','NM','NV','OR','UT','WA'),
           SUBPARTITION sales_q2_region_south_2014
             VALUES ('AL','AR','GA','KY','LA','MS','TN','TX'),
           SUBPARTITION sales_q2_region_central_2014
             VALUES ('IA','IL','KS','MI','MO','ND','OH','OK','SD'),
           SUBPARTITION sales_q2_region_other_2014
             VALUES ('HI','PR'),
           SUBPARTITION sales_q2_region_null_2014
             VALUES (NULL),
           SUBPARTITION
             VALUES (DEFAULT)
          ),
 PARTITION sales_q3_2014 VALUES LESS THAN (TO_DATE('01-OCT-2014','dd-MON-yyyy'))
     (SUBPARTITION sales_q3_region_east_2014
             VALUES ('CT','MA','MD','ME','NH','NJ','NY','PA','VA'),
           SUBPARTITION sales_q3_region_west_2014
             VALUES ('AZ','CA','CO','NM','NV','OR','UT','WA'),
           SUBPARTITION sales_q3_region_south_2014
             VALUES ('AL','AR','GA','KY','LA','MS','TN','TX'),
           SUBPARTITION sales_q3_region_central_2014
             VALUES ('IA','IL','KS','MI','MO','ND','OH','OK','SD'),
           SUBPARTITION sales_q3_region_other_2014
             VALUES ('HI','PR'),
           SUBPARTITION sales_q3_region_null_2014
             VALUES (NULL),
           SUBPARTITION
             VALUES (DEFAULT)
          ),
 PARTITION sales_q4_2014 VALUES LESS THAN (TO_DATE('01-JAN-2015','dd-MON-yyyy'))
     (SUBPARTITION sales_q4_region_east_2014
             VALUES ('CT','MA','MD','ME','NH','NJ','NY','PA','VA'),
           SUBPARTITION sales_q4_region_west_2014
             VALUES ('AZ','CA','CO','NM','NV','OR','UT','WA'),
           SUBPARTITION sales_q4_region_south_2014
             VALUES ('AL','AR','GA','KY','LA','MS','TN','TX'),
           SUBPARTITION sales_q4_region_central_2014
             VALUES ('IA','IL','KS','MI','MO','ND','OH','OK','SD'),
           SUBPARTITION sales_q4_region_other_2014
             VALUES ('HI','PR'),
           SUBPARTITION sales_q4_region_null_2014
             VALUES (NULL),
           SUBPARTITION
             VALUES (DEFAULT)
          ),
 PARTITION sales_q1_2015 VALUES LESS THAN (TO_DATE('01-APR-2015','dd-MON-yyyy'))
     (SUBPARTITION sales_q1_region_east_2015
             VALUES ('CT','MA','MD','ME','NH','NJ','NY','PA','VA'),
           SUBPARTITION sales_q1_region_west_2015
             VALUES ('AZ','CA','CO','NM','NV','OR','UT','WA'),
           SUBPARTITION sales_q1_region_south_2015
             VALUES ('AL','AR','GA','KY','LA','MS','TN','TX'),
           SUBPARTITION sales_q1_region_central_2015
             VALUES ('IA','IL','KS','MI','MO','ND','OH','OK','SD'),
           SUBPARTITION sales_q1_region_other_2015
             VALUES ('HI','PR'),
           SUBPARTITION sales_q1_region_null_2015
             VALUES (NULL),
           SUBPARTITION
             VALUES (DEFAULT)
          )
)
;
grant SELECT on quarterly_regional_sales to public;

CREATE TABLE regions
( region_id      NUMBER
      CONSTRAINT region_id_nn NOT NULL
    ,                CONSTRAINT reg_id_pk
      PRIMARY KEY (region_id)
    , region_name    VARCHAR2(25)
);
grant SELECT on regions to public;

CREATE TABLE countries2
( country_id      CHAR(2)
      CONSTRAINT country_id_nn NOT NULL
    ,                 CONSTRAINT country_c_id_pk
      PRIMARY KEY (country_id)
    , country_name    VARCHAR2(40)
    , region_id       NUMBER
    ,                 CONSTRAINT countr_reg_fk
      FOREIGN KEY (region_id)
          REFERENCES regions (region_id)
)
    ORGANIZATION INDEX
;
grant SELECT on countries2 to public;


CREATE TABLE "mixed_case_name"(
    name varchar2(100)
);
grant SELECT on "mixed_case_name" to public;

CREATE TABLE "Mixed_case_name"(
    name varchar2(100)
);
grant SELECT on "Mixed_case_name" to public;

CREATE TABLE MIXED_CASE_NAME(
    name varchar2(100)
);
grant SELECT on MIXED_CASE_NAME to public;

-- create an external table
BEGIN
    DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
            table_name      =>'SPARK_TEST_T1',
            credential_name =>'OS_EXT_OCI',
            file_uri_list   =>'<oci object store uri>',
            format          => json_object('type' value 'parquet', 'schema' value 'first'),
            column_list     => 'name VARCHAR2(4000),
                        age NUMBER(10)');
END;
/
grant SELECT on SPARK_TEST_T1 to public;

