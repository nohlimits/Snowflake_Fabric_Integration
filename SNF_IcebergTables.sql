// GENERAL
// ---------
// Set up Snowflake 

USE ROLE SYSADMIN;

-- For Better UI usage
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE WAREHOUSE iceberg_demo_wh
WAREHOUSE_TYPE = STANDARD
WAREHOUSE_SIZE = 'X-Small'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED=TRUE

USE WAREHOUSE iceberg_demo_wh;

CREATE OR REPLACE DATABASE iceberg_demo_db;
USE DATABASE iceberg_demo_db;

// ICEBERG
// ---------
// 1 – Set up connection to external volume (ADLS Gen 2) in Snowflake 

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL VOLUME adlsgen2_exvol_training_tenant
  STORAGE_LOCATIONS =
    (
      (
        NAME = '<NAME>'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://<ADLS GEN 2 ACCOUNT - BLOB!>/<CONTAINER>'
        AZURE_TENANT_ID = '<AZURE TENANT ID>'
      )
    );

// Azure Consent    
DESC EXTERNAL VOLUME adlsgen2_exvol_training_tenant;
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('adlsgen2_exvol_training_tenant');

// 2 - Write TPCH table to ADLS Gen 2 Iceberg 
CREATE OR REPLACE ICEBERG TABLE TPCH_SF10_CUSTOMER LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'adlsgen2_exvol_training_tenant'
    BASE_LOCATION = 'TPCH_SF10_CUSTOMER/';

INSERT INTO TPCH_SF10_CUSTOMER SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER;

SELECT COUNT(*) FROM TPCH_SF10_CUSTOMER

INSERT INTO TPCH_SF10_CUSTOMER (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES
    (15000001, 'Alice', '123 Main St', 1, '123-456-7890', 1000, 'BUILDING', 'N/A');

DELETE FROM TPCH_SF10_CUSTOMER
WHERE C_CUSTKEY = '15000001';

// 3 – Set up connection to external volume (OneLake) in Snowflake
CREATE OR REPLACE EXTERNAL VOLUME onelake_exvol_training_tenant
  STORAGE_LOCATIONS =
    (
      (
        NAME = '<NAME>'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://<ONELAKE LAKEHOUSE URL>/Files/<LANDING NAME>'
        AZURE_TENANT_ID = '<AZURE TENANT ID>'
      )
    );
    
// Azure Consent	
DESC EXTERNAL VOLUME onelake_exvol_training_tenant;
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('onelake_exvol_training_tenant');

// 4 – Write TPCH table to OneLake
CREATE OR REPLACE ICEBERG TABLE TPCH_SF10_LINEITEM LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'onelake_exvol_training_tenant'
    BASE_LOCATION = 'TPCH_SF10_LINEITEM/';

INSERT INTO TPCH_SF10_LINEITEM SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM;

SELECT TOP 100 * FROM TPCH_SF10_LINEITEM;

CREATE OR REPLACE ICEBERG TABLE TPCH_SF10_ORDERS LIKE SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'onelake_exvol_training_tenant'
    BASE_LOCATION = 'TPCH_SF10_ORDERS/';

INSERT INTO TPCH_SF10_ORDERS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS;

/* 
-- Clean up block within Snowflake

DROP WAREHOUSE IF EXISTS iceberg_demo_wh;
DROP DATABASE IF EXISTS iceberg_demo_db;
DROP EXTERNAL VOLUME IF EXISTS adlsgen2_exvol_training_tenant;
DROP EXTERNAL VOLUME IF EXISTS onelake_exvol_training_tenant;

*/

/*
// To write table from scratch (and not copy from Snowflake sample data):
CREATE OR REPLACE ICEBERG TABLE customer (
    c_custkey INTEGER,
    c_name STRING,
    c_address STRING,
    c_nationkey INTEGER,
    c_phone STRING,
    c_acctbal INTEGER,
    c_mktsegment STRING,
    c_comment STRING
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'onelake_exvol_training_tenant'
    BASE_LOCATION = 'customer/';

INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES
    (1, 'Alice', '123 Main St', 1, '123-456-7890', 1000, 'BUILDING', 'N/A'),
    (2, 'Bob', '456 Elm St', 2, '234-567-8901', 1500, 'AUTOMOBILE', 'Frequent buyer'),
    (3, 'Charlie', '789 Oak St', 1, '345-678-9012', 2000, 'ELECTRONICS', 'Loyal customer'),
    (4, 'David', '321 Maple St', 3, '456-789-0123', 1200, 'BUILDING', 'Interested in discounts'),
    (5, 'Eve', '654 Pine St', 2, '567-890-1234', 1600, 'AUTOMOBILE', 'Looking for new offers');
*/