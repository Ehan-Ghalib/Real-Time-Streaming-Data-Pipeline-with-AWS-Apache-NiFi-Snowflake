-- Create a schema for SCD
CREATE SCHEMA IF NOT EXISTS MANAGE_DB.SCD_DEMO;

-- DDL for Customer Dimension
CREATE OR REPLACE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM (
    CUSTOMER_ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR, 
    STREET VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    COUNTRY VARCHAR,
    HASH_TXT_SCD2 VARCHAR,
    HASH_TXT_SCD1 VARCHAR,
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATE_TS TIMESTAMP_NTZ,
    ACTIVE_IND VARCHAR,
    RCRD_STRT_DATE TIMESTAMP_NTZ,
    RCRD_END_DATE TIMESTAMP_NTZ
);

--DDL for Customer Dimension Stage table
CREATE OR REPLACE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_STG (
    CUSTOMER_ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR,
    STREET VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    COUNTRY VARCHAR,
    HASH_TXT_SCD2 VARCHAR,
    HASH_TXT_SCD1 VARCHAR,
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--DDL for Customer Dimension Raw table
CREATE OR REPLACE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_RAW (
    CUSTOMER_ID NUMBER,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    EMAIL VARCHAR,
    STREET VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    COUNTRY VARCHAR,
    HASH_TXT_SCD2 VARCHAR,
    HASH_TXT_SCD1 VARCHAR,
    INSERT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--view the storage integration
DESC integration s3_init;

--Add new storage location (list should have old & new though) to storage integration
ALTER STORAGE INTEGRATION S3_INIT
SET STORAGE_ALLOWED_LOCATIONS = ('s3://aws-src-files','s3://spotify-elt-pipelines/transformed-data/','s3://aws-src-files/stream-data/');

-- Creating external stage with the integration
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.AWS_S3_CUST_STREAM_STG
URL='s3://aws-src-files/stream-data/'
STORAGE_INTEGRATION = S3_INIT
FILE_FORMAT = FILE_FORMATS.CSV_FILE_FORMAT;

--list different files in the stage
LIST @MANAGE_DB.EXTERNAL_STAGES.AWS_S3_CUST_STREAM_STG;

--create customer pipe
CREATE OR REPLACE PIPE MANAGE_DB.SCD_DEMO.CUSTOMER_S3_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_RAW
(
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  EMAIL,
  STREET,
  CITY,
  STATE,
  COUNTRY,
  HASH_TXT_SCD2,
  HASH_TXT_SCD1
)
FROM (
  SELECT
    $1::NUMBER AS CUSTOMER_ID,
    $2        AS FIRST_NAME,
    $3        AS LAST_NAME,
    $4        AS EMAIL,
    $5        AS STREET,
    $6        AS CITY,
    $7        AS STATE,
    $8        AS COUNTRY,
    MD5($5 || '|' || $6 || '|' || $7 || '|' || $8) AS HASH_TXT_SCD2,
    MD5($2 || '|' || $3 || '|' || $4)               AS HASH_TXT_SCD1
  FROM @MANAGE_DB.EXTERNAL_STAGES.AWS_S3_CUST_STREAM_STG
)
FILE_FORMAT = (FORMAT_NAME = FILE_FORMATS.CSV_FILE_FORMAT 
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'CONTINUE';

--=============================================================================================================
--=============================================================================================================
--=============================================================================================================

TRUNCATE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_RAW;
TRUNCATE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_STG;
TRUNCATE TABLE MANAGE_DB.SCD_DEMO.CUSTOMER_DIM;

--view the pipes
SHOW PIPES;

--check data in raw table
SELECT * FROM MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_RAW;

--check data in stage table
SELECT * FROM MANAGE_DB.SCD_DEMO.CUSTOMER_DIM_STG;

--check data in Dim table
SELECT * FROM MANAGE_DB.SCD_DEMO.CUSTOMER_DIM;

--=============================================================================================================
--=============================================================================================================


























