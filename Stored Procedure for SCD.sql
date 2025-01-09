--Stored Procedure for processing data from RAW -> STG -> DIM
CREATE OR REPLACE PROCEDURE PRCDR_LOAD_CUSTOMER_DIM
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
	var cmd1_truncate_stg = "TRUNCATE TABLE SCD_DEMO.CUSTOMER_DIM_STG;"

	-- Insert raw data into stage_table
	var cmd2_insert_stg = `
		INSERT INTO CUSTOMER_DIM_STG  
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
			HASH_TXT_SCD1, 
			INSERT_TS
		) 
		WITH CTE_PARAM_CTRL AS
		(
			SELECT TO_TIMESTAMP(PARAMETER_VALUE, 'YYYY-MM-DD HH24:MI:SS.FF3') AS LETS_VALUE 
			FROM SNWFLK_PARAMETER_CTRL 
			WHERE JOB_NAME = 'CUSTOMER RAW -> STG' AND PARAMETER_NAME = 'LETS'
		)
		SELECT 
		RAWTAB.CUSTOMER_ID, 
		RAWTAB.FIRST_NAME, 
		RAWTAB.LAST_NAME, 
		RAWTAB.EMAIL, 
		RAWTAB.STREET, 
		RAWTAB.CITY, 
		RAWTAB.STATE, 
		RAWTAB.COUNTRY, 
		RAWTAB.HASH_TXT_SCD2, 
		RAWTAB.HASH_TXT_SCD1, 
		MAX(RAWTAB.INSERT_TS)
		FROM CUSTOMER_DIM_RAW RAWTAB
		JOIN CTE_PARAM_CTRL CTE_PARAM ON (1=1)
		WHERE RAWTAB.INSERT_TS > CTE_PARAM.LETS_VALUE
		GROUP BY RAWTAB.CUSTOMER_ID, RAWTAB.FIRST_NAME, RAWTAB.LAST_NAME, RAWTAB.EMAIL, RAWTAB.STREET, 
		RAWTAB.CITY, RAWTAB.STATE, RAWTAB.COUNTRY, RAWTAB.HASH_TXT_SCD2, RAWTAB.HASH_TXT_SCD1;
		`

	--DML statement for implementing SCD-2
	-- Part 1: Inactivate the existing record		
	var cmd3_scd2_inactivate = `
		UPDATE CUSTOMER_DIM FNL
		SET
			FNL.UPDATE_TS = STG.INSERT_TS,
			FNL.ACTIVE_IND = 'N', 
			FNL.RCRD_END_DATE = STG.INSERT_TS
		FROM CUSTOMER_DIM_STG STG 
		WHERE FNL.CUSTOMER_ID=STG.CUSTOMER_ID 
		AND FNL.HASH_TXT_SCD2<>STG.HASH_TXT_SCD2
		AND FNL.ACTIVE_IND = 'Y';
		`
	
	var cmd4_scd2_insert = `
		INSERT INTO CUSTOMER_DIM  
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
			HASH_TXT_SCD1, 
			INSERT_TS, 
			UPDATE_TS, 
			ACTIVE_IND, 
			RCRD_STRT_DATE, 
			RCRD_END_DATE
		) 
		SELECT 
		STG.CUSTOMER_ID, 
		STG.FIRST_NAME, 
		STG.LAST_NAME, 
		STG.EMAIL, 
		STG.STREET, 
		STG.CITY, 
		STG.STATE, 
		STG.COUNTRY, 
		STG.HASH_TXT_SCD2, 
		STG.HASH_TXT_SCD1, 
		FNL.INSERT_TS, 
		STG.INSERT_TS, 
		'Y', 
		STG.INSERT_TS, 
		TO_TIMESTAMP('31-DEC-2999','DD-MON-YYYY')
		FROM CUSTOMER_DIM_STG STG 
		LEFT JOIN CUSTOMER_DIM FNL ON (FNL.CUSTOMER_ID = STG.CUSTOMER_ID)
		WHERE FNL.CUSTOMER_ID=STG.CUSTOMER_ID 
		AND FNL.HASH_TXT_SCD2<>STG.HASH_TXT_SCD2
		AND NOT EXISTS 
		(
			SELECT 1 FROM CUSTOMER_DIM DIM
			WHERE DIM.CUSTOMER_ID=STG.CUSTOMER_ID 
			AND DIM.HASH_TXT_SCD2<>STG.HASH_TXT_SCD2
			AND DIM.ACTIVE_IND = 'Y'
		);
		`
	
	--Merge statement for implementing SCD-1
	var cmd5_scd1_update = `
		MERGE INTO CUSTOMER_DIM FNL
		USING CUSTOMER_DIM_STG STG 
		ON (FNL.CUSTOMER_ID=STG.CUSTOMER_ID)
		WHEN MATCHED AND (FNL.HASH_TXT_SCD1<>STG.HASH_TXT_SCD1 AND FNL.ACTIVE_IND = 'Y') THEN 
		UPDATE SET 
			FNL.FIRST_NAME = STG.FIRST_NAME,
			FNL.LAST_NAME = STG.LAST_NAME,
			FNL.EMAIL = STG.EMAIL,
			FNL.HASH_TXT_SCD1 = STG.HASH_TXT_SCD1,
			FNL.UPDATE_TS = STG.INSERT_TS
		WHEN NOT MATCHED THEN 
		INSERT  
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
			HASH_TXT_SCD1, 
			INSERT_TS, 
			UPDATE_TS, 
			ACTIVE_IND, 
			RCRD_STRT_DATE, 
			RCRD_END_DATE
		) 
		VALUES 
		(
			STG.CUSTOMER_ID, 
			STG.FIRST_NAME, 
			STG.LAST_NAME, 
			STG.EMAIL, 
			STG.STREET, 
			STG.CITY, 
			STG.STATE, 
			STG.COUNTRY, 
			STG.HASH_TXT_SCD2, 
			STG.HASH_TXT_SCD1, 
			STG.INSERT_TS, 
			STG.INSERT_TS, 
			'Y', 
			STG.INSERT_TS, 
			TO_TIMESTAMP('31-DEC-2999','DD-MON-YYYY')
		);
		`
	
	-- Update the LETS value in the parameter control table
	var cmd6_lets_update = `
		UPDATE SNWFLK_PARAMETER_CTRL 
		SET PARAMETER_VALUE = 
		(SELECT MAX(LETS) AS LETS FROM
			(
				SELECT MAX(INSERT_TS) LETS FROM SCD_DEMO.CUSTOMER_DIM
				UNION ALL
				SELECT MAX(UPDATE_TS) LETS FROM SCD_DEMO.CUSTOMER_DIM
			)
		)
		WHERE JOB_NAME = 'CUSTOMER RAW -> STG' AND PARAMETER_NAME = 'LETS';
		`
	
	--Delete from Raw based on insert into Stage
	var cmd7_delete_raw = `
		DELETE FROM CUSTOMER_DIM_RAW
		WHERE INSERT_TS <= 
		(
			SELECT TO_TIMESTAMP(PARAMETER_VALUE, 'YYYY-MM-DD HH24:MI:SS.FF3') AS LETS_VALUE 
			FROM SNWFLK_PARAMETER_CTRL 
			WHERE JOB_NAME = 'CUSTOMER RAW -> STG' AND PARAMETER_NAME = 'LETS'
		);
		`
	
	var sql1 = snowflake.createStatement({sqlText: cmd1_truncate_stg});
	var sql2 = snowflake.createStatement({sqlText: cmd2_insert_stg});
	var sql3 = snowflake.createStatement({sqlText: cmd3_scd2_inactivate});
	var sql4 = snowflake.createStatement({sqlText: cmd4_scd2_insert});
	var sql5 = snowflake.createStatement({sqlText: cmd5_scd1_update});
	var sql6 = snowflake.createStatement({sqlText: cmd6_lets_update});
	var sql7 = snowflake.createStatement({sqlText: cmd7_delete_raw});

	var result1 = sql1.execute();
	var result2 = sql2.execute();
	var result3 = sql3.execute();
	var result4 = sql4.execute();
	var result5 = sql5.execute();
	var result6 = sql6.execute();
	var result7 = sql7.execute();
	
return cmd1+'\n'+cmd2+'\n'+cmd3+'\n'+cmd4+'\n'+cmd5+'\n'+cmd6+'\n'+cmd7;
$$;

--Calling Stored Procedure that implements SCD-1
CALL PRCDR_LOAD_CUSTOMER_DIM();