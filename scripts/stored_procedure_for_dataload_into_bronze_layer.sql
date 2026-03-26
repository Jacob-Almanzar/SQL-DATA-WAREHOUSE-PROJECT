-- CREATION OF LOAD PROCEDURE TO LOAD DATA INTO BRONZE LAYER
CREATE OR ALTER PROCEDURE BRONZE.LOAD_BRONZE AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME --> FOR CALCULATION OF LOAD DURATION
	BEGIN TRY
		SET @BATCH_START_TIME = GETDATE();
		PRINT'============================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT'============================';
			--LOAD SCRIPT
			-- BULK INSERT
			--LOADING LARGE AMMOUNT OF DATA QUICKLY

				-- CLEARS THE TABLE PRIOR TO LOAD, PREVENTS DOUBLE-LOADING AND ALLOWS FOR EASIER DATA REFRESHES
				--BELOW WILL BE REPEATED ACROSS ALL CSV FILES TO THEIR RESPECTIVE TABLES

		SET @START_TIME = GETDATE();
		PRINT 'crm FILE LOAD' ;
			TRUNCATE TABLE BRONZE.CRM_CUST_INFO
			BULK INSERT BRONZE.CRM_CUST_INFO
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2, -- FIRST ROW IS HEADERS, DATA STARTS ON ROW 2
				FIELDTERMINATOR = ',', -- HOW THE DATA IS DELIMINATED IN THE CSV
				TABLOCK -- LOCKS THE TABLE DURING DATALOAD
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.CRM_CUST_INFO LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;

			SET @START_TIME = GETDATE();
			TRUNCATE TABLE BRONZE.CRM_PRD_INFO
			BULK INSERT BRONZE.CRM_PRD_INFO
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK 
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.CRM_PRD_INFO LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;

			SET @START_TIME = GETDATE();
			TRUNCATE TABLE BRONZE.CRM_SALES_DETAILS
			BULK INSERT BRONZE.CRM_SALES_DETAILS
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK 
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.CRM_SALES_DETAILS LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================
			
			
			' ;

		PRINT 'erp FILE LOAD';

			SET @START_TIME = GETDATE();
			TRUNCATE TABLE BRONZE.ERP_CUST_AZ12
			BULK INSERT BRONZE.ERP_CUST_AZ12
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK 
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.ERP_CUST_AZ12 LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;

			SET @START_TIME = GETDATE();
			TRUNCATE TABLE BRONZE.ERP_LOC_A101
			BULK INSERT BRONZE.ERP_LOC_A101
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK 
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.ERP_LOC_A101 LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;

			SET @START_TIME = GETDATE();
			TRUNCATE TABLE BRONZE.ERP_PX_CAT_G1V2
			BULK INSERT BRONZE.ERP_PX_CAT_G1V2
			FROM 'C:\Users\12398\OneDrive\Desktop\sql\PROJECTS\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2, 
				FIELDTERMINATOR = ',',
				TABLOCK 
				);
			SET @END_TIME = GETDATE();
			PRINT '>>BRONZE.ERP_PX_CAT_G1V2 LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;
	
		SET @BATCH_END_TIME = GETDATE()
		PRINT '>>TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @BATCH_START_TIME, @BATCH_END_TIME) AS VARCHAR) + 'SECONDS';
			PRINT '==============================================================' ;
	END TRY
		BEGIN CATCH
			PRINT '===========================================';
			PRINT ' ERROR LOADING DURING BRONZE LAYER';
			PRINT ' ERROR_MESSAGE()'; 
			PRINT '===========================================';
		END CATCH
END
