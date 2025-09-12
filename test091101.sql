-- ==============================================
-- O1_FUTURE_ARCHIVE Duplicate Removal Workflow (Daily Basis)
-- ==============================================

-- Transaction Log Table
CREATE TABLE IF NOT EXISTS [dbo].[O1_FUTURE_ARCHIVE_DAILY_TRANSACTION_LOG] (
    [LogID] INT IDENTITY(1,1) PRIMARY KEY,
    [ReportDate] DATE NOT NULL,
    [ExecutionStart] DATETIME2 NOT NULL,
    [ExecutionEnd] DATETIME2,
    [Status] NVARCHAR(50) NOT NULL, 
    [DuplicatesFound] INT,
    [ErrorMessage] NVARCHAR(MAX)
);
GO

-- Deletion Log Table
CREATE TABLE IF NOT EXISTS [dbo].[O1_FUTURE_ARCHIVE_DELETION_LOG] (
    [ID] INT NOT NULL,
    [REPORTING_FIRM_ID] VARCHAR(50),
    [ACCOUNT_CODE] VARCHAR(50),
    [CONTRACT_MARKET_CODE] VARCHAR(50),
    [FUTURES_EXPIRATION_DATE] DATE,
    [REPORT_DATE] DATE,
    [LONG] FLOAT,
    [SHORT] FLOAT,
    [ACCOUNT_TYPE_CODE] VARCHAR(10),
    [SPECIAL_ACCOUNT_NAME] VARCHAR(255),
    [OWNER_ID] VARCHAR(50),
    [TRADER_SUFFIX] VARCHAR(10),
    [TRADER_NAME] VARCHAR(255),
    [CLASSIFICATION_CODE] VARCHAR(10),
    [COMMERCIAL_FLAG] CHAR(1),
    [COUNTRY_CODE] VARCHAR(10),
    [STATE_CODE] VARCHAR(10),
    [NOTICES_ISSUED] INT,
    [NOTICES_STOPPED] INT,
    [XFCS_BOUGHT] INT,
    [XFCS_SOLD] INT,
    [DELETED_AT] DATETIME DEFAULT GETDATE()
);
GO

-- Stored Procedure
CREATE OR ALTER PROCEDURE [dbo].[sp_Remove_Duplicates_From_O1_FUTURE_ARCHIVE_TRANSACTION_BASIS]
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @current_day DATE,
        @log_id INT,
        @duplicates_count INT;

    SET @current_day = @start_date;

    WHILE @current_day <= @end_date
    BEGIN
        BEGIN TRANSACTION;

        SET @duplicates_count = 0;

        -- Log start of transaction
        INSERT INTO [dbo].[O1_FUTURE_ARCHIVE_DAILY_TRANSACTION_LOG] (
            ReportDate, ExecutionStart, [Status]
        )
        VALUES (@current_day, SYSDATETIME(), 'In Progress');

        SET @log_id = SCOPE_IDENTITY();

        BEGIN TRY
            -- Identify duplicates
            IF OBJECT_ID('tempdb..#duplicates') IS NOT NULL DROP TABLE #duplicates;

            SELECT id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        [REPORTING_FIRM_ID], [ACCOUNT_CODE], [CONTRACT_MARKET_CODE], 
                        [FUTURES_EXPIRATION_DATE], [REPORT_DATE], [LONG], [SHORT], 
                        [ACCOUNT_TYPE_CODE], [SPECIAL_ACCOUNT_NAME], [OWNER_ID],
                        [TRADER_SUFFIX], [TRADER_NAME], [CLASSIFICATION_CODE], 
                        [COMMERCIAL_FLAG], [COUNTRY_CODE], [STATE_CODE], [NOTICES_ISSUED], 
                        [NOTICES_STOPPED], [XFCS_BOUGHT], [XFCS_SOLD]
                    ORDER BY id DESC
                ) AS rn
            INTO #duplicates
            FROM [ISS_DB].[dbo].[O1_FUTURE_ARCHIVE] WITH (NOLOCK)
            WHERE report_date = @current_day;

            -- Count duplicates
            SELECT @duplicates_count = COUNT(*) FROM #duplicates WHERE rn > 1;

            -- Log duplicates
            IF @duplicates_count > 0
            BEGIN
                INSERT INTO [dbo].[O1_FUTURE_ARCHIVE_DELETION_LOG] (
                    id, REPORTING_FIRM_ID, ACCOUNT_CODE, CONTRACT_MARKET_CODE, FUTURES_EXPIRATION_DATE,
                    REPORT_DATE, [LONG], [SHORT], ACCOUNT_TYPE_CODE, SPECIAL_ACCOUNT_NAME, OWNER_ID,
                    TRADER_SUFFIX, TRADER_NAME, CLASSIFICATION_CODE, COMMERCIAL_FLAG, COUNTRY_CODE,
                    STATE_CODE, NOTICES_ISSUED, NOTICES_STOPPED, XFCS_BOUGHT, XFCS_SOLD
                )
                SELECT 
                    id, REPORTING_FIRM_ID, ACCOUNT_CODE, CONTRACT_MARKET_CODE, FUTURES_EXPIRATION_DATE,
                    REPORT_DATE, [LONG], [SHORT], ACCOUNT_TYPE_CODE, SPECIAL_ACCOUNT_NAME, OWNER_ID,
                    TRADER_SUFFIX, TRADER_NAME, CLASSIFICATION_CODE, COMMERCIAL_FLAG, COUNTRY_CODE,
                    STATE_CODE, NOTICES_ISSUED, NOTICES_STOPPED, XFCS_BOUGHT, XFCS_SOLD
                FROM #duplicates WHERE rn > 1;
            END

            -- Delete duplicates
            DELETE FROM [ISS_DB].[dbo].[O1_FUTURE_ARCHIVE]
            WHERE id IN (SELECT id FROM #duplicates WHERE rn > 1);

            -- Commit
            COMMIT TRANSACTION;

            -- Log success
            UPDATE [dbo].[O1_FUTURE_ARCHIVE_DAILY_TRANSACTION_LOG]
            SET 
                ExecutionEnd = SYSDATETIME(),
                [Status] = 'Success',
                DuplicatesFound = @duplicates_count
            WHERE LogID = @log_id;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

            UPDATE [dbo].[O1_FUTURE_ARCHIVE_DAILY_TRANSACTION_LOG]
            SET 
                ExecutionEnd = SYSDATETIME(),
                [Status] = 'Failed',
                ErrorMessage = ERROR_MESSAGE()
            WHERE LogID = @log_id;

            THROW;
        END CATCH;

        SET @current_day = DATEADD(DAY, 1, @current_day);
    END
END;
GO

-- Example execution
EXEC [dbo].[sp_Remove_Duplicates_From_O1_FUTURE_ARCHIVE_TRANSACTION_BASIS]
    @start_date = '2024-01-01',
    @end_date = '2024-12-31';
