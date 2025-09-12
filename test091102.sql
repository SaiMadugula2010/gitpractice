-- ==============================================
-- O1_OPTION_ARCHIVE Duplicate Removal Workflow (Daily Basis)
-- ==============================================

-- Transaction Log Table
CREATE TABLE IF NOT EXISTS [dbo].[O1_OPTION_ARCHIVE_MONTHLY_TRANSACTION_LOG] (
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
CREATE TABLE IF NOT EXISTS [dbo].[O1_OPTION_ARCHIVE_DELETION_LOG] (
    [id] INT NOT NULL,
    [REPORTING_FIRM_ID] INT,
    [ACCOUNT_CODE] VARCHAR(50),
    [CONTRACT_MARKET_CODE] VARCHAR(50),
    [FUTURES_EXPIRATION_DATE] DATE,
    [OPTION_CLASS_CODE] VARCHAR(50),
    [EXPIRATION_ID_DATE] DATE,
    [PUT_CALL_INDICATOR] VARCHAR(10),
    [STRIKE_PRICE] DECIMAL(18,2),
    [DELTA_FACTOR] DECIMAL(18,2),
    [REPORT_DATE] DATE,
    [LONG] INT,
    [SHORT] INT,
    [ACCOUNT_TYPE_CODE] VARCHAR(50),
    [SPECIAL_ACCOUNT_NAME] VARCHAR(100),
    [OWNER_ID] VARCHAR(50),
    [TRADER_SUFFIX] VARCHAR(50),
    [TRADER_NAME] VARCHAR(100),
    [CLASSIFICATION_CODE] VARCHAR(50),
    [COMMERCIAL_FLAG] BIT,
    [COUNTRY_CODE] VARCHAR(50),
    [STATE_CODE] VARCHAR(50),
    [DELETED_AT] DATETIME DEFAULT GETDATE()
);
GO

-- Stored Procedure
CREATE OR ALTER PROCEDURE [dbo].[sp_Remove_Duplicates_From_O1_OPTION_ARCHIVE_DAILY_BASIS]
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
        INSERT INTO [dbo].[O1_OPTION_ARCHIVE_MONTHLY_TRANSACTION_LOG] (
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
                          [FUTURES_EXPIRATION_DATE], [OPTION_CLASS_CODE], [EXPIRATION_ID_DATE],
                          [PUT_CALL_INDICATOR], [STRIKE_PRICE], [DELTA_FACTOR], [REPORT_DATE],
                          [LONG], [SHORT], [ACCOUNT_TYPE_CODE], [SPECIAL_ACCOUNT_NAME], [OWNER_ID],
                          [TRADER_SUFFIX], [TRADER_NAME], [CLASSIFICATION_CODE], [COMMERCIAL_FLAG],
                          [COUNTRY_CODE], [STATE_CODE]
                    ORDER BY id DESC
                ) AS rn
            INTO #duplicates
            FROM [ISS_DB].[dbo].[O1_OPTION_ARCHIVE] WITH (NOLOCK)
            WHERE report_date = @current_day;

            -- Count duplicates
            SELECT @duplicates_count = COUNT(*) FROM #duplicates WHERE rn > 1;

            -- Log duplicates
            IF @duplicates_count > 0
            BEGIN
                INSERT INTO [dbo].[O1_OPTION_ARCHIVE_DELETION_LOG] (
                    id, REPORTING_FIRM_ID, ACCOUNT_CODE, CONTRACT_MARKET_CODE, FUTURES_EXPIRATION_DATE,
                    OPTION_CLASS_CODE, EXPIRATION_ID_DATE, PUT_CALL_INDICATOR, STRIKE_PRICE, DELTA_FACTOR,
                    REPORT_DATE, [LONG], [SHORT], ACCOUNT_TYPE_CODE, SPECIAL_ACCOUNT_NAME, OWNER_ID,
                    TRADER_SUFFIX, TRADER_NAME, CLASSIFICATION_CODE, COMMERCIAL_FLAG, COUNTRY_CODE, STATE_CODE
                )
                SELECT 
                    id, REPORTING_FIRM_ID, ACCOUNT_CODE, CONTRACT_MARKET_CODE, FUTURES_EXPIRATION_DATE,
                    OPTION_CLASS_CODE, EXPIRATION_ID_DATE, PUT_CALL_INDICATOR, STRIKE_PRICE, DELTA_FACTOR,
                    REPORT_DATE, [LONG], [SHORT], ACCOUNT_TYPE_CODE, SPECIAL_ACCOUNT_NAME, OWNER_ID,
                    TRADER_SUFFIX, TRADER_NAME, CLASSIFICATION_CODE, COMMERCIAL_FLAG, COUNTRY_CODE, STATE_CODE
                FROM #duplicates WHERE rn > 1;
            END

            -- Delete duplicates
            DELETE FROM [ISS_DB].[dbo].[O1_OPTION_ARCHIVE]
            WHERE id IN (SELECT id FROM #duplicates WHERE rn > 1);

            -- Commit
            COMMIT TRANSACTION;

            -- Log success
            UPDATE [dbo].[O1_OPTION_ARCHIVE_MONTHLY_TRANSACTION_LOG]
            SET 
                ExecutionEnd = SYSDATETIME(),
                [Status] = 'Success',
                DuplicatesFound = @duplicates_count
            WHERE LogID = @log_id;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

            UPDATE [dbo].[O1_OPTION_ARCHIVE_MONTHLY_TRANSACTION_LOG]
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
EXEC [dbo].[sp_Remove_Duplicates_From_O1_OPTION_ARCHIVE_DAILY_BASIS]
    @start_date = '2024-01-01',
    @end_date = '2024-12-31';
