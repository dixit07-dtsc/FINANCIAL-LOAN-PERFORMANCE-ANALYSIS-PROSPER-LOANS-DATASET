-- =============================================
-- PROSPER LOANS ETL SCRIPT
-- =============================================

-- STEP 1: CREATE DATABASE
CREATE DATABASE IF NOT EXISTS prosper_loans;
USE prosper_loans;

-- STEP 2: CREATE TABLE (DDL)
CREATE TABLE IF NOT EXISTS prosperLoandata_temp (
    ListingKey VARCHAR(50) PRIMARY KEY,
    LoanOriginationDate DATE,
    LoanOriginalAmount DECIMAL(10,2),
    Term INT,
    LoanStatus VARCHAR(50),
    BorrowerRate DECIMAL(5,4),
    BorrowerAPR DECIMAL(5,4),
    ProsperRating VARCHAR(5),
    ProsperScore INT,
    BorrowerState VARCHAR(2),
    Occupation VARCHAR(100),
    EmploymentStatus VARCHAR(50),
    IsBorrowerHomeowner VARCHAR(10),
    StatedMonthlyIncome DECIMAL(10,2),
    DebtToIncomeRatio DECIMAL(5,4),
    CreditScoreRangeLower INT,
    CreditScoreRangeUpper INT,
    CurrentDelinquencies INT,
    DelinquenciesLast7Years INT,
    MonthlyLoanPayment DECIMAL(10,2),
    LP_CustomerPayments DECIMAL(10,2),
    LP_ServiceFees DECIMAL(10,2),
    LP_CollectionFees DECIMAL(10,2),
    EstimatedReturn DECIMAL(5,4),
    ListingCategory INT
    -- ... (other 56 columns)
);

-- STEP 3: LOAD DATA (EXTRACT)
LOAD DATA LOCAL INFILE 'C:/Data/prosperLoanData.csv'
INTO TABLE prosperLoandata_temp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Verify load
SELECT COUNT(*) AS TotalRecords FROM prosperLoandata_temp;
-- Expected: 113,937

-- STEP 4: TRANSFORM - Data Type Conversions
UPDATE prosperLoandata_temp
SET LoanOriginationDate = STR_TO_DATE(LoanOriginationDate, '%Y-%m-%d')
WHERE LoanOriginationDate IS NOT NULL;

-- STEP 5: TRANSFORM - Handle NULLs
UPDATE prosperLoandata_temp
SET EmploymentStatus = 'Unknown'
WHERE EmploymentStatus IS NULL OR EmploymentStatus = '';

UPDATE prosperLoandata_temp
SET DebtToIncomeRatio = 0
WHERE DebtToIncomeRatio IS NULL;

UPDATE prosperLoandata_temp
SET CurrentDelinquencies = 0
WHERE CurrentDelinquencies IS NULL;

-- STEP 6: TRANSFORM - Data Validation
DELETE FROM prosperLoandata_temp
WHERE 
    LoanOriginalAmount <= 0
    OR BorrowerRate < 0
    OR BorrowerRate > 0.50
    OR Term NOT IN (12, 36, 60);

-- STEP 7: TRANSFORM - Clean Text Fields
UPDATE prosperLoandata_temp
SET BorrowerState = UPPER(TRIM(BorrowerState)),
    Occupation = TRIM(Occupation),
    EmploymentStatus = TRIM(EmploymentStatus);

-- STEP 8: TRANSFORM - Add Calculated Fields
ALTER TABLE prosperLoandata_temp
ADD COLUMN IsProblematic BOOLEAN,
ADD COLUMN RiskCategory VARCHAR(20),
ADD COLUMN LoanAgeDays INT;

UPDATE prosperLoandata_temp
SET IsProblematic = CASE 
    WHEN LoanStatus IN ('Chargedoff', 'Defaulted') THEN TRUE
    ELSE FALSE
END;

UPDATE prosperLoandata_temp
SET RiskCategory = CASE
    WHEN ProsperRating IN ('AA', 'A') THEN 'Low Risk'
    WHEN ProsperRating IN ('B', 'C') THEN 'Medium Risk'
    WHEN ProsperRating IN ('D', 'E', 'HR') THEN 'High Risk'
    ELSE 'Not Rated'
END;

UPDATE prosperLoandata_temp
SET LoanAgeDays = DATEDIFF(CURDATE(), LoanOriginationDate);

-- STEP 9: CREATE INDEXES (LOAD - Optimize)
CREATE INDEX idx_loan_status ON prosperLoandata_temp(LoanStatus);
CREATE INDEX idx_prosper_rating ON prosperLoandata_temp(ProsperRating);
CREATE INDEX idx_borrower_state ON prosperLoandata_temp(BorrowerState);
CREATE INDEX idx_origination_date ON prosperLoandata_temp(LoanOriginationDate);

OPTIMIZE TABLE prosperLoandata_temp;

-- STEP 10: FINAL VALIDATION
SELECT 
    COUNT(*) AS TotalRecords,
    COUNT(DISTINCT ListingKey) AS UniqueLoans,
    SUM(CASE WHEN LoanOriginalAmount IS NULL THEN 1 ELSE 0 END) AS NullAmounts,
    SUM(CASE WHEN BorrowerRate IS NULL THEN 1 ELSE 0 END) AS NullRates,
    MIN(LoanOriginationDate) AS EarliestLoan,
    MAX(LoanOriginationDate) AS LatestLoan
FROM prosperLoandata_temp;

-- ETL COMPLETE!
