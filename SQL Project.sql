--Part 1: Creating a Data Model.--
use InsigniaDW;
--creation of Lineage table--
CREATE TABLE Lineage (
    Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_System VARCHAR(100),
    Load_Stat_Datetime DATETIME,
    Load_EndDatetime DATETIME,
    Rows_at_Source INT,
    Rows_at_destination_Fact INT,
    Load_Status BIT
);

--creation of customer dimension table--
CREATE TABLE CustomerDimension (
    CustomerName VARCHAR(255),
    CustomerCategory VARCHAR(50),
    CustomerContactName VARCHAR(255),
    CustomerPostalCode VARCHAR(20),
    CustomerContactNumber VARCHAR(20),
    City_Id INT,
    City VARCHAR(100),
    State_Province VARCHAR(100),
    Country VARCHAR(100),
    Continent VARCHAR(50),
    Sales_Territory VARCHAR(50),
    Region VARCHAR(50),
    Subregion VARCHAR(50),
    Lineage_Id BIGINT
);
--creation of DateDimension table--
CREATE TABLE DateDimension (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day_Number INT,
    Month_Name VARCHAR(20),
    Short_Month VARCHAR(3),
    Calendar_Month_Number INT,
    Calendar_Year INT,
    Fiscal_Month_Number INT,
    Fiscal_Year INT,
    Week_Number INT
);

-- Load data from 2000 to 2023
DECLARE @StartDate DATE = '2000-01-01';
DECLARE @EndDate DATE = '2023-12-31';
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    DECLARE @DateKey INT = CONVERT(INT, CONVERT(CHAR(8), @CurrentDate, 112));
    DECLARE @Day_Number INT = DATEPART(DAY, @CurrentDate);
    DECLARE @Month_Name VARCHAR(20) = DATENAME(MONTH, @CurrentDate);
    DECLARE @Short_Month VARCHAR(3) = LEFT(@Month_Name, 3);
    DECLARE @Calendar_Month_Number INT = DATEPART(MONTH, @CurrentDate);
    DECLARE @Calendar_Year INT = DATEPART(YEAR, @CurrentDate);
    DECLARE @Fiscal_Month_Number INT = DATEPART(MONTH, DATEADD(MONTH, -6, @CurrentDate));
    DECLARE @Fiscal_Year INT = DATEPART(YEAR, DATEADD(MONTH, -6, @CurrentDate));
    DECLARE @Week_Number INT = DATEPART(WEEK, @CurrentDate);

    INSERT INTO DateDimension
    VALUES (@DateKey, @CurrentDate, @Day_Number, @Month_Name, @Short_Month, @Calendar_Month_Number, 
            @Calendar_Year, @Fiscal_Month_Number, @Fiscal_Year, @Week_Number);

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;

select * from DateDimension;
--creation of customer dimension table--
CREATE TABLE CustomerDimension (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName VARCHAR(255),
    CustomerAge INT,
    CustomerGender VARCHAR(10),
    CustomerEmail VARCHAR(255),
    CustomerStreet VARCHAR(255),
    CustomerCity VARCHAR(100),
    CustomerState VARCHAR(100),
    CustomerZipcode VARCHAR(20),
    CustomerCountry VARCHAR(100),
    CustomerContactNumber VARCHAR(20),
    Lineage_Id BIGINT
);
--creation of employee dimension table--
CREATE TABLE EmployeeDimension (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeId INT,
    Lineage_Id BIGINT
);
--creation of product dimension table--
CREATE TABLE ProductDimension (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    Description VARCHAR(255),
    Lineage_Id BIGINT
);
--creation of geography dimension table--
CREATE TABLE GeographyDimension (
    GeographyKey INT IDENTITY(1,1) PRIMARY KEY,
    City_ID INT,
    City VARCHAR(100),
    State_Province VARCHAR(100),
    Country VARCHAR(100),
    Continent VARCHAR(100),
    Sales_Territory VARCHAR(100),
    Region VARCHAR(100),
    Subregion VARCHAR(100),
    Latest_Recorded_Population INT,
    Previous_Population INT,
    Lineage_Id BIGINT
);
--creation of sales fact table--
CREATE TABLE SalesFact (
    SalesFactId BIGINT IDENTITY(1,1) PRIMARY KEY,
    InvoiceId INT,
    Quantity INT,
    Unit_Price DECIMAL(18, 2),
    Total_Excluding_Tax DECIMAL(18, 2),
    Tax_Amount DECIMAL(18, 2),
    Profit DECIMAL(18, 2),
    Total_Including_Tax DECIMAL(18, 2),
    EmployeeId INT,
    StockItemId INT,
    CustomerId INT,
    CityId INT,
    Lineage_Id BIGINT
);




--Part 2: ETL and SCD Implementation--
--Creating a Copy of the Insignia Staging Table:--
use InsigniaDW;
IF OBJECT_ID('dbo.Insignia_staging_copy', 'U') IS NOT NULL
    DROP TABLE dbo.Insignia_staging_copy;

SELECT * INTO Insignia_staging_copy
FROM ['Insignia_staging 2$'];


--Loading Data into Dimensions:--
DECLARE @Load_Id BIGINT = 1; -- Example Load Id
DECLARE @CurrentDatetime DATETIME = GETDATE();

INSERT INTO CustomerDimension (
    CustomerName, CustomerCategory, CustomerContactName, CustomerPostalCode, 
    CustomerContactNumber, City_Id, City, State_Province, Country, Continent, 
    Sales_Territory, Region, Subregion, Lineage_Id)
SELECT DISTINCT 
    src.CustomerName, src.CustomerCategory, src.CustomerContactName, src.CustomerPostalCode, 
    src.CustomerContactNumber, src.City_Id, src.City, src.State_Province, src.Country, src.Continent, 
    src.Sales_Territory, src.Region, src.Subregion, @Load_Id
FROM Insignia_staging_copy src
LEFT JOIN CustomerDimension tgt ON src.CustomerContactNumber = tgt.CustomerContactNumber
WHERE tgt.CustomerContactNumber IS NULL OR (
    tgt.CustomerName != src.CustomerName OR
    tgt.CustomerCategory != src.CustomerCategory OR
    tgt.CustomerContactName != src.CustomerContactName OR
    tgt.CustomerPostalCode != src.CustomerPostalCode OR
    tgt.CustomerContactNumber != src.CustomerContactNumber OR
    tgt.City_Id != src.City_Id OR
    tgt.City != src.City OR
    tgt.State_Province != src.State_Province OR
    tgt.Country != src.Country OR
    tgt.Continent != src.Continent OR
    tgt.Sales_Territory != src.Sales_Territory OR
    tgt.Region != src.Region OR
    tgt.Subregion != src.Subregion
);
GO

--Loading Data into Fact Table:--
-- Declaring necessary variables
DECLARE @Load_Id BIGINT = 1; -- Example Load Id

-- Inserting data into the SalesFact table--
INSERT INTO SalesFact (
    InvoiceId, Quantity, Unit_Price, Total_Excluding_Tax, Tax_Amount, Profit, Total_Including_Tax, 
    EmployeeId, StockItemId, CustomerId, CityId, Lineage_Id)
SELECT 
    InvoiceId, Quantity, Unit_Price, Total_Excluding_Tax, Tax_Amount, Profit, Total_Including_Tax,
    Employee_Id, Stock_Item_Id, Customer_Id, City_Id, @Load_Id
FROM Insignia_staging_copy;
GO

--ETL Process for Incremental Load:--
-- Truncate Insignia_staging_copy
TRUNCATE TABLE Insignia_staging_copy;

-- Load Incremental Data
INSERT INTO Insignia_staging_copy
SELECT * FROM ['Insignia_incremental 2$'];

--Reconciliation Module--
--To check number of rows processed
use InsigniaDW;
SELECT COUNT(*) AS SourceRows FROM Insignia_staging_copy;
SELECT COUNT(*) AS DestinationRows FROM SalesFact;

















