CREATE DATABASE Staging_win
GO


/* SELECCIONA LOS DATOS FILTRANDO SOLO PARA SUBNET WIN */
SELECT [Gateway SN], 
[Issue Type],
[Level of Service Bearing Capability Unattainable],
[Networking Type],
[Total APs],
[2.4G Usage Rate],
[5G Usage Rate],
[Assessed On]
INTO Transform1_win
FROM DataWin where [Subnet Name] = 'WIN'


CREATE TABLE [DATAWAREHOUSE_WIN].DBO.DimIssueType(
	IssueSR INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	IssueName NVARCHAR(100) NOT NULL,
)

CREATE TABLE [DATAWAREHOUSE_WIN].DBO.DimSignalType(
	SignalSR INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	SignalName NVARCHAR(100) NOT NULL
)

CREATE TABLE [DATAWAREHOUSE_WIN].DBO.DimCustomer (
	CustomerSR INT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	CustomerID nvarchar(100) NOT NULL,
	QuantityRepeaters INT NOT NULL
)



CREATE TABLE [DATAWAREHOUSE_WIN].DBO.DimCalendar(
	[DateKey] int NOT NULL PRIMARY KEY, /* Format: AAAAMMDD */
   [Date] date NOT NULL,          /* Actual date */
   [FullDate_Description] nvarchar(100) NULL,     /* Actual date description*/
   [DayNumberOfWeek] tinyint NULL,    /* 1 to 7 */
   [DayNumberOfMonth] tinyint NULL,   /* 1 to 31 */
   [DayNumberOfYear] smallint NULL,   /* 1 to 366 */
   [WeekNumberOfYear] tinyint NULL,   /* 1 to 53 */
   [MonthNumberOfYear] tinyint NULL,  /* 1 to 12 */
   [CalendarQuarterOfYear] tinyint NULL,    /* 1 to 4 */
   [CalendarSemesterOfYear] tinyint NULL,   /* 1 to 2 */
   [CalendarYear] char(4) NULL, /* Just the number */
   [CalendarYearWeek] nvarchar(25) NULL,/* Week Unique Identifier: Week + Year */
   [CalendarYearMonth] nvarchar(25) NULL,/* Month Unique Identifier: Month + Year */
   [CalendarYearQuarter] nvarchar(25) NULL,/* Quarter Unique Identifier: Quarter + Year */
   [CalendarYearSemester] nvarchar(25) NULL,/* Semester Unique Identifier: Semester + Year */
   [CalendarYearWeek_Description] nvarchar (25) NULL,
   [CalendarYearMonth_Description] nvarchar(25) NULL,/* Month Unique Descriptor: example - '2007-12'  */
   [CalendarYearQuarter_Description] nvarchar(25) NULL,/* Quarter Unique Descriptor: example - 'Q2/2007'  */
   [CalendarYearSemester_Description] nvarchar(25) NULL,/* Semester Unique Descriptor: example - 'H1.07'  */
   [CalendarYear_Description] nvarchar(25) NULL,/* Calendar Year Descriptor: example - 'CY 2007'  */
   [SpanishDayNameOfWeek] nvarchar(10) NULL,
   [SpanishMonthName] nvarchar(10) NULL, /* Enero a Diciembre */
)

SET LANGUAGE  'SPANISH' -- PARA INGRESAR LOS DIAS DE SEMANA EN ESPAÑOL
DECLARE @StartDate DATE;
DECLARE @EndDate DATE;

SELECT @StartDate = MIN([Assessed On]) FROM Transform1_win ;
SELECT @EndDate = CONVERT(DATE, GETDATE());

WITH DateSequence as (
	SELECT @StartDate AS [Date]
    UNION ALL
    SELECT DATEADD(DAY, 1, [Date])
    FROM DateSequence
    WHERE DATEADD(DAY, 1, [Date]) <= @EndDate
)

INSERT INTO DATAWAREHOUSE_WIN.DBO.DimCalendar(
   [DateKey],
   [Date],       
   [FullDate_Description] ,
   [DayNumberOfWeek],
   [DayNumberOfMonth] ,
   [DayNumberOfYear] ,
   [WeekNumberOfYear] ,
   [MonthNumberOfYear] ,
   [CalendarQuarterOfYear] ,
   [CalendarSemesterOfYear] ,
   [CalendarYear],
   [CalendarYearWeek] ,
   [CalendarYearMonth],
   [CalendarYearQuarter] ,
   [CalendarYearSemester],
   [CalendarYearWeek_Description] ,
   [CalendarYearMonth_Description] ,
   [CalendarYearQuarter_Description] ,
   [CalendarYearSemester_Description] ,
   [CalendarYear_Description] ,
   [SpanishDayNameOfWeek],
   [SpanishMonthName] 
)
SELECT 
	CONVERT(INT, FORMAT([Date], 'yyyyMMdd')) AS DateKey,
	[Date],
	FORMAT(DATE,'yyyy-MM-dd') AS [FullDate_Description],
	DATEPART(WEEKDAY, [Date]) AS  [DayNumberOfWeek],
	DAY([Date]) AS [DayNumberOfMonth],
	DATEPART(DAYOFYEAR, [Date]) AS [DayNumberOfYear],
	DATEPART(WEEK, [Date]) AS [WeekNumberOfYear],
	MONTH([DATE]) AS [MonthNumberOfYear],
	DATEPART(QUARTER,[Date] ) AS CalendarQuarterOfYear,
	CASE WHEN MONTH([Date]) <=6 THEN 1 ELSE 2 END AS CalendarSemesterOfYear,
	YEAR([Date]) as CalendarYear,
	FORMAT([Date],'yyyy') + '-w' + FORMAT(DATEPART(WEEK,[Date]),'00') AS CalendarYearWeek,
	FORMAT([Date],'yyyy') + '-' + FORMAT([DATE],'MM') AS CalendarYearMonth,
	FORMAT([Date],'yyyy') + '-Q' + FORMAT(DATEPART(QUARTER, [DATE]),'0' ) AS CalendarYearQuarter,
	FORMAT([Date],'yyyy') + '-S' + CASE WHEN MONTH([DATE])  <= 6 THEN '1' ELSE '2' END AS CalendarYearSemester,
	FORMAT([Date],'yyyy') +  '-' +  FORMAT(DATEPART(WEEK,[Date]),'00') as [CalendarYearWeek_Description],
	FORMAT([Date],'yyyy') +  '-' + FORMAT([DATE],'MM') AS CalendarYearMonth_Description ,
	FORMAT([Date],'yyyy') +   '-' + FORMAT(DATEPART(QUARTER, [DATE]),'0' ) as [CalendarYearQuarter_Description] ,
    FORMAT([Date],'yyyy') +  '-' +  CASE WHEN MONTH([DATE])  <= 6 THEN '1' ELSE '2' END AS [CalendarYearSemester_Description],
	'YC '+ FORMAT([Date],'yyyy') as [CalendarYear_Description],
	DATENAME(WEEKDAY, [Date]) as [SpanishDayNameOfWeek] ,
   DATENAME(MONTH, [Date]) as [SpanishMonthName]
FROM DateSequence
OPTION(MAXRECURSION 366);


-- proceso ETL



  INSERT INTO  DATAWAREHOUSE_WIN.DBO.DimIssueType (
	issueName
  )
  SELECT DISTINCT ([Issue Type]) FROM Staging_win.dbo.Transform_issuesWin


  CREATE TABLE DATAWAREHOUSE_WIN.DBO.FactIssue(
	[DateKey] int NOT NULL, 
	[IssueSR] int NOT NULL, 
	[IssueName] nvarchar(100) NOT NULL,
	[Quantity] int not null
	CONSTRAINT PK_FactIssueID PRIMARY KEY (DateKey, IssueSR)
	CONSTRAINT FK_DateID FOREIGN KEY (DateKey) REFERENCES DATAWAREHOUSE_WIN.DBO.DimCalendar(DateKey),
	CONSTRAINT FK_IssueSR FOREIGN KEY (IssueSR) REFERENCES DATAWAREHOUSE_WIN.DBO.DimIssueType(IssueSR)
  )

  
  INSERT INTO DATAWAREHOUSE_WIN.DBO.FactIssue
  (
	[DateKey], 
	[IssueSR], 
	[IssueName],
	[Quantity] 
  ) SELECT 
		DC.DateKey, DIT.issueSR, DIT.issueName, COUNT(DIT.issueName)
	FROM Staging_win.dbo.Transform_issuesWin as STI
	LEFT JOIN DimCalendar DC ON DC.[Date] = STI.[Assessed On]
	LEFT JOIN DimIssueType DIT ON DIT.issueName = STI.[Issue Type]
	GROUP BY  DC.DateKey, DIT.issueSR, DIT.issueName


	-- poblamos dimSignalType

	INSERT INTO DATAWAREHOUSE_WIN.dbo.DimSignalType 
	(SignalName)
	VALUES ('2.4 GHZ'), ('5 GHZ')

	CREATE TABLE DATAWAREHOUSE_WIN.DBO.FactSignalUse (
		[DateKey] INT NOT NULL ,
		[SignalSR] INT NOT NULL,
		[Percentage] DECIMAL (5,2)
		CONSTRAINT PK_FactSignalUse PRIMARY KEY (DateKey, SignalSR)
		CONSTRAINT FK_DateIDFactSignalUse FOREIGN KEY (DateKey) REFERENCES DATAWAREHOUSE_WIN.DBO.DimCalendar([DateKey]),
		CONSTRAINT FK_SignalSR FOREIGN KEY (SignalSR) REFERENCES DATAWAREHOUSE_WIN.DBO.DimSignalType(SignalSR)
	)

	INSERT INTO DATAWAREHOUSE_WIN.DBO.FactSignalUse 
	SELECT DC.[DateKey], DST.SignalSR, TUR.[Usage Rate] FROM Staging_win.dbo.TransformUsageRate TUR
	LEFT JOIN DATAWAREHOUSE_WIN.DBO.DimCalendar DC ON DC.[Date] =TUR.[Date]
	LEFT JOIN DATAWAREHOUSE_WIN.DBO.DimSignalType DST ON DST.SignalName = TUR.frecuency


CREATE TABLE TransformUsageRate 
(
 [Date] DATE NOT NULL, 
 Frecuency NVARCHAR(20) NOT NULL,
 [Usage Rate] DECIMAL(10,2) NOT NULL
)

CREATE PROCEDURE CalculateUsageRates
	@Date DATE
AS
BEGIN
	DECLARE @Sum24G DECIMAL(20,2);
	DECLARE @Sum5G DECIMAL(20,2);
	DECLARE @Count INT;
	SELECT @Sum24G = ROUND( sum(CONVERT(DECIMAL(20,2), REPLACE([2.4G Usage Rate], '%', ''))) ,2)
	FROM Staging_win.dbo.Transform1_win
	WHERE [Assessed On]= @Date

	SELECT @Sum5G = ROUND( sum(CONVERT(DECIMAL(20,2), REPLACE([5G Usage Rate], '%', ''))) ,2)
	FROM Staging_win.dbo.Transform1_win
	WHERE [Assessed On] = @Date

	SELECT @Count = COUNT(*) FROM Staging_win.dbo.Transform1_win
	WHERE [Assessed On] = @Date

	INSERT INTO TransformUsageRate
	SELECT @Date AS [Date], '2.4 GHZ' AS Frecuency, ROUND(@Sum24G/@Count,2) AS [Usage Rate] 
	UNION ALL
	SELECT @Date AS [Date], '5 GHZ' AS Frecuency, ROUND( @Sum5G/@Count,2) AS [Usage Rate]
END;
	

CREATE PROCEDURE InsertDateUsageRate
AS
BEGIN
DECLARE date_cursor CURSOR LOCAL FOR
SELECT DISTINCT [Assessed On] FROM Transform1_win;
DECLARE @CurrentDate DATE;
OPEN date_cursor;
FETCH NEXT FROM date_cursor INTO @CurrentDate;
WHILE @@FETCH_STATUS = 0 
BEGIN
 EXEC CalculateUsageRates @Date = @CurrentDate;
 FETCH NEXT FROM Date_cursor
END		
CLOSE date_cursor
DEALLOCATE  date_cursor
END;

SELECT * FROM Staging_win.dbo.TransformUsageRate

--- VERIFICAR LAS FECHAS...

CREATE TABLE Staging_win.dbo.TransformUserRepeaterCount(
	[Date] DATE NOT NULL,
	[UserRepeaterCount] INT NOT NULL 
)

INSERT INTO Staging_win.DBO.TransformUserRepeaterCount
SELECT [assessed on], COUNT(*) AS [UserRepeaterCount] FROM Staging_win.dbo.Transform1_win
WHERE [Total APs] != 0 
GROUP BY [Assessed On]


CREATE TABLE DATAWAREHOUSE_WIN.DBO.FactUserRepeaterCount(
	[DateKey] int NOT NULL, 
	[Quantity] int NOT NULL
	CONSTRAINT Pk_factUserRepeater PRIMARY KEY(DateKey)
	CONSTRAINT FK_factUserRepeater FOREIGN KEY (DateKey) REFERENCES DATAWAREHOUSE_WIN.DBO.DimCalendar(DateKey)
)


INSERT INTO DATAWAREHOUSE_WIN.DBO.FactUserRepeaterCount
SELECT DC.[DateKey], TURC.UserRepeaterCount FROM Staging_win.dbo.TransformUserRepeaterCount TURC
LEFT JOIN DimCalendar DC ON DC.[Date] = TURC.[Date]

-- REPEATERS TOTAL APS
CREATE TABLE Staging_win.dbo.TransformTotalAps (
	[Date] DATE NOT NULL,
	[Total APs] INT NOT NULL
)

INSERT INTO Staging_win.dbo.TransformTotalAps
SELECT [Assessed On], SUM([Total APs]) AS [Total Aps] from Staging_win.dbo.Transform1_win
GROUP BY [Assessed On]



CREATE TABLE DATAWAREHOUSE_WIN.DBO.FactRepeaterUsage(
	[DateKey] INT NOT NULL,
	[Quantity] INT NOT NULL
	CONSTRAINT PK_FactRepeaterUsage PRIMARY KEY (DateKey)
	CONSTRAINT FK_DateFactRepeater FOREIGN KEY (DateKey) REFERENCES  DATAWAREHOUSE_WIN.DBO.DimCalendar(DateKey)
)

INSERT INTO DATAWAREHOUSE_WIN.DBO.FactRepeaterUsage
SELECT DC.DateKey, TTA.[Total APs] FROM Staging_win.DBO.TransformTotalAps TTA
LEFT JOIN DATAWAREHOUSE_WIN.DBO.DimCalendar DC ON DC.[Date] = TTA.[Date]


