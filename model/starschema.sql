/* =====================================================
   1. CREATE & USE DATABASE
===================================================== */

CREATE DATABASE IF NOT EXISTS ai_job_market_bi;
USE ai_job_market_bi;


/* =====================================================
   2. DROP TABLES 
===================================================== */

DROP TABLE IF EXISTS Fact_AI_Job_Market;
DROP TABLE IF EXISTS Dim_Time;
DROP TABLE IF EXISTS Dim_Qualification;
DROP TABLE IF EXISTS Dim_Location;
DROP TABLE IF EXISTS Dim_Industry;
DROP TABLE IF EXISTS Dim_Job;
DROP TABLE IF EXISTS Staging_AI_Job_Market;


/* =====================================================
   3. STAGING TABLE 
===================================================== */

CREATE TABLE Staging_AI_Job_Market (
    JobTitle VARCHAR(255),
    Industry VARCHAR(100),
    JobStatus VARCHAR(50),
    AIImpactLevel VARCHAR(50),
    MedianSalary DECIMAL(10,2),
    RequiredEducation VARCHAR(100),
    ExperienceRequired VARCHAR(100),
    JobOpenings_2024 INT,
    ProjectedOpenings_2030 INT,
    RemoteWorkRatio DECIMAL(5,2),
    AutomationRisk DECIMAL(5,2),
    Location VARCHAR(100),
    GenderDiversity DECIMAL(5,2),
    NetJobChange INT,
    PercentChange DECIMAL(9,2),
    AIImpactScore DECIMAL(5,2),
    CareerStabilityIndex DECIMAL(7,2),
    MedianSalaryNorm DECIMAL(5,2),
    HighOpportunityCareerScore DECIMAL(5,2)
);


/* =====================================================
   4. DIMENSION TABLES (SURROGATE KEYS)
===================================================== */

CREATE TABLE Dim_Job (
    JobID INT AUTO_INCREMENT PRIMARY KEY,
    JobTitle VARCHAR(255),
    JobStatus VARCHAR(50),
    AIImpactLevel VARCHAR(50)
);

CREATE TABLE Dim_Industry (
    IndustryID INT AUTO_INCREMENT PRIMARY KEY,
    IndustryName VARCHAR(100)
);

CREATE TABLE Dim_Location (
    LocationID INT AUTO_INCREMENT PRIMARY KEY,
    LocationName VARCHAR(100)
);

CREATE TABLE Dim_Qualification (
    QualificationID INT AUTO_INCREMENT PRIMARY KEY,
    RequiredEducation VARCHAR(100),
    ExperienceRequired VARCHAR(100)
);

CREATE TABLE Dim_Time (
    TimeID INT AUTO_INCREMENT PRIMARY KEY,
    ReferenceYear INT
);


/* =====================================================
   5. FACT TABLE 
===================================================== */

CREATE TABLE Fact_AI_Job_Market (
    FactID INT AUTO_INCREMENT PRIMARY KEY,

    JobID INT,
    IndustryID INT,
    LocationID INT,
    QualificationID INT,
    TimeID INT,

    JobOpenings_2024 INT,
    ProjectedOpenings_2030 INT,
    NetJobChange INT,
    PercentChange DECIMAL(9,2),
    MedianSalary DECIMAL(10,2),
    MedianSalaryNorm DECIMAL(5,2),
    AutomationRisk DECIMAL(5,2),
    RemoteWorkRatio DECIMAL(5,2),
    GenderDiversity DECIMAL(5,2),
    AIImpactScore DECIMAL(5,2),
    CareerStabilityIndex DECIMAL(9,2),
    HighOpportunityCareerScore DECIMAL(5,2),

    FOREIGN KEY (JobID) REFERENCES Dim_Job(JobID),
    FOREIGN KEY (IndustryID) REFERENCES Dim_Industry(IndustryID),
    FOREIGN KEY (LocationID) REFERENCES Dim_Location(LocationID),
    FOREIGN KEY (QualificationID) REFERENCES Dim_Qualification(QualificationID),
    FOREIGN KEY (TimeID) REFERENCES Dim_Time(TimeID)
);


/* =====================================================
   6. LOAD CLEANED CSV INTO STAGING
===================================================== */

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ai_job_market_cleaned.csv'
INTO TABLE Staging_AI_Job_Market
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


/* =====================================================
   7. POPULATE DIMENSION TABLES
===================================================== */

INSERT INTO Dim_Job (JobTitle, JobStatus, AIImpactLevel)
SELECT DISTINCT JobTitle, JobStatus, AIImpactLevel
FROM Staging_AI_Job_Market;

INSERT INTO Dim_Industry (IndustryName)
SELECT DISTINCT Industry
FROM Staging_AI_Job_Market;

INSERT INTO Dim_Location (LocationName)
SELECT DISTINCT Location
FROM Staging_AI_Job_Market;

INSERT INTO Dim_Qualification (RequiredEducation, ExperienceRequired)
SELECT DISTINCT RequiredEducation, ExperienceRequired
FROM Staging_AI_Job_Market;

INSERT INTO Dim_Time (ReferenceYear)
VALUES (2024), (2030);


/* =====================================================
   8. POPULATE FACT TABLE
===================================================== */

INSERT INTO Fact_AI_Job_Market (
    JobID,
    IndustryID,
    LocationID,
    QualificationID,
    TimeID,
    JobOpenings_2024,
    ProjectedOpenings_2030,
    NetJobChange,
    PercentChange,
    MedianSalary,
    MedianSalaryNorm,
    AutomationRisk,
    RemoteWorkRatio,
    GenderDiversity,
    AIImpactScore,
    CareerStabilityIndex,
    HighOpportunityCareerScore
)
SELECT
    j.JobID,
    i.IndustryID,
    l.LocationID,
    q.QualificationID,
    t.TimeID,

    s.JobOpenings_2024,
    s.ProjectedOpenings_2030,
    s.NetJobChange,
    s.PercentChange,
    s.MedianSalary,
    s.MedianSalaryNorm,
    s.AutomationRisk,
    s.RemoteWorkRatio,
    s.GenderDiversity,
    s.AIImpactScore,
    s.CareerStabilityIndex,
    s.HighOpportunityCareerScore

FROM Staging_AI_Job_Market s
JOIN Dim_Job j
    ON s.JobTitle = j.JobTitle
   AND s.JobStatus = j.JobStatus
   AND s.AIImpactLevel = j.AIImpactLevel
JOIN Dim_Industry i
    ON s.Industry = i.IndustryName
JOIN Dim_Location l
    ON s.Location = l.LocationName
JOIN Dim_Qualification q
    ON s.RequiredEducation = q.RequiredEducation
   AND s.ExperienceRequired = q.ExperienceRequired
JOIN Dim_Time t
    ON t.ReferenceYear = 2024;


/* =====================================================
   9. BASIC VALIDATION QUERIES
===================================================== */

SELECT COUNT(*) AS StagingRows FROM Staging_AI_Job_Market;
SELECT COUNT(*) AS FactRows FROM Fact_AI_Job_Market;
SELECT COUNT(*) AS Jobs FROM Dim_Job;
SELECT COUNT(*) AS Industries FROM Dim_Industry;
SELECT COUNT(*) AS Locations FROM Dim_Location;
SELECT COUNT(*) AS Qualifications FROM Dim_Qualification;
