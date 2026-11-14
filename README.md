# hiring-analytics-databricks-pipeline

HR Workforce & Attrition Analytics: Databricks + Power BI
An end-to-end data engineering project built on Databricks (Free Edition). Raw HR data goes through a Bronze, Silver, Gold pipeline (Medallion Architecture), runs as an orchestrated Lakeflow Job with quality checks, and feeds a 3-page Power BI dashboard.

Tech stack
Databricks | PySpark | SQL | Delta Lake | Unity Catalog | Lakeflow Jobs | Power BI (DAX)

Architecture


employee_data.csv (Unity Catalog Volume)
        |
   [ Bronze ]  raw copy + source file + load time
        |
   [ Silver ]  cleaned, typed, enriched
        |
   [ Quality checks ]  6 rules, job fails if any rule fails
        |
   [ Gold ]  hiring_trend, attrition, performance
        |
   [ Power BI ]  3-page dashboard
Dataset
Public Kaggle HR dataset (employee_data.csv): 3,000 employees, 26 columns, covering Aug 2018 to Aug 2023. 
Source: "Employee/HR Dataset (All In One)" by ravindrasinghrana on Kaggle: https://www.kaggle.com/datasets/ravindrasinghrana/employeedataset. The CSV is not included in this repo.

Pipeline
Notebook	What it does
00_setup	Creates the hiring_bronze, hiring_silver, hiring_gold schemas and the raw_files volume
01_bronze_ingest	Reads the CSV, makes column names Delta-safe, adds _source_file and _load_time, saves a Delta table
02_silver_clean	Trims text, converts two text date formats to real dates, replaces Unk with Not Terminated, removes duplicates, adds StartYear, IsTerminated, TenureYears, Age, and drops ADEmail and DOB for privacy
04_quality_checks	6 checks on Silver (no null or duplicate EmpID, no null StartDate, exit after start, rating between 1 and 5, table not empty). The notebook raises an error if any check fails
03_gold_analytics	Builds 3 Gold tables: hiring_trend, attrition, performance

Lakeflow Job
Tasks run in this order: bronze_ingest -> silver_clean -> quality_checks -> gold_analytics. Quality checks run before Gold, so bad data stops the job instead of reaching the dashboard.

Job graph Successful run

Gold tables
Table	Answers
hiring_trend	How many people joined each year, by department?
attrition	Which departments and business units lose the most people?
performance	How are performance levels spread across departments?
Power BI dashboard
Connected to the Gold tables through the Databricks connector. The file is in dashboard/hiring_analytics_dashboard.pbix.

DAX measures

Total Hires = SUM(hiring_trend[hires])
Total Exits = SUM(attrition[exits])
Attrition Rate % = DIVIDE(SUM(attrition[exits]), SUM(attrition[total_employees])) * 100
Avg Rating = DIVIDE(SUMX(performance, performance[avg_rating] * performance[employees]), SUM(performance[employees]))

Hiring Trend
Hiring trend

Attrition
Attrition

Performance
Performance

Key findings
>> 3,000 employees in total, 1,533 of them have left: an overall attrition rate of 51.1%.
>> Executive Office has the highest attrition rate (about 79%), but it is a small department, so a few exits move the rate a lot.
>> Hiring was steady at roughly 45 per month in 2023 (January to July). 2018 and 2023 are partial years because the data covers Aug 2018 to Aug 2023, so the drop at the ends of the line is not a real decline.
>> The average employee rating is 2.97 out of 5.


Notes and limitations
>> The performance chart is a 100% stacked bar: it shows each department's share, not its size.
>> The three Gold tables are not linked by a shared Departments table, so a slicer filters only its own page. Adding a Departments dimension (star schema) is a next step.
>> The source data was already fairly clean. The quality checks are there because real pipelines need them.
>> Personal columns (email, date of birth) are removed in Silver and are not used in Gold or Power BI.
>> Built on Databricks Free Edition. Do not commit access tokens or connection details.

 What I learned
Medallion architecture, Delta Lake, Unity Catalog volumes, PySpark cleaning, data quality gates in a job, Lakeflow orchestration, and building a dashboard on Gold tables with DAX.
