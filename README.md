# Astellas Data Project

This challenge focuses on building a robust ETL (Extract, Transform, Load) solution for an insurance company's billing system data. 

## Table of Contents

- [Project Description](#project-description)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [SQL Queries](#sql-queries)
- [Future Scope](#future-scope)

## Project Description <a name="project-description"></a>

The system handles three main data feeds: Patient Information, Provider Information, and Claim Information. It aims to make this data accessible for analysis by data scientists. Key features include data extraction from CSV files, rigorous data transformation adhering to specific business rules, and data storage in an SQLite database.

The project follows best engineering practices, including data type conversions, duplicate handling, and index management. It consists of three main components: DataExtractor (for CSV file reading), DataTransformer (for data transformation), and DataLoader (for database interaction). The main script, main.py, orchestrates the entire data processing workflow.

To improve the user experience for analysis, I connected my SQLite database to a database viewer tool (DBeaver). This tool offers a user-friendly interface for viewing table data and running queries. You can download DBeaver from [here](https://dbeaver.io/download/)

The README provides comprehensive instructions for setup, execution, and suggests potential enhancements for the solution.

## Getting Started <a name="getting-started"></a>

To get started with this project, follow these instructions:

### Prerequisites <a name="prerequisites"></a>

1. **Git**: You need Git installed on your local machine to clone the repository. If you don't have Git, you can download it [here](https://git-scm.com/downloads).

2. **Python**: This project is built using Python. Make sure you have Python installed. You can download Python from [here](https://www.python.org/downloads/).

### Installation <a name="installation"></a>

Use Git to clone this repository to your local machine:

```bash
git clone https://github.com/aprince0/astellas-data-project.git
```
## Usage <a name="usage"></a>

### To use the project: 

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the ETL process:
```bash
python main.py
```

3. To run the tests, use the following command:
```bash
pytest -v test/test_data_transformer.py
```

4. Access the processed data stored in astellas.db. You can use SQLite clients or Python libraries for data access.



## SQL Queries <a name="sql-queries"></a>

1. Get all claims that have not been billed. The results should include the claim information as well as the patient’s name, patient’s address and provider’s name.

```bash
WITH t1 AS (
	SELECT claim_id, provider_id, fname, lname, address, city, state
	FROM claim c
	LEFT JOIN patient p ON c.patient_id = p.patient_id
	WHERE billed = 'F'
),
t2 AS (
	SELECT claim_id, t1.fname as patient_firstname, t1.lname as patient_lastname, t1.address, t1.city, pr.fname AS provider_fname, pr.lname AS provider_lname
	FROM t1
	LEFT JOIN provider pr ON t1.provider_id = pr.provider_id
)
SELECT *
FROM t2;
```

2. Get all providers who have had claims in the last 30 days where the insurance claim must be 100% covered.

```bash
SELECT DISTINCT p.provider_id, p.fname
FROM provider p
JOIN claim c ON p.provider_id = c.provider_id
WHERE c.claim_date >= date('now', '-30 days') -- This would not give us a result as there is no data for last 30 days from the current date. But you can try changing this to -2000 to view the sample result
AND c.coverage_type = '100';
```

3. Get all patients who have had a surgery claim with a specific provider id. 

```bash
WITH t1 AS (
    SELECT p.patient_id, c.provider_id, p.fname AS first_name, p.lname AS last_name
    FROM claim c
    LEFT JOIN patient p ON p.patient_id = c.patient_id
    WHERE c.visit_type = 'SURGERY'
),
t2 AS (
    SELECT DISTINCT patient_id, first_name, last_name
    FROM t1
    LEFT JOIN provider pr ON t1.provider_id = pr.provider_id
    WHERE pr.specialty_code = 'HEART_SURGEON'
)
SELECT * FROM t2;
```

## Future Scope <a name="future-scope"></a>
 
Here are some future improvements and enhancements to the project:

1. **Orchestrate the Pipeline with Airflow Triggers**: Automate the pipeline using Apache Airflow triggers to initiate processing as soon as new CSV files are updated from the source. Additionally, implement a notification system to inform data owners once the process is completed successfully.

2. **Enhance Data Source Flexibility**: Ensure the codebase is adaptable to various data sources beyond just CSV, such as JSON or any other incoming formats. This makes the system more versatile and future-proof.

3. **Implement Data Quality Metrics**: Incorporate data quality metrics within the ETL pipeline to assess the accuracy and completeness of data transformations. Create a user-friendly dashboard using QlikSense or Tableau for engineers and managers to monitor the quality of data products. 
   1. Check for Data Duplication: Introduce checks to identify and eliminate duplicate records within incoming data. 
   2. Validate Data Based on Business Rules: Implement business rule validations, e.g., verifying ZIP codes according to state patterns and address validity using a Geo-spatial library.


4. **Consider Spark for Large Volumes**: When dealing with massive data volumes, consider using Apache Spark for efficient data processing.

5. **Optimize Performance**: Improve table performance by leveraging technologies like Hive, serverless Google Cloud Platform (GCP), or Redshift, and consider partitioning strategies.

6. **Implement Comprehensive Logging**: Enhance monitoring capabilities by adding detailed logs that track which files have been processed and their status.

7. Include additional test cases to thoroughly assess the functionality of the code.


8. **Implement Comprehensive Logging**: Enhance monitoring and tracking capabilities by adding detailed logging. This should include tracking which files have been processed, their processing status, and any errors or issues encountered during the ETL process. Comprehensive logging aids in troubleshooting and auditing.

9. **Additional Test Cases**: Expand the test suite by including additional test cases that thoroughly assess the functionality of the code. This ensures robust and reliable data processing.

These future improvements aim to enhance the project's capabilities, scalability, and maintainability, allowing it to evolve in response to changing requirements and data sources.


Please refer to [Google Doc](https://docs.google.com/document/d/1ICRSASGg2A2zOalfwnUziY5DA0MjGpRd6m3P2MO1n0s/edit?usp=sharing), if needed.
