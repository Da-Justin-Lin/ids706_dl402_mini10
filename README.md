[![CI](https://github.com/Da-Justin-Lin/ids706_dl402_mini10/actions/workflows/cicd.yml/badge.svg)](https://github.com/Da-Justin-Lin/ids706_dl402_mini10/actions/workflows/cicd.yml)
## Template for Python projects with RUFF linter

# PySpark Data Processing Project

This project uses **PySpark** to perform data processing and analysis on a large dataset. The project demonstrates data transformations, Spark SQL queries, and basic operations to handle and analyze data at scale using Apache Spark.

## Project Overview

This project provides a framework for data processing using PySpark, demonstrating:
- Data loading and cleaning
- Transformations and aggregations
- SQL-style queries for data analysis

The project is particularly suitable for handling large datasets and generating insights through Spark’s parallel processing capabilities.

## Requirements

- Python 3.8 or later
- [Apache Spark](https://spark.apache.org/)
- [PySpark](https://pypi.org/project/pyspark/) library

You can install dependencies with:
```bash
pip install -r requirements.txt
```

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Da-Justin-Lin/ids706_dl402_mini10.git
   cd pyspark-data-processing
   ```

2. **Install PySpark** (if not already installed):
   ```bash
   pip install pyspark
   ```

## Usage

To start using the PySpark project, follow these steps:

1. **Run the main PySpark script**:
   ```bash
   make test
   ```

2. **Modify the Dataset Path**:
   - If you’re using a different dataset, replace the file path in the script to load your data.

3. **Inspect Output**:
   - Results from transformations and Spark SQL queries will be printed in the console or saved to specified output files.

## Features

- **Data Transformation**: Filters and manipulates data based on specified conditions.
- **Spark SQL Queries**: Executes SQL-like queries on large datasets using Spark SQL.
- **Aggregation**: Performs aggregations like average, sum, and count for data analysis.
- **Extensible**: Easily add custom transformations and queries to suit your data analysis needs.

## Running Tests

Unit tests are included for core transformations and Spark SQL queries. To run tests:

1. Install [pytest](https://pypi.org/project/pytest/) if not already installed:
   ```bash
   make install
   ```

2. Run the tests:
   ```bash
   make test
   ```

Tests are designed to validate data processing steps and ensure the reliability of key functions.

## Project Structure

```
root/
│                     
├── main.py             # Main PySpark script
├── test_main.py        # Unit tests for the PySpark script
├── requirements.txt              # List of dependencies
└── README.md                     # Project documentation
```
