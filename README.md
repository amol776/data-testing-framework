# Data Testing Framework

A comprehensive data testing framework with Streamlit UI for comparing data from various sources, generating test reports, and performing data quality checks.

## Features

- **Multiple Data Source Support:**
  - CSV files
  - DAT files
  - SQL Server databases
  - Stored Procedures
  - Teradata
  - API endpoints
  - Parquet files
  - Flat files in ZIP archives

- **Interactive UI Features:**
  - Source and target selection
  - Column mapping interface
  - Custom separator configuration
  - Ignore/include columns
  - One-click comparison

- **Comprehensive Reports:**
  1. Pytest + Allure Report with Data Quality Checks:
     - Row count validation
     - Duplicate detection
     - Null value analysis
     - Business rule validation
  2. Data Comparison Report
  3. Y-Data Profiling Report

- **Large File Support:**
  - Handles files >3GB using chunked processing
  - Memory-efficient operations

- **CI/CD Integration:**
  - GitLab CI/CD pipeline configuration
  - Automated testing
  - Report generation
  - Pipeline artifacts

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
cd streamlit_testing_framework
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the `streamlit_testing_framework` directory with your database connections:
```env
SQL_SERVER_CONNECTION=mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server
TERADATA_CONNECTION=teradatasql://username:password@server/database
```

## Usage

1. Start the Streamlit application:
```bash
cd streamlit_testing_framework
streamlit run main.py
```

2. Using the UI:
   - Select source and target types from the sidebar
   - Upload files or provide connection details
   - Configure column mapping
   - Select columns to ignore (optional)
   - Set custom separators for file-based sources (optional)
   - Click "Compare" to generate reports

3. Accessing Reports:
   - Download links will appear after comparison
   - Reports are saved in the `streamlit_testing_framework/reports` directory

## Running Tests

Run the test suite:
```bash
cd streamlit_testing_framework
pytest tests/test_framework.py --alluredir=./allure-results
```

Generate Allure report:
```bash
allure generate allure-results --clean -o allure-report
allure open allure-report
```

## CI/CD Pipeline

The included `.gitlab-ci.yml` configures a pipeline with:
- Dependency installation
- Test execution
- Report generation
- Report publishing to GitLab Pages

## Project Structure

```
streamlit_testing_framework/
├── main.py                 # Streamlit UI
├── data_processor.py       # Data loading and comparison logic
├── reports.py             # Report generation
├── requirements.txt       # Python dependencies
├── tests/
│   └── test_framework.py  # Test cases
└── reports/              # Generated reports directory
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
