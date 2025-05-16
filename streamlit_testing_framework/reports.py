import pandas as pd
import os
import json
from datetime import datetime
from typing import Dict, Any
from ydata_profiling import ProfileReport
import subprocess
from pathlib import Path

# Create reports directory if it doesn't exist
REPORTS_DIR = Path("streamlit_testing_framework/reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def generate_allure_report(results: Dict[str, Any]) -> str:
    """
    Generate an Allure report based on the comparison results.
    Returns the path to the generated report.
    """
    try:
        # Create timestamp for unique report naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORTS_DIR / f"allure_report_{timestamp}.html"

        # Create HTML report
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Comparison Test Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background-color: #f8f9fa; padding: 20px; border-radius: 5px; }
                .section { margin: 20px 0; padding: 15px; border: 1px solid #dee2e6; border-radius: 5px; }
                .pass { color: green; }
                .fail { color: red; }
                .details { margin-left: 20px; }
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #dee2e6; padding: 8px; text-align: left; }
                th { background-color: #f8f9fa; }
            </style>
        </head>
        <body>
        """

        # Add header
        html_content += f"""
        <div class="header">
            <h1>Data Comparison Test Report</h1>
            <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </div>
        """

        # Add results sections
        for section, data in results.items():
            status = "pass" if data["passed"] else "fail"
            html_content += f"""
            <div class="section">
                <h2>{section.replace("_", " ").title()} <span class="{status}">{'✓' if status == 'pass' else '✗'}</span></h2>
            """

            if isinstance(data["details"], dict):
                html_content += "<table><tr><th>Metric</th><th>Value</th></tr>"
                for key, value in data["details"].items():
                    if isinstance(value, dict):
                        html_content += f"<tr><td colspan='2'><strong>{key}</strong></td></tr>"
                        for subkey, subvalue in value.items():
                            html_content += f"<tr><td>{subkey}</td><td>{subvalue}</td></tr>"
                    else:
                        html_content += f"<tr><td>{key}</td><td>{value}</td></tr>"
                html_content += "</table>"
            elif isinstance(data["details"], list):
                for item in data["details"]:
                    html_content += f"<div class='details'><pre>{json.dumps(item, indent=2)}</pre></div>"

            html_content += "</div>"

        html_content += """
        </body>
        </html>
        """

        # Write report to file
        with open(report_path, 'w') as f:
            f.write(html_content)

        return str(report_path)

    except Exception as e:
        raise Exception(f"Error generating Allure report: {str(e)}")

def generate_data_diff_report(source_df: pd.DataFrame, target_df: pd.DataFrame) -> str:
    """
    Generate a detailed difference report between source and target DataFrames.
    Returns the path to the generated report.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORTS_DIR / f"diff_report_{timestamp}.csv"

        # Ensure both DataFrames have the same columns
        common_columns = list(set(source_df.columns) & set(target_df.columns))
        source_df = source_df[common_columns].copy()
        target_df = target_df[common_columns].copy()

        # Add a source identifier column
        source_df['_source'] = 'source'
        target_df['_source'] = 'target'

        # Combine DataFrames and find differences
        combined_df = pd.concat([source_df, target_df])
        diff_df = combined_df.drop_duplicates(subset=common_columns, keep=False)

        # Add additional metadata
        diff_df['_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Save to CSV
        diff_df.to_csv(report_path, index=False)
        
        return str(report_path)

    except Exception as e:
        raise Exception(f"Error generating difference report: {str(e)}")

def generate_profiling_report(source_df: pd.DataFrame, target_df: pd.DataFrame) -> str:
    """
    Generate a Y-Data Profiling report comparing source and target DataFrames.
    Returns the path to the generated report.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORTS_DIR / f"profile_report_{timestamp}.html"

        # Create profile reports
        source_profile = ProfileReport(
            source_df,
            title="Source Data Profile",
            explorative=True,
            minimal=True
        )
        
        target_profile = ProfileReport(
            target_df,
            title="Target Data Profile",
            explorative=True,
            minimal=True
        )

        # Compare profiles
        comparison_report = source_profile.compare(target_profile)
        
        # Save comparison report
        comparison_report.to_file(report_path)
        
        return str(report_path)

    except Exception as e:
        raise Exception(f"Error generating profiling report: {str(e)}")

def run_pytest_with_allure() -> None:
    """
    Run pytest with Allure reporting enabled.
    This function is called when running tests through CI/CD.
    """
    try:
        # Ensure allure results directory exists
        allure_results_dir = REPORTS_DIR / "allure-results"
        allure_results_dir.mkdir(parents=True, exist_ok=True)

        # Run pytest with allure
        subprocess.run([
            "pytest",
            "--alluredir",
            str(allure_results_dir),
            "streamlit_testing_framework/tests"
        ], check=True)

        # Generate allure report
        subprocess.run([
            "allure",
            "generate",
            str(allure_results_dir),
            "--clean",
            "-o",
            str(REPORTS_DIR / "allure-report")
        ], check=True)

    except Exception as e:
        raise Exception(f"Error running pytest with Allure: {str(e)}")
