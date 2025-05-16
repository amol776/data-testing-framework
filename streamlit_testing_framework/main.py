import streamlit as st
import pandas as pd
import os
from data_processor import load_data, perform_comparison
from reports import generate_allure_report, generate_data_diff_report, generate_profiling_report

def main():
    st.set_page_config(
        page_title="Data Testing Framework",
        page_icon="📊",
        layout="wide"
    )

    # Header
    st.title("📊 Data Testing & Comparison Framework")
    st.markdown("""
    This framework allows you to compare data from different sources and generate comprehensive test reports.
    """)

    # Sidebar for source and target selection
    st.sidebar.header("Data Source Configuration")
    
    source_options = [
        "CSV file", "Dat file", "SQL Server", "Stored Procs",
        "Teradata", "API", "Parquet file", "Flat files inside zipped folder"
    ]
    
    # Source Selection
    st.sidebar.subheader("Source Configuration")
    source_type = st.sidebar.selectbox("Select Source Type", source_options, key="source")
    
    # Target Selection
    st.sidebar.subheader("Target Configuration")
    target_type = st.sidebar.selectbox("Select Target Type", source_options, key="target")

    # Main content area with two columns
    col1, col2 = st.columns(2)

    # Source Data Input
    with col1:
        st.header("Source Data")
        source_data = None
        source_separator = None

        if source_type in ["CSV file", "Dat file", "Flat files inside zipped folder"]:
            source_separator = st.text_input("Source Separator", value=",", key="source_sep")
            source_file = st.file_uploader("Upload Source File", type=["csv", "dat", "zip"], key="source_file")
        elif source_type == "Parquet file":
            source_file = st.file_uploader("Upload Source Parquet File", type=["parquet"], key="source_parquet")
        else:
            st.text_input("Connection String", key="source_conn")
            st.text_area("Query", key="source_query")

    # Target Data Input
    with col2:
        st.header("Target Data")
        target_data = None
        target_separator = None

        if target_type in ["CSV file", "Dat file", "Flat files inside zipped folder"]:
            target_separator = st.text_input("Target Separator", value=",", key="target_sep")
            target_file = st.file_uploader("Upload Target File", type=["csv", "dat", "zip"], key="target_file")
        elif target_type == "Parquet file":
            target_file = st.file_uploader("Upload Target Parquet File", type=["parquet"], key="target_parquet")
        else:
            st.text_input("Connection String", key="target_conn")
            st.text_area("Query", key="target_query")

    # Column Mapping Section
    st.header("Column Mapping")
    if 'mapping_done' not in st.session_state:
        st.session_state.mapping_done = False

    if st.button("Load Data and Show Mapping"):
        try:
            # Load source and target data
            source_data = load_data(source_type, source_file if 'source_file' in locals() else None, source_separator)
            target_data = load_data(target_type, target_file if 'target_file' in locals() else None, target_separator)

            # Store column names in session state
            st.session_state.source_columns = source_data.columns.tolist()
            st.session_state.target_columns = target_data.columns.tolist()
            st.session_state.mapping_done = True

            # Store DataFrames in session state
            st.session_state.source_data = source_data
            st.session_state.target_data = target_data

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")

    # Show mapping interface if data is loaded
    if st.session_state.get('mapping_done', False):
        st.subheader("Column Mapping Configuration")
        
        mapping = {}
        ignored_columns = []
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("Source Columns")
            for source_col in st.session_state.source_columns:
                if not st.checkbox(f"Include {source_col}", value=True, key=f"src_{source_col}"):
                    ignored_columns.append(source_col)
                else:
                    target_col = st.selectbox(
                        f"Map {source_col} to:",
                        [""] + st.session_state.target_columns,
                        key=f"map_{source_col}"
                    )
                    if target_col:
                        mapping[source_col] = target_col

        with col2:
            st.write("Target Columns")
            for target_col in st.session_state.target_columns:
                if not st.checkbox(f"Include {target_col}", value=True, key=f"tgt_{target_col}"):
                    ignored_columns.append(target_col)

        # Compare Button
        if st.button("Compare Data"):
            try:
                with st.spinner("Performing comparison and generating reports..."):
                    # Perform comparison
                    results = perform_comparison(
                        st.session_state.source_data,
                        st.session_state.target_data,
                        mapping,
                        ignored_columns
                    )

                    # Generate reports
                    allure_report = generate_allure_report(results)
                    diff_report = generate_data_diff_report(st.session_state.source_data, st.session_state.target_data)
                    profile_report = generate_profiling_report(st.session_state.source_data, st.session_state.target_data)

                    # Display download buttons
                    st.success("Comparison complete! Download the reports below:")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        with open(allure_report, "rb") as file:
                            st.download_button(
                                "Download Allure Report",
                                file,
                                file_name="allure_report.html",
                                mime="text/html"
                            )
                    
                    with col2:
                        with open(diff_report, "rb") as file:
                            st.download_button(
                                "Download Difference Report",
                                file,
                                file_name="difference_report.csv",
                                mime="text/csv"
                            )
                    
                    with col3:
                        with open(profile_report, "rb") as file:
                            st.download_button(
                                "Download Profile Report",
                                file,
                                file_name="profile_report.html",
                                mime="text/html"
                            )

            except Exception as e:
                st.error(f"Error during comparison: {str(e)}")

if __name__ == "__main__":
    main()
