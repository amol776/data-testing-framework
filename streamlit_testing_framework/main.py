import streamlit as st
import pandas as pd
import os
from data_processor import load_data, perform_comparison
from reports import generate_allure_report, generate_data_diff_report, generate_profiling_report
from difflib import SequenceMatcher

def get_column_similarity(col1: str, col2: str) -> float:
    """Calculate similarity ratio between two column names"""
    return SequenceMatcher(None, col1.lower(), col2.lower()).ratio()

def auto_map_columns(source_cols: list, target_cols: list) -> dict:
    """Automatically map columns based on name similarity"""
    mapping = {}
    for source_col in source_cols:
        # Find the most similar target column
        similarities = [(target_col, get_column_similarity(source_col, target_col)) 
                       for target_col in target_cols]
        best_match = max(similarities, key=lambda x: x[1])
        
        # If similarity is above threshold (0.6), add to mapping
        if best_match[1] > 0.6:
            mapping[source_col] = best_match[0]
    return mapping

def run_metadata_comparison(metadata_file):
    """Handle metadata-driven comparisons"""
    from metadata_processor import process_metadata_comparisons
    
    try:
        comparison_pairs = process_metadata_comparisons(metadata_file)
        
        if not comparison_pairs:
            st.warning("No valid comparison pairs found in metadata file.")
            return
            
        st.info(f"Found {len(comparison_pairs)} comparison pairs to process.")
        
        for i, pair in enumerate(comparison_pairs, 1):
            st.subheader(f"Comparison {i}: {pair['source_file']} vs {pair['target_file']}")
            
            try:
                # Load source and target data
                source_data = load_data(pair['source_type'], pair['source_file'], pair['source_separator'])
                target_data = load_data(pair['target_type'], pair['target_file'], pair['target_separator'])
                
                # Perform comparison
                results = perform_comparison(
                    source_data,
                    target_data,
                    pair['mapping'],
                    pair['ignored_columns']
                )
                
                # Generate reports
                allure_report = generate_allure_report(results)
                diff_report = generate_data_diff_report(source_data, target_data)
                profile_report = generate_profiling_report(source_data, target_data)
                
                # Display results and download buttons
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    with open(allure_report, "rb") as file:
                        st.download_button(
                            f"Download Allure Report - {i}",
                            file,
                            file_name=f"allure_report_{i}.html",
                            mime="text/html"
                        )
                
                with col2:
                    with open(diff_report, "rb") as file:
                        st.download_button(
                            f"Download Difference Report - {i}",
                            file,
                            file_name=f"difference_report_{i}.csv",
                            mime="text/csv"
                        )
                
                with col3:
                    with open(profile_report, "rb") as file:
                        st.download_button(
                            f"Download Profile Report - {i}",
                            file,
                            file_name=f"profile_report_{i}.html",
                            mime="text/html"
                        )
                        
            except Exception as e:
                st.error(f"Error processing comparison {i}: {str(e)}")
                
    except Exception as e:
        st.error(f"Error processing metadata file: {str(e)}")

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

    # Initialize session state
    if 'auto_mapping' not in st.session_state:
        st.session_state.auto_mapping = {}
    if 'ignored_columns' not in st.session_state:
        st.session_state.ignored_columns = set()

    # Define source options
    source_options = [
        "CSV file", "Dat file", "SQL Server", "Stored Procs",
        "Teradata", "API", "Parquet file", "Flat files inside zipped folder"
    ]

    # Comparison mode selection
    comparison_mode = st.radio(
        "Select Comparison Mode",
        ["Single Comparison", "Metadata-Driven Comparison"]
    )

    if comparison_mode == "Metadata-Driven Comparison":
        st.subheader("Metadata File Upload")
        metadata_file = st.file_uploader("Upload Metadata Excel File", type=["xlsx"])
        
        if metadata_file:
            if st.button("Run Metadata Comparisons"):
                run_metadata_comparison(metadata_file)
                
    else:
        # Original single comparison interface
        st.subheader("Single File Comparison")
        
        # Sidebar for source and target selection
        st.sidebar.header("Data Source Configuration")
        
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
                
                # Generate automatic mapping
                st.session_state.auto_mapping = auto_map_columns(
                    st.session_state.source_columns,
                    st.session_state.target_columns
                )
                
                st.session_state.mapping_done = True
                st.session_state.source_data = source_data
                st.session_state.target_data = target_data

            except Exception as e:
                st.error(f"Error loading data: {str(e)}")

        # Show mapping interface if data is loaded
        if st.session_state.get('mapping_done', False):
            st.subheader("Column Mapping Configuration")
            
            # Create three columns: one for checkboxes, one for source columns, one for target columns
            check_col, source_col, target_col = st.columns([0.2, 0.4, 0.4])
            
            with check_col:
                st.write("Include")
            with source_col:
                st.write("Source Columns")
            with target_col:
                st.write("Target Columns")

            # Initialize mapping dictionary
            mapping = {}
            ignored_columns = set()

            # Display mapping rows
            for source_col_name in st.session_state.source_columns:
                check_col, src_col, tgt_col = st.columns([0.2, 0.4, 0.4])
                
                # Checkbox for including/excluding column
                with check_col:
                    include = st.checkbox(
                        "##",
                        value=source_col_name not in st.session_state.ignored_columns,
                        key=f"include_{source_col_name}"
                    )
                    if not include:
                        ignored_columns.add(source_col_name)
                
                # Display source column name
                with src_col:
                    st.write(source_col_name)
                
                # Target column selection with auto-mapping
                with tgt_col:
                    default_idx = (
                        st.session_state.target_columns.index(st.session_state.auto_mapping[source_col_name])
                        if source_col_name in st.session_state.auto_mapping
                        else 0
                    )
                    target_col_name = st.selectbox(
                        "##",
                        [""] + st.session_state.target_columns,
                        index=default_idx + 1 if source_col_name in st.session_state.auto_mapping else 0,
                        key=f"map_{source_col_name}"
                    )
                    if target_col_name:
                        mapping[source_col_name] = target_col_name

            # Update session state
            st.session_state.ignored_columns = ignored_columns

            # Compare Button
            if st.button("Compare Data"):
                try:
                    with st.spinner("Performing comparison and generating reports..."):
                        # Perform comparison
                        results = perform_comparison(
                            st.session_state.source_data,
                            st.session_state.target_data,
                            mapping,
                            list(ignored_columns)
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
            
            # Generate automatic mapping
            st.session_state.auto_mapping = auto_map_columns(
                st.session_state.source_columns,
                st.session_state.target_columns
            )
            
            st.session_state.mapping_done = True
            st.session_state.source_data = source_data
            st.session_state.target_data = target_data

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")

    # Show mapping interface if data is loaded
    if st.session_state.get('mapping_done', False):
        st.subheader("Column Mapping Configuration")
        
        # Create three columns: one for checkboxes, one for source columns, one for target columns
        check_col, source_col, target_col = st.columns([0.2, 0.4, 0.4])
        
        with check_col:
            st.write("Include")
        with source_col:
            st.write("Source Columns")
        with target_col:
            st.write("Target Columns")

        # Initialize mapping dictionary
        mapping = {}
        ignored_columns = set()

        # Display mapping rows
        for source_col_name in st.session_state.source_columns:
            check_col, src_col, tgt_col = st.columns([0.2, 0.4, 0.4])
            
            # Checkbox for including/excluding column
            with check_col:
                include = st.checkbox(
                    "##",
                    value=source_col_name not in st.session_state.ignored_columns,
                    key=f"include_{source_col_name}"
                )
                if not include:
                    ignored_columns.add(source_col_name)
            
            # Display source column name
            with src_col:
                st.write(source_col_name)
            
            # Target column selection with auto-mapping
            with tgt_col:
                default_idx = (
                    st.session_state.target_columns.index(st.session_state.auto_mapping[source_col_name])
                    if source_col_name in st.session_state.auto_mapping
                    else 0
                )
                target_col_name = st.selectbox(
                    "##",
                    [""] + st.session_state.target_columns,
                    index=default_idx + 1 if source_col_name in st.session_state.auto_mapping else 0,
                    key=f"map_{source_col_name}"
                )
                if target_col_name:
                    mapping[source_col_name] = target_col_name

        # Update session state
        st.session_state.ignored_columns = ignored_columns

        # Compare Button
        if st.button("Compare Data"):
            try:
                with st.spinner("Performing comparison and generating reports..."):
                    # Perform comparison
                    results = perform_comparison(
                        st.session_state.source_data,
                        st.session_state.target_data,
                        mapping,
                        list(ignored_columns)
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
