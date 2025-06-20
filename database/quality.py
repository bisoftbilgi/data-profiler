import streamlit as st
import pandas as pd
import pandera as pa
from pandera.typing import Series
import plotly.express as px
from datetime import datetime

def is_valid_datetime(series: Series) -> Series:
    """Custom check function to validate datetime values"""
    try:
        pd.to_datetime(series)
        return True
    except:
        return False

def get_available_tests(column_info):
    """Get available tests for a column based on its data type"""
    data_type = column_info[1].lower()
    tests = {
        'null_check': {
            'name': 'Null Value Check',
            'description': 'Check for null values',
            'available_for': 'all'
        },
        'distinct_check': {
            'name': 'Distinct Value Check',
            'description': 'Check for unique values',
            'available_for': 'all'
        },
        'range_check': {
            'name': 'Range Check',
            'description': 'Check if values are within valid range',
            'available_for': ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric']
        },
        'length_check': {
            'name': 'Length Check',
            'description': 'Check string length',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'datetime_check': {
            'name': 'Datetime Format Check',
            'description': 'Check if values are valid datetime',
            'available_for': ['date', 'datetime', 'timestamp']
        },
        'must_contain_at': {
            'name': 'Value must contain "@"',
            'description': 'Check if value contains "@"',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'no_numbers': {
            'name': 'Value must not contain numbers',
            'description': 'Check if value does not contain numbers',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'no_letters': {
            'name': 'Value must not contain letters',
            'description': 'Check if value does not contain letters',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'allowed_values': {
            'name': 'Value must be in allowed list',
            'description': 'Check if value is in allowed list',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'length_between': {
            'name': 'Value character length should be between min/max',
            'description': 'Check if value length is between min and max',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'eng_numeric_format': {
            'name': 'ENG Numeric Format',
            'description': 'Check if numeric values use dot (.) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double']
        },
        'tr_numeric_format': {
            'name': 'TR Numeric Format',
            'description': 'Check if numeric values use comma (,) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double']
        },
        'case_consistency': {
            'name': 'Case Consistency Check',
            'description': 'Check if all strings follow same casing (upper/lower/title)',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'category_cardinality': {
            'name': 'Category Cardinality Check',
            'description': 'Ensure number of unique values is within a threshold',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'future_date': {
            'name': 'Future Date Check',
            'description': 'Ensure dates are not in the future (or are in the future)',
            'available_for': ['date', 'datetime', 'timestamp']
        },
        'date_range': {
            'name': 'Date Range Check',
            'description': 'Ensure dates fall within a specific range',
            'available_for': ['date', 'datetime', 'timestamp']
        },
        'no_special_chars': {
            'name': 'No Special Characters',
            'description': "Ensure values don't contain unwanted symbols",
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'email_format': {
            'name': 'Email Format Check',
            'description': 'Full email validation using regex',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'regex_pattern': {
            'name': 'Regex Pattern Match',
            'description': 'Custom regular expression validation',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text']
        },
        'zscore_outlier': {
            'name': 'Z-Score Outlier Detection',
            'description': 'Flag values with high standard deviations from the mean',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint']
        },
        'integer_type': {
            'name': 'Integer Type Check',
            'description': 'Check if float values are actually integers',
            'available_for': ['decimal', 'numeric', 'float', 'double']
        },
        'positive_value': {
            'name': 'Positive Value Check',
            'description': 'Ensure values are non-negative or strictly positive',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint']
        },
        'column_correlation': {
            'name': 'Column Correlation Check',
            'description': 'Ensure two columns maintain logical correlation (e.g., start_date <= end_date)',
            'available_for': 'all'
        },
        'value_equality': {
            'name': 'Value Equality Check',
            'description': 'Ensure all values in a column are the same',
            'available_for': 'all'
        }
    }
    
    available_tests = {}
    for test_id, test_info in tests.items():
        if test_info['available_for'] == 'all' or data_type in test_info['available_for']:
            available_tests[test_id] = test_info
    
    return available_tests

def create_schema_for_column(column_info, selected_tests, custom_test_params=None):
    """Create a Pandera schema for a column based on selected tests"""
    col_name = column_info[0]
    data_type = column_info[1].lower()
    is_nullable = column_info[2] == 'YES'
    max_length = column_info[3]
    precision = column_info[4]
    scale = column_info[5]
    
    checks = []
    if custom_test_params is None:
        custom_test_params = {}
    # Add selected checks
    if 'null_check' in selected_tests:
        checks.append(pa.Check(lambda x: x.notna(), error="Value cannot be null"))
    if 'distinct_check' in selected_tests:
        checks.append(pa.Check(lambda x: x.nunique() > 0, error="Column must have at least one distinct value"))
    if 'range_check' in selected_tests and data_type in ['int', 'bigint', 'smallint', 'tinyint']:
        checks.append(
            pa.Check.in_range(
                min_value=-(2**(8*get_int_size(data_type)-1)),
                max_value=2**(8*get_int_size(data_type)-1)-1
            )
        )
    elif 'range_check' in selected_tests and data_type in ['decimal', 'numeric']:
        checks.append(
            pa.Check.in_range(
                min_value=-(10**precision),
                max_value=10**precision
            )
        )
    if 'length_check' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        checks.append(
            pa.Check.str_length(max_value=max_length)
        )
    if 'datetime_check' in selected_tests and data_type in ['date', 'datetime', 'timestamp']:
        checks.append(
            pa.Check(is_valid_datetime, error="Value must be a valid datetime")
        )
    if 'must_contain_at' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        checks.append(
            pa.Check(lambda s: s.str.contains("@", na=True), error="Value must contain '@'")
        )
    if 'no_numbers' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        checks.append(
            pa.Check(lambda s: ~s.str.contains(r"\\d", na=True), error="Value must not contain numbers")
        )
    if 'no_letters' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        checks.append(
            pa.Check(lambda s: ~s.str.contains(r"[a-zA-Z]", na=True), error="Value must not contain letters")
        )
    if 'allowed_values' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        allowed_values = custom_test_params.get('allowed_values', [])
        if allowed_values:
            checks.append(pa.Check.isin(allowed_values))
    if 'length_between' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        min_len = custom_test_params.get('min_len', 0)
        max_len = custom_test_params.get('max_len', 100)
        checks.append(pa.Check.str_length(min_value=min_len, max_value=max_len))
    if 'eng_numeric_format' in selected_tests and data_type in ['decimal', 'numeric', 'float', 'double']:
        # Accepts numbers like 1234.56 or -1234.56, but not 1,234.56 or 1234,56
        checks.append(pa.Check(lambda s: s.astype(str).str.match(r'^-?\d+(\.\d+)?$', na=True), error="Value must be in ENG format (dot as decimal separator)"))
    if 'tr_numeric_format' in selected_tests and data_type in ['decimal', 'numeric', 'float', 'double']:
        # Accepts numbers like 1234,56 or -1234,56, but not 1234.56
        checks.append(pa.Check(lambda s: s.astype(str).str.match(r'^-?\d+(,\d+)?$', na=True), error="Value must be in TR format (comma as decimal separator)"))
    if 'value_equality' in selected_tests:
        expected_value = custom_test_params.get('value_equality', None)
        if expected_value is not None:
            checks.append(pa.Check(lambda s: (s == expected_value).all(), error=f"All values must be {expected_value}"))
    if 'positive_value' in selected_tests and data_type in ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint']:
        strict = custom_test_params.get('positive_value_strict', False)
        if strict:
            checks.append(pa.Check(lambda s: (s > 0).all(), error="All values must be strictly positive"))
        else:
            checks.append(pa.Check(lambda s: (s >= 0).all(), error="All values must be non-negative"))
    if 'integer_type' in selected_tests and data_type in ['decimal', 'numeric', 'float', 'double']:
        checks.append(pa.Check(lambda s: s.dropna().apply(lambda x: float(x).is_integer()), error="All values must be integer-like (no decimals)"))
    if 'zscore_outlier' in selected_tests and data_type in ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint']:
        threshold = custom_test_params.get('zscore_outlier_threshold', 3)
        checks.append(pa.Check(lambda s: ((abs((s - s.mean()) / s.std(ddof=0)) <= threshold).all()), error=f"No value should be a z-score outlier (>|{threshold}|)"))
    if 'regex_pattern' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        pattern = custom_test_params.get('regex_pattern', None)
        if pattern:
            checks.append(pa.Check(lambda s: s.astype(str).str.match(pattern, na=True), error=f"Value must match regex: {pattern}"))
    if 'email_format' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        checks.append(pa.Check(lambda s: s.astype(str).str.match(email_regex, na=True), element_wise=False, error="Value must be a valid email address"))
    if 'no_special_chars' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        allowed = custom_test_params.get('no_special_chars_allowed', r'\w\s')
        regex = fr'^[{allowed}]+$'
        checks.append(pa.Check(lambda s: s.astype(str).str.match(regex, na=True), error="Value must not contain special characters"))
    if 'date_range' in selected_tests and data_type in ['date', 'datetime', 'timestamp']:
        min_date = custom_test_params.get('date_range_min', '1900-01-01')
        max_date = custom_test_params.get('date_range_max', '2100-01-01')
        checks.append(pa.Check(lambda s: (pd.to_datetime(s) >= pd.to_datetime(min_date)).all() and (pd.to_datetime(s) <= pd.to_datetime(max_date)).all(), error=f"Date must be between {min_date} and {max_date}"))
    if 'future_date' in selected_tests and data_type in ['date', 'datetime', 'timestamp']:
        must_be_future = custom_test_params.get('future_date', False)
        if must_be_future:
            checks.append(pa.Check(lambda s: (pd.to_datetime(s) > pd.Timestamp.now()).all(), error="Date must be in the future"))
        else:
            checks.append(pa.Check(lambda s: (pd.to_datetime(s) <= pd.Timestamp.now()).all(), error="Date must not be in the future"))
    if 'category_cardinality' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        threshold = custom_test_params.get('category_cardinality', 100)
        checks.append(pa.Check(lambda s: s.nunique() <= threshold, error=f"Number of unique values must be <= {threshold}"))
    if 'case_consistency' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
        case_type = custom_test_params.get('case_consistency', 'upper')
        if case_type == 'upper':
            checks.append(pa.Check(lambda s: s.astype(str).str.isupper().all(), error="All values must be uppercase"))
        elif case_type == 'lower':
            checks.append(pa.Check(lambda s: s.astype(str).str.islower().all(), error="All values must be lowercase"))
        elif case_type == 'title':
            checks.append(pa.Check(lambda s: s.astype(str).str.istitle().all(), error="All values must be title case"))
    # Column correlation check (handled at column level for now)
    if 'column_correlation' in selected_tests:
        other_col = custom_test_params.get('correlation_column', None)
        op = custom_test_params.get('correlation_operator', None)
        if other_col and op:
            if op == '<=':
                checks.append(pa.Check(lambda s, df: (s <= df[other_col]).all(), element_wise=False, error=f"Column must be <= {other_col}"))
            elif op == '>=':
                checks.append(pa.Check(lambda s, df: (s >= df[other_col]).all(), element_wise=False, error=f"Column must be >= {other_col}"))
            elif op == '<':
                checks.append(pa.Check(lambda s, df: (s < df[other_col]).all(), element_wise=False, error=f"Column must be < {other_col}"))
            elif op == '>':
                checks.append(pa.Check(lambda s, df: (s > df[other_col]).all(), element_wise=False, error=f"Column must be > {other_col}"))
            elif op == '==':
                checks.append(pa.Check(lambda s, df: (s == df[other_col]).all(), element_wise=False, error=f"Column must be == {other_col}"))
            elif op == '!=':
                checks.append(pa.Check(lambda s, df: (s != df[other_col]).all(), element_wise=False, error=f"Column must be != {other_col}"))
    return pa.Column(
        name=col_name,
        nullable=is_nullable,
        checks=checks
    )

def get_int_size(data_type):
    """Get the size in bytes for integer types"""
    sizes = {
        'tinyint': 1,
        'smallint': 2,
        'int': 4,
        'bigint': 8
    }
    return sizes.get(data_type, 4)

def run_quality_tests(connector, schema: str, table: str, selected_columns, selected_tests, custom_test_params=None):
    """Run quality tests on selected columns using selected tests"""
    try:
        # Get column information
        columns = connector.get_columns(schema, table)
        if not columns:
            st.error("No columns found in the table")
            return
        
        # Filter columns based on selection
        selected_columns_info = [col for col in columns if col[0] in selected_columns]
        
        # Create schema for each selected column
        schema_dict = {}
        for col in selected_columns_info:
            schema_dict[col[0]] = create_schema_for_column(col, selected_tests, custom_test_params)
        
        # Create the complete schema
        table_schema = pa.DataFrameSchema(schema_dict)
        
        # Get sample data
        sample_data = connector.get_sample_data(schema, table)
        if not sample_data:
            st.error("No data found in the table")
            return
        
        # Convert to DataFrame with column names
        col_names = [col[0] for col in columns]
        df = pd.DataFrame(sample_data, columns=col_names)
        
        # Run validation
        try:
            table_schema.validate(df)
            st.success("✅ All quality tests passed!")
            
            # Show validation summary
            st.subheader("Validation Summary")
            st.write("All columns passed their validation rules:")
            for col_name, col_schema in schema_dict.items():
                st.write(f"- {col_name}: {col_schema.checks}")
            
        except pa.errors.SchemaError as e:
            st.error("❌ Quality tests failed!")
            st.subheader("Validation Errors")
            if hasattr(e, 'failure_cases') and hasattr(e.failure_cases, 'to_dict'):
                failure_cases = e.failure_cases.drop_duplicates()
                if not failure_cases.empty:
                    st.dataframe(failure_cases)
                else:
                    st.write("No detailed failure cases available.")
            else:
                st.write(str(e))
        
        # Show data quality metrics
        st.subheader("Data Quality Metrics")
        
        # Calculate metrics for each column
        metrics = []
        for col in selected_columns_info:
            col_name = col[0]
            data_type = col[1].lower()
            
            # Get column details
            col_details = connector.get_column_details(schema, table, col_name)
            if not col_details:
                continue
            
            # Calculate metrics
            total_rows = len(df)
            null_count = col_details['null_count']
            distinct_count = col_details['distinct_count']
            
            metrics.append({
                'Column': col_name,
                'Data Type': data_type,
                'Total Rows': total_rows,
                'Null Count': null_count,
                'Null Percentage': (null_count / total_rows * 100) if total_rows > 0 else 0,
                'Distinct Count': distinct_count,
                'Distinct Percentage': (distinct_count / total_rows * 100) if total_rows > 0 else 0
            })
        
        # Display metrics as a table
        metrics_df = pd.DataFrame(metrics)
        st.dataframe(metrics_df)
        
        # Create visualizations
        st.subheader("Data Quality Visualizations")
        
        # Null value distribution
        fig = px.bar(metrics_df, x='Column', y='Null Percentage',
                    title='Null Value Distribution by Column',
                    labels={'Column': 'Column Name', 'Null Percentage': 'Null Values (%)'})
        st.plotly_chart(fig)
        
        # Distinct value distribution
        fig = px.bar(metrics_df, x='Column', y='Distinct Percentage',
                    title='Distinct Value Distribution by Column',
                    labels={'Column': 'Column Name', 'Distinct Percentage': 'Distinct Values (%)'})
        st.plotly_chart(fig)
        
    except Exception as e:
        st.error(f"Error running quality tests: {str(e)}")

def show_quality_tests_page(connector, schema: str):
    """Show the quality tests page"""
    st.title("Data Quality Tests")
    
    # Get all tables
    tables = connector.get_all_tables_and_views(schema)
    if not tables:
        st.warning("No tables found in the schema")
        return
    
    # Create table selector
    selected_table = st.selectbox(
        "Select a table to test:",
        [table[0] for table in tables]
    )
    
    if selected_table:
        # Get columns for selected table
        columns = connector.get_columns(schema, selected_table)
        if not columns:
            st.warning("No columns found in the selected table")
            return
        
        # Create column selector
        st.subheader("Select Columns to Test")
        selected_columns = st.multiselect(
            "Choose columns:",
            [col[0] for col in columns],
            default=[col[0] for col in columns]
        )
        
        # UI for custom test parameters
        custom_test_params = {}
        if selected_columns:
            # Get available tests for each selected column
            all_available_tests = {}
            for col in columns:
                if col[0] in selected_columns:
                    tests = get_available_tests(col)
                    for test_id, test_info in tests.items():
                        all_available_tests[test_id] = test_info
            
            # Group tests by type
            test_groups = {
                'Basic Checks': ['null_check', 'distinct_check', 'value_equality', 'column_correlation'],
                'Numeric Checks': [
                    'range_check', 'positive_value', 'integer_type', 'zscore_outlier', 'eng_numeric_format', 'tr_numeric_format'
                ],
                'String Checks': [
                    'length_check', 'must_contain_at', 'no_numbers', 'no_letters', 'allowed_values', 'length_between',
                    'regex_pattern', 'email_format', 'no_special_chars', 'case_consistency', 'category_cardinality'
                ],
                'Date Checks': [
                    'datetime_check', 'date_range', 'future_date'
                ]
            }
            
            selected_tests = []
            
            st.subheader("Select Tests to Run")
            for group_name, test_ids in test_groups.items():
                st.write(f"**{group_name}**")
                for test_id in test_ids:
                    if test_id in all_available_tests:
                        test_info = all_available_tests[test_id]
                        # Use a unique key per column and test
                        checkbox_key = f"test_{test_id}_{col[0]}"
                        if test_id == 'allowed_values':
                            allowed_values_str = st.text_input("Allowed values (comma separated):", key=f"allowed_values_input_{col[0]}")
                            if allowed_values_str:
                                custom_test_params['allowed_values'] = [v.strip() for v in allowed_values_str.split(",") if v.strip()]
                        if test_id == 'length_between':
                            min_len = st.number_input("Min length:", min_value=0, value=0, key=f"min_len_input_{col[0]}")
                            max_len = st.number_input("Max length:", min_value=0, value=100, key=f"max_len_input_{col[0]}")
                            custom_test_params['min_len'] = min_len
                            custom_test_params['max_len'] = max_len
                        if test_id == 'case_consistency':
                            case_type = st.selectbox(
                                f"Case type for {col[0]}",
                                ['upper', 'lower', 'title'],
                                key=f"case_type_select_{col[0]}"
                            )
                            custom_test_params['case_consistency'] = case_type
                        if st.checkbox(test_info['name'], key=checkbox_key):
                            selected_tests.append(test_id)
            
            if st.button("Run Quality Tests"):
                with st.spinner("Running quality tests..."):
                    run_quality_tests(connector, schema, selected_table, selected_columns, selected_tests, custom_test_params) 