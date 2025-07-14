import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from database.utils import load_db_config, check_connection

PASS_ICON = "\u2705"  # ✅
FAIL_ICON = "\u274C"  # ❌

@st.cache_data(show_spinner=False)
def get_cached_table_analysis(_connector, schema, table):
    return _connector.get_table_analysis(schema, table)

@st.cache_data(show_spinner=False)
def get_cached_columns(_connector, schema, table):
    return _connector.get_columns(schema, table)

@st.cache_data(show_spinner=False)
def get_all_cached_tables_and_views(_connector, schema):
    return _connector.get_all_tables_and_views(schema)




def get_available_tests(column_info):
    data_type = column_info[1].lower()
    return {
        'null_check': {
            'name': 'Column Values to be Not Null',
            'description': 'Check for null values',
            'available_for': 'all'
        },
        'distinct_check': {
            'name': 'Column Values to be All Distinct',
            'description': 'Check for all values are distinct',
            'available_for': 'all'
        },
        'range_check': {
            'name': 'Min-Max Range Check',
            'description': 'Check values are within min/max range for numeric columns',
            'available_for': ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'real', 'number']
        },
        'length_check': {
            'name': 'String Length Check',
            'description': 'Check string length range',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
        'datetime_check': {
            'name': 'Datetime Format Check',
            'description': 'Check if values are valid datetime',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)']
        },
        'letter_check': {
            'name': 'Letter Not to be Present',
            'description': 'Check for letters in the column',
            'available_for': 'all'
        },
        'number_check': {   
            'name': 'Number Not to be Present',
            'description': 'Check for numbers in the column',
            'available_for': 'all'
        },
        'allowed_values': {
            'name': 'Value must be in allowed list',
            'description': 'Check if value is in allowed list',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
        'eng_numeric_format': {
            'name': 'ENG Numeric Format',
            'description': 'Check if numeric values use dot (.) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'real', 'number']
        },
        'tr_numeric_format': {
            'name': 'TR Numeric Format',
            'description': 'Check if numeric values use comma (,) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'real', 'number']
        },
        'case_consistency': {
            'name': 'Case Consistency Check',
            'description': 'Check if all strings follow same casing (upper/lower)',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
        'future_date': {
            'name': 'Future Date Check',
            'description': 'Ensure dates are not in the future (or are in the future)',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)']
        },
        'date_range': {
            'name': 'Date Range Check',
            'description': 'Ensure dates fall within a specific range',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)', 'timestamp(6)(11)']
        },
        'no_special_chars': {
            'name': 'No Special Characters',
            'description': "Ensure values don't contain unwanted symbols",
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
        'email_format': {
            'name': 'Email Format Check',
            'description': 'Full email validation using regex',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
        'regex_pattern': {
            'name': 'Regex Pattern Match',
            'description': 'Custom regular expression validation',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2']
        },
            'positive_value': {
            'name': 'Positive Value Check',
            'description': 'Ensure values are non-negative or strictly positive',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint', 'real', 'number']
        }
    }

def create_schema_for_column(column_info, selected_tests, custom_test_params=None):
    return None

def run_quality_tests(connector, schema: str, table: str, selected_columns, selected_tests, custom_test_params=None):
    st.subheader("Running Data Quality Checks")
    columns = get_cached_columns(connector, schema, table)
    selected_columns_info = [col for col in columns if col[0] in selected_columns]

    table_analysis = get_cached_table_analysis(connector, schema, table)
    total_rows = table_analysis.get('row_count', 0)
    violated_rows_by_column = {}

    metrics = []
    for col in selected_columns_info:
        col_name, data_type = col[0], col[1].lower()
        null_count = distinct_count = letter_count = number_count = invalid_datetime_count = None
        null_pass = distinct_pass = letter_pass = number_pass = datetime_pass = None
        range_stats = length_stats = None
        range_pass = length_pass = None
        allowed_values_pass = None
        eng_numeric_format_pass = tr_numeric_format_pass = case_inconsistency_pass = future_date_pass = date_range_pass = special_char_pass = None
        eng_numeric_format_violation_count = tr_numeric_format_violation_count = case_inconsistency_count = future_date_violation_count = date_range_violation_count = special_char_violation_count = None
        allowed_values_violation_count = allowed_values_non_violation_count = None
        regex_pattern_violation_count = None
        regex_pattern_pass = None
        positive_value_violation_count = None
        positive_value_pass = None
        invalid_datetime_count = None
        datetime_pass = None

        print(data_type)


        try:
            if 'null_check' in selected_tests:
                null_count = None
                null_count = connector.get_null_count(schema, table, col_name)
                
                if null_count==0:
                    null_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'null_check')] = connector.get_null_violations(schema, table, col_name)
                    null_pass = FAIL_ICON
        except:
            null_count = None

        try:
            if 'distinct_check' in selected_tests:
                distinct_count = connector.get_distinct_count(schema, table, col_name) 
                if distinct_count==total_rows:
                    distinct_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'distinct_check')] = connector.get_non_distinct_violations(schema, table, col_name)
                    distinct_pass = FAIL_ICON
        except:
            distinct_count = None

        try:
            if 'range_check' in selected_tests and data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'real','number']:
                range_stats = connector.get_min_max_range(schema, table, col_name)
                user_min = custom_test_params.get('range_check_min')
                user_max = custom_test_params.get('range_check_max')
                range_pass = None
                if user_min is not None and user_max is not None and range_stats is not None:
                    passed = user_min <= range_stats.get("min", 0) and range_stats.get("max", 0) <= user_max
                    if passed:
                        range_pass = PASS_ICON 
                    else:
                        violated_rows_by_column[(col_name, 'range_check')] = connector.get_min_max_violations(schema, table, col_name, user_min, user_max)
                        range_pass = FAIL_ICON
            else:
                range_stats = None
                range_pass = None
        except:
            range_stats = None
            range_pass = None

        try:
            if 'length_check' in selected_tests and data_type in ['varchar', 'char', 'nvarchar', 'nchar', 'text']:
                length_stats = connector.get_char_length_range(schema, table, col_name)
                user_min = custom_test_params.get('length_check_min')
                user_max = custom_test_params.get('length_check_max')
                length_pass = None
                if user_min is not None and user_max is not None and length_stats is not None:
                    passed = user_min <= length_stats.get("min_length", 0) and length_stats.get("max_length", 0) <= user_max
                    if passed:
                        length_pass = PASS_ICON 
                    else:
                        violated_rows_by_column[(col_name, 'length_check')] = connector.get_char_length_violations(schema, table, col_name, user_min, user_max)
                        length_pass = FAIL_ICON
            else:
                length_stats = None
                length_pass = None
        except:
            length_stats = None

        try:
            if 'datetime_check' in selected_tests:
                datetime_check_regex = custom_test_params.get('datetime_check_regex')
                datetime_check_format = custom_test_params.get('datetime_check')
                print("datettime_check_format", datetime_check_format)
                invalid_datetime_count = connector.get_invalid_datetime_count(schema, table, col_name, datetime_check_format, datetime_check_regex)
                print("invalid_datetime_count", invalid_datetime_count)
                
                if invalid_datetime_count == 0:
                    
                    datetime_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'datetime_check')] = connector.get_invalid_datetime_violations(schema, table, col_name, datetime_check_format, datetime_check_regex)
                    datetime_pass = FAIL_ICON
            else:
                invalid_datetime_count = None
                datetime_pass = None
        except:
            invalid_datetime_count = None
        
        try:
            if 'letter_check' in selected_tests:
                letter_count = connector.get_letter_count(schema, table, col_name)
                letter_pass = None
                if letter_count == 0:
                    letter_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'letter_check')] = connector.get_letter_violations(schema, table, col_name)
                    letter_pass = FAIL_ICON
            else:
                letter_count = None
                letter_pass = 'else'
        except Exception as e:
            letter_count = None
            letter_pass = f'❌ ({str(e)})'

        try:
            if 'number_check' in selected_tests:
                number_count = connector.get_number_count(schema, table, col_name)
                number_pass = None
                if number_count == 0:
                    number_pass = PASS_ICON
                else:

                    number_pass = FAIL_ICON
                    violated_rows_by_column[(col_name, 'number_check')] = connector.get_number_violations(schema, table, col_name)
            else:
                number_count = None
                number_pass = None
        except:
            number_count = None
            number_pass = None

        try:
            if 'allowed_values' in selected_tests:
                allowed_values_str = custom_test_params.get('allowed_values_str')
                allowed_values_pass = None
                if allowed_values_str:
                    allowed_values_list = [val.strip() for val in allowed_values_str.split(',')]
                    result = connector.get_allowed_values_violation_count(schema, table, col_name, allowed_values_list)
                    allowed_values_violation_count = result['violation']
                    allowed_values_non_violation_count = result['non_violation']
                    if allowed_values_violation_count == 0:
                        allowed_values_pass = PASS_ICON 
                    else:
                        violated_rows_by_column[(col_name, 'allowed_values')] = connector.get_allowed_values_violations(schema, table, col_name, allowed_values_list)
                        allowed_values_pass = FAIL_ICON
                else:
                    allowed_values_violation_count = None
                    allowed_values_non_violation_count = None
                    allowed_values_pass = None
            else:
                allowed_values_violation_count = None
                allowed_values_non_violation_count = None
                allowed_values_pass = None
        except Exception as e:
            allowed_values_violation_count = None
            allowed_values_non_violation_count = None
            allowed_values_pass = f"{FAIL_ICON} ({str(e)})"

        try:
            if 'eng_numeric_format' in selected_tests:
                eng_numeric_format_violation_count = connector.get_eng_numeric_format_violation_count(schema, table, col_name)
                eng_numeric_format_pass = None
                if eng_numeric_format_violation_count == 0:
                    eng_numeric_format_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'eng_numeric_format')] = connector.get_eng_numeric_format_violations(schema, table, col_name)
                    eng_numeric_format_pass = FAIL_ICON
            else:
                eng_numeric_format_violation_count = None
                eng_numeric_format_pass = None
        except Exception as e:
            eng_numeric_format_violation_count = None
            eng_numeric_format_pass = f"{FAIL_ICON} ({str(e)})"
        try:
            if 'tr_numeric_format' in selected_tests:
                tr_numeric_format_violation_count = connector.get_tr_numeric_format_violation_count(schema, table, col_name)
                tr_numeric_format_pass = None
                if tr_numeric_format_violation_count == 0:
                    tr_numeric_format_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'tr_numeric_format')] = connector.get_tr_numeric_format_violations(schema, table, col_name)
                    tr_numeric_format_pass = FAIL_ICON
            else:
                tr_numeric_format_violation_count = None
                tr_numeric_format_pass = None
        except Exception as e:
            tr_numeric_format_violation_count = None
            tr_numeric_format_pass = f"{FAIL_ICON} ({str(e)})"
        try:
            if 'case_consistency' in selected_tests:
                case_consistency = custom_test_params.get('case_consistency')
                case_inconsistency_count = connector.get_case_inconsistency_count(schema, table, col_name, case_consistency)
                case_inconsistency_pass = None
                if case_inconsistency_count == 0:
                    case_inconsistency_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'case_consistency')] = connector.get_case_inconsistency_violations(schema, table, col_name, case_consistency)
                    case_inconsistency_pass = FAIL_ICON
            else:
                case_inconsistency_count = None
                case_inconsistency_pass = None
        except Exception as e:
            case_inconsistency_count = None
            case_inconsistency_pass = f"{FAIL_ICON} ({str(e)})"
        try:
            if 'future_date' in selected_tests:
                future_date_violation_count = connector.get_future_date_violation_count(schema, table, col_name)
                future_date_pass = None
                if future_date_violation_count == 0:
                    future_date_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'future_date')] = connector.get_future_date_violations(schema, table, col_name)
                    future_date_pass = FAIL_ICON
            else:
                future_date_violation_count = None
                future_date_pass = None
        except Exception as e:
            future_date_violation_count = None
            future_date_pass = f"{FAIL_ICON} ({str(e)})"
        try:
            if 'date_range' in selected_tests:
                start_date = custom_test_params.get('start_date')
                end_date = custom_test_params.get('end_date')
                date_range_violation_count = connector.get_date_range_violation_count(schema, table, col_name, start_date, end_date)
                date_range_pass = None
                if date_range_violation_count == 0:
                    date_range_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'date_range')] = connector.get_date_range_violations(schema, table, col_name, start_date, end_date)
                    date_range_pass = FAIL_ICON
            else:
                date_range_violation_count = None
                date_range_pass = None
        except Exception as e:
            date_range_violation_count = None
            date_range_pass = f"{FAIL_ICON} ({str(e)})"
        try:
            if 'no_special_chars' in selected_tests:
                allowed_pattern = custom_test_params.get('allowed_pattern')
                special_char_violation_count = connector.get_special_char_violation_count(schema, table, col_name, allowed_pattern)
                special_char_pass = None
                if special_char_violation_count == 0:
                    special_char_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'no_special_chars')] = connector.get_special_char_violations(schema, table, col_name, allowed_pattern)
                    special_char_pass = FAIL_ICON
            else:
                special_char_violation_count = None
                special_char_pass = None
        except Exception as e:
            special_char_violation_count = None
            special_char_pass = f"{FAIL_ICON} ({str(e)})"

        try:
            if 'email_format' in selected_tests:
                email_format_violation_count = connector.get_email_format_violation_count(schema, table, col_name)
                email_format_pass = None
                if email_format_violation_count == 0:
                    email_format_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'email_format')] = connector.get_email_format_violations(schema, table, col_name)
                    email_format_pass = FAIL_ICON
            else:
                email_format_violation_count = None
                email_format_pass = None
        except Exception as e:
            email_format_violation_count = None
            email_format_pass = f"{FAIL_ICON} ({str(e)})"

        try:
            if 'regex_pattern' in selected_tests:
                regex_pattern = custom_test_params.get('regex_pattern')
                print(" try icinde regex_pattern", regex_pattern)
                regex_pattern_violation_count = connector.get_regex_pattern_violation_count(schema, table, col_name, regex_pattern)
                
                if regex_pattern_violation_count == 0:
                    regex_pattern_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'regex_pattern')] = connector.get_regex_pattern_violations(schema, table, col_name, regex_pattern)
                    regex_pattern_pass = FAIL_ICON
            else:
                regex_pattern_violation_count = None
                regex_pattern_pass = None
        except Exception as e:
            regex_pattern_violation_count = None

        try:
            if 'positive_value' in selected_tests:
                strict = custom_test_params.get('strict')
                positive_value_violation_count = connector.get_positive_value_violation_count(schema, table, col_name, strict)
                positive_value_pass = None
                if positive_value_violation_count == 0:
                    positive_value_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'positive_value')] = connector.get_positive_value_violations(schema, table, col_name, strict)
                    positive_value_pass = FAIL_ICON
            else:
                positive_value_violation_count = None
                positive_value_pass = None
        except Exception as e:
            positive_value_violation_count = None
            positive_value_pass = f"{FAIL_ICON} ({str(e)})"

        metrics.append({
            'Column': col_name,
            'Data Type': data_type,
            'Null Count': null_count,
            'Null Pass': null_pass,
            'Null %': (null_count / total_rows * 100) if total_rows and null_count is not None else None,
            'Distinct Count': distinct_count,
            'Distinct Pass': distinct_pass,
            'Distinct %': (distinct_count / total_rows * 100) if total_rows and distinct_count is not None else None,
            'Min': range_stats.get("min") if range_stats else None,
            'Max': range_stats.get("max") if range_stats else None,
            'Range': range_stats.get("range") if range_stats else None,
            'Range Pass': range_pass,
            'Min Length': length_stats.get("min_length") if length_stats else None,
            'Max Length': length_stats.get("max_length") if length_stats else None,
            'Length Pass': length_pass,
            'Invalid Datetime Count': invalid_datetime_count,
            'Datetime Pass': datetime_pass,
            'Letter Count': letter_count,
            'Letter Pass': letter_pass,
            'Number Count': number_count,
            'Number Pass': number_pass,
            'Allowed Values Violation Count': allowed_values_violation_count,
            'Allowed Values Non Violation Count': allowed_values_non_violation_count,
            'Allowed Values Pass': allowed_values_pass,
            'ENG Numeric Format Violation Count': eng_numeric_format_violation_count,
            'ENG Numeric Format Pass': eng_numeric_format_pass,
            'TR Numeric Format Violation Count': tr_numeric_format_violation_count,
            'TR Numeric Format Pass': tr_numeric_format_pass,
            'Case Inconsistency Count': case_inconsistency_count,
            'Case Inconsistency Pass': case_inconsistency_pass,
            'Future Date Violation Count': future_date_violation_count,
            'Future Date Pass': future_date_pass,
            'Date Range Violation Count': date_range_violation_count,
            'Date Range Pass': date_range_pass,
            'Special Char Violation Count': special_char_violation_count,
            'Special Char Pass': special_char_pass,
            'Email Format Violation Count': email_format_violation_count,
            'Email Format Pass': email_format_pass,
            'Regex Pattern Violation Count': regex_pattern_violation_count,
            'Regex Pattern Pass': regex_pattern_pass,
            'Positive Value Violation Count': positive_value_violation_count,
            'Positive Value Pass': positive_value_pass
        })

    df = pd.DataFrame(metrics)
    st.subheader("Validation Summary")
    display_cols = ['Column', 'Data Type']
    if 'null_check' in selected_tests:
        display_cols += ['Null Count', 'Null Pass', 'Null %']
    if 'distinct_check' in selected_tests:
        display_cols += ['Distinct Count', 'Distinct Pass', 'Distinct %']
    if 'range_check' in selected_tests:
        display_cols += ['Min', 'Max', 'Range', 'Range Pass']
    if 'length_check' in selected_tests:
        display_cols += ['Min Length', 'Max Length', 'Length Pass']
    if 'datetime_check' in selected_tests:
        display_cols += ['Invalid Datetime Count', 'Datetime Pass']
    if 'letter_check' in selected_tests:
        display_cols += ['Letter Count', 'Letter Pass']
    if 'number_check' in selected_tests:
        display_cols += ['Number Count', 'Number Pass']
    if 'allowed_values' in selected_tests:
        display_cols += ['Allowed Values Violation Count', 'Allowed Values Non Violation Count', 'Allowed Values Pass']
    if 'eng_numeric_format' in selected_tests:
        display_cols += ['ENG Numeric Format Violation Count', 'ENG Numeric Format Pass']
    if 'tr_numeric_format' in selected_tests:
        display_cols += ['TR Numeric Format Violation Count', 'TR Numeric Format Pass']
    if 'case_consistency' in selected_tests:
        display_cols += ['Case Inconsistency Count', 'Case Inconsistency Pass']
    if 'future_date' in selected_tests:
        display_cols += ['Future Date Violation Count', 'Future Date Pass']
    if 'date_range' in selected_tests:
        display_cols += ['Date Range Violation Count', 'Date Range Pass']
    if 'no_special_chars' in selected_tests:
        display_cols += ['Special Char Violation Count', 'Special Char Pass']
    if 'email_format' in selected_tests:
        display_cols += ['Email Format Violation Count', 'Email Format Pass']
    if 'regex_pattern' in selected_tests:
        display_cols += ['Regex Pattern Violation Count', 'Regex Pattern Pass']
    if 'positive_value' in selected_tests:
        display_cols += ['Positive Value Violation Count', 'Positive Value Pass']
    st.dataframe(df[display_cols])

    print("violated_rows_by_column calısıyor keys:", violated_rows_by_column.keys())

    if violated_rows_by_column:
        st.subheader("Violated Rows Preview")
        print("violated_rows_by_column burada", violated_rows_by_column)
        for (col_name, test_name), rows in violated_rows_by_column.items():
            if rows:
                st.markdown(f"**{col_name} – {test_name}**")
                try:
                    df_rows = pd.DataFrame(rows, columns=[col[0] for col in columns])
                    st.dataframe(df_rows.head(10))  # show only first 10 violating rows
                except Exception as e:
                    st.warning(f"Error showing violations for {col_name} – {test_name}: {e}")



def show_quality_tests_page(connector, schema: str):
        # ✅ Safely initialize db_config ONCE
    if "db_config" not in st.session_state:
        db_config = load_db_config()
        st.session_state.db_config = db_config
    else:
        db_config = st.session_state.db_config
    st.title("Data Quality Checks")
    connector.ensure_connected(st.session_state.db_config)

    tables = get_all_cached_tables_and_views(connector, schema)
    if not tables:
        st.warning("No tables found in the schema")
        return

    selected_table = st.selectbox("Select a table to test:", [table[0] for table in tables])
    if not selected_table:
        return
    connector.ensure_connected(st.session_state.db_config)

    columns = get_cached_columns(connector, schema, selected_table)
    if not columns:
        st.warning("No columns found in the selected table")
        return

    st.subheader("Sample Data Preview")
    # Only extract config once and reuse
    if "db_config" not in st.session_state:
        db_config = load_db_config()
        st.session_state.db_config = db_config
    else:
        db_config = st.session_state.db_config


    connector.ensure_connected(db_config)

    try:
        connector.ensure_connected(st.session_state.db_config)
        sample_data = connector.get_sample_data(schema, selected_table, limit=100)
        if sample_data:
            sample_df = pd.DataFrame(sample_data, columns=[col[0] for col in columns])
            st.dataframe(sample_df.head(10))
        else:
            st.info("No sample data returned.")
    except Exception as e:
        st.error(f"Error retrieving sample data: {str(e)}")
    selected_columns = st.multiselect("Choose columns:", [col[0] for col in columns], default=[col[0] for col in columns])

    all_tests = ['null_check', 'distinct_check', 'range_check', 'length_check', 'datetime_check', 'letter_check', 'number_check', 'allowed_values', 'eng_numeric_format', 'tr_numeric_format', 'case_consistency', 'future_date', 'date_range', 'no_special_chars', 'email_format', 'regex_pattern', 'positive_value']
    selected_tests = []
    custom_test_params = {}
    st.subheader("Select Tests to Run")
    # Get the data type of the first selected column to filter applicable tests
    if selected_columns:
        selected_col_name = selected_columns[0]
        selected_col_info = next((col for col in columns if col[0] == selected_col_name), None)
        data_type = selected_col_info[1].lower() if selected_col_info else ''
        print("veri tipi", data_type)
        # Filter applicable tests
        available_tests = {
            key: val for key, val in get_available_tests(selected_col_info).items()
            if val['available_for'] == 'all' or data_type in val['available_for']
        }

        st.markdown("**Test Descriptions:**")
        for key, val in available_tests.items():
            st.markdown(f"- **{val['name']}**: {val['description']}")
        st.markdown("---")

        for key, val in available_tests.items():
            if st.checkbox(val['name'], key=f"{key}_checkbox"):
                selected_tests.append(key)
    else:
        st.info("Please select at least one column to view available tests.")

    if 'range_check' in selected_tests:
        st.markdown("**Range Check Settings:**")
        custom_test_params['range_check_min'] = st.number_input("Minimum acceptable value", value=0.0)
        custom_test_params['range_check_max'] = st.number_input("Maximum acceptable value", value=100.0)

    if 'length_check' in selected_tests:
        st.markdown("**Length Check Settings:**")
        custom_test_params['length_check_min'] = st.number_input("Minimum acceptable length", value=0)
        custom_test_params['length_check_max'] = st.number_input("Maximum acceptable length", value=100)

    if 'datetime_check' in selected_tests:
        st.markdown("**Datetime Check Settings:**")

        # Format options and their corresponding regex patterns
        format_options = {
            'YYYY-MM-DD HH24:MI:SS': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            'YYYY-MM-DD': r'^\d{4}-\d{2}-\d{2}$',
            'DD/MM/YYYY': r'^\d{2}/\d{2}/\d{4}$',
            'MM-DD-YYYY': r'^\d{2}-\d{2}-\d{4}$',
            'YYYY.MM.DD': r'^\d{4}\.\d{2}\.\d{2}$',
            'HH24:MI:SS': r'^\d{2}:\d{2}:\d{2}$',
            'YYYY-MM-DD HH24:MI:SS.FF3': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$',
            'YYYY-MM-DD HH24:MI:SS.FF': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{2}$',
            'YYYY-MM-DD HH24:MI:SS.FF6': r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$'
        }

        # User selects from dropdown
        selected_format = st.selectbox(
            "Choose expected datetime format:",
            options=list(format_options.keys()),
            index=0  # default to first format
        )

        # Store Oracle format string for display or metadata
        custom_test_params['datetime_check'] = selected_format

        # Get corresponding regex string for backend use
        custom_test_params['datetime_check_regex'] = format_options[selected_format]


    if 'allowed_values' in selected_tests:
        custom_test_params['allowed_values_str'] = st.text_input("Allowed values (comma separated):", value='')

    if 'case_consistency' in selected_tests:
        st.markdown("**Case Consistency Check Settings:**")
        custom_test_params['case_consistency'] = st.selectbox("Expected case:", ['upper', 'lower'], index=0)

    if 'future_date' in selected_tests:
        st.markdown("**Future Date Check Settings:**")
        custom_test_params['future_date'] = st.selectbox("Future date check:", ['future', 'not_future'], index=0)

    if 'date_range' in selected_tests:
        st.markdown("**Date Range Check Settings:**")
        custom_test_params['start_date'] = st.date_input("Start date", value=datetime.now() - timedelta(days=30))
        custom_test_params['end_date'] = st.date_input("End date", value=datetime.now())

    if 'no_special_chars' in selected_tests:
        st.markdown("**No Special Characters Check Settings:**")
        custom_test_params['allowed_pattern'] = st.text_input("Allowed pattern (e.g. '^[a-zA-Z0-9]+$'):", value='^[a-zA-Z0-9]+$')


    if 'regex_pattern' in selected_tests:
        st.markdown("**Regex Pattern Check Settings:**")
        custom_test_params['regex_pattern'] = st.text_input("Regex pattern:", value='')

    if 'positive_value' in selected_tests:
        st.markdown("**Positive Value Check Settings:**")
        custom_test_params['strict'] = st.checkbox("Strict positive value check", value=False)

    if st.button("Run Quality Tests"):
        run_quality_tests(connector, schema, selected_table, selected_columns, selected_tests, custom_test_params=custom_test_params)
