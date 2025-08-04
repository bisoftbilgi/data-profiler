import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from database.utils import load_db_config, check_connection
from collections import Counter

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

def get_column_params(custom_test_params, col_name, param_name, default=None):
    """Safely get column-specific parameters"""
    col_params = custom_test_params.get(col_name, {})
    return col_params.get(param_name, default)

def date_format_to_regex(format_str):
    mapping = {
        'YYYY': r'\d{4}',
        'YY': r'\d{2}',
        'MM': r'(0[1-9]|1[0-2])',
        'M': r'([1-9]|1[0-2])',
        'DD': r'(0[1-9]|[12][0-9]|3[01])',
        'D': r'([1-9]|[12][0-9]|3[01])'
    }

    # Uzun anahtarları önce işlemek için sırala
    for key in sorted(mapping.keys(), key=len, reverse=True):
        format_str = format_str.replace(key, mapping[key])

    return f'^{format_str}$'


def get_available_tests(column_info):
    data_type = column_info[1].lower()
    return {
        'null_check': {
            'name': 'Column Values to be Not Null',
            'description': 'Check for null values, test passes if no NULL values are found.',
            'available_for': 'all'
        },
        'distinct_check': {
            'name': 'Column Values to be All Distinct',
            'description': 'Checks if all values in the column are unique. Passes if distinct count equals total rows.',
            'available_for': 'all'
        },
        'range_check': {
            'name': 'Min-Max Range Check',
            'description': 'Checks if numeric column values are within user-defined min and max limit. Passes if all values are in range.',
            'available_for': ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'real', 'number','double precision']
        },
        'length_check': {
            'name': 'String Length Check',
            'description': 'Checks if all string lengths in the column are within user-defined min and max limits. Passes if all values fit the length range.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
    
        'letter_check': {
            'name': 'Letter Not to be Present',
            'description': 'Checks that no alphabetic characters (A–Z, a–z) appear in the column. Passes if no letters are found.',
            'available_for': 'all'
        },
        'number_check': {   
            'name': 'Number Not to be Present',
            'description': 'Checks if the column contains any numeric characters (0-9). Fails if any digits are found.',
            'available_for': 'all'
        },
        'allowed_values': {
            'name': 'Value must be in allowed list',
            'description': 'Verifies that all column values are within a user-defined list of allowed values. Passes if no disallowed values are found.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
        'eng_numeric_format': {
            'name': 'ENG Numeric Format',
            'description': 'Check if numeric values use dot (.) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'real', 'number','double precision']
        },
        'tr_numeric_format': {
            'name': 'TR Numeric Format',
            'description': 'Check if numeric values use comma (,) as decimal separator',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'real', 'number','double precision']
        },
        'case_consistency': {
            'name': 'Case Consistency Check',
            'description': 'Check if all strings follow same casing (upper/lower)',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
        'future_date': {
            'name': 'Future Date Check',
            'description': 'Ensure dates are not in the future',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)']
        },
        'date_range': {
            'name': 'Date Range Check',
            'description': 'Checks whether all date values in a column fall within a specified start–enddate range. If all values are within the range, the test passes.',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)', 'timestamp(6)(11)']
        },
        'no_special_chars': {
            'name': 'No Special Characters',
            'description': "Ensure values don't contain unwanted symbols",
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
        'email_format': {
            'name': 'Email Format Check',
            'description': 'Checks whether values in a column follow valid email format. If all non-null values match the email regex, the test passes.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
        'regex_pattern': {
            'name': 'Regex Pattern Match',
            'description': 'Validates column values against a user-provided regex.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
            'positive_value': {
            'name': 'Positive Value Check',
            'description': 'Checks whether all non-null values in the column are positive.'
                           '- If `strict=True`, only values > 0 are accepted.'
                           '- If `strict=False`, values >= 0 are accepted.',
            'available_for': ['decimal', 'numeric', 'float', 'double', 'int', 'bigint', 'smallint', 'tinyint', 'real', 'number', 'double precision']
        },
        'tckn_check': {
            'name': 'TCKN Check',
            'description': 'Checks for invalid Turkish Identification Numbers (TCKN) in a column.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2', 'character varying']
        },
        'date_check': {
            'name': 'Date Format Distibution Check',
            'description': 'Validates date formats in a text column by parsing each value.'
                           '- Counts invalid date entries; passes if none found.'
                           '- Stores invalid rows for review.'
                           '- Displays distribution of detected date formats.',
            'available_for': ['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        },
        'date_logic_check': {
            'name': 'Date Logic Check',
            'description': 'Check if Start Date is before than End Date',
            'available_for': ['date', 'datetime', 'timestamp', 'timestamp(6)', 'timestamp(6)(11)']
                },

        'date_format_check':{
            'name': 'Date Format Check',
            'description':'Check if input date format is compatible with column values',
            'available_for':['varchar', 'char', 'nvarchar', 'nchar', 'text','varchar2','character varying']
        }
    }

def create_schema_for_column(column_info, selected_tests, custom_test_params=None):
    return None

def run_quality_tests(connector, schema: str, table: str, column_test_map, custom_test_params=None):

    st.subheader("Running Data Quality Checks")
    columns = get_cached_columns(connector, schema, table)
    selected_columns_info = [col for col in columns if col[0] in column_test_map.keys()]

    table_analysis = get_cached_table_analysis(connector, schema, table)
    total_rows = table_analysis.get('row_count', 0)
    violated_rows_by_column = {}

    metrics = []
    for col in selected_columns_info:
        col_name, data_type = col[0], col[1].lower()
        tests_for_column = column_test_map.get(col_name, [])
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
        tckn_check_violation_count = None
        tckn_check_pass = None
        debug_mode = True
        date_violation_count = None
        date_check_pass = None
        date_logic_violation_count = None
        date_logic_check_pass = None
        date_format_violation_count=None
        date_format_pass = None




        try:
            if 'null_check' in tests_for_column:
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
            if 'distinct_check' in tests_for_column:
                distinct_count = connector.get_distinct_count(schema, table, col_name) 
                if distinct_count==total_rows:
                    distinct_pass = PASS_ICON
                else:
                    violated_rows_by_column[(col_name, 'distinct_check')] = connector.get_non_distinct_violations(schema, table, col_name)
                    distinct_pass = FAIL_ICON
        except:
            distinct_count = None

        try:
            if 'range_check' in tests_for_column and data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'real','number']:
                range_stats = connector.get_min_max_range(schema, table, col_name)
                user_min = get_column_params(custom_test_params, col_name, 'range_check_min')
                user_max = get_column_params(custom_test_params, col_name, 'range_check_max')
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
            if 'length_check' in tests_for_column:
                length_stats = connector.get_char_length_range(schema, table, col_name)
                user_min = get_column_params(custom_test_params, col_name, 'length_check_min')
                user_max = get_column_params(custom_test_params, col_name, 'length_check_max')
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
            if 'letter_check' in tests_for_column:
                
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
            if 'number_check' in tests_for_column:
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
            if 'allowed_values' in tests_for_column:
                allowed_values_str = get_column_params(custom_test_params, col_name, 'allowed_values_str')
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
            if 'eng_numeric_format' in tests_for_column:
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
            if 'tr_numeric_format' in tests_for_column:
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
            if 'case_consistency' in tests_for_column:
                
                case_consistency = get_column_params(custom_test_params, col_name, 'case_consistency')
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
            if 'future_date' in tests_for_column:
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
            if 'date_range' in tests_for_column:
                start_date = get_column_params(custom_test_params, col_name, 'start_date')
                end_date = get_column_params(custom_test_params, col_name, 'end_date')
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
            if 'no_special_chars' in tests_for_column:
                allowed_pattern = get_column_params(custom_test_params, col_name, 'allowed_pattern')
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
            if 'email_format' in tests_for_column:
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
            if 'regex_pattern' in tests_for_column:

                regex_pattern = get_column_params(custom_test_params, col_name, 'regex_pattern')
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
            if 'positive_value' in tests_for_column:

                strict = get_column_params(custom_test_params, col_name, 'strict')
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
        # In your validation mechanism
        try:
            if 'tckn_check' in tests_for_column:
                # First get the violation count
                tckn_check_violation_count = connector.get_tckn_violation_count(schema, table, col_name)
                tckn_check_pass = None
                # Determine pass/fail status
                tckn_check_pass = PASS_ICON 
                
                if tckn_check_violation_count == 0:
                    tckn_check_pass = PASS_ICON
                        # Get some valid samples for debugging
                else:
                    # Get sample violations for display
                    tckn_check_pass = FAIL_ICON 
                    violated_rows_by_column[(col_name, 'tckn_check')] = connector.get_tckn_violations(
                        schema, table, col_name, 100
                    )
                

        except Exception as e:
            tckn_check_violation_count = None
            tckn_check_pass = f"{FAIL_ICON} ({str(e)})"
            st.error(f"TCKN validation error: {str(e)}")

        try:
            if 'date_check' in tests_for_column:
                # Get all parsed results (each row contains: raw value, format, is_valid, parsed_date)
                parsed_rows = connector.get_text_column_date_formats(schema, table, col_name)
    
                # Count how many rows are invalid
                date_violation_count = sum(1 for row in parsed_rows if not row['is_valid'])
                print(f"[DEBUG] Date violation count: {date_violation_count}")

                if date_violation_count == 0:
                    date_check_pass = PASS_ICON
                    print(f"[DEBUG] Date check pass: {date_check_pass}")
                                        # Store failed rows in debug map

                else:
                    date_check_pass = FAIL_ICON
                    print(f"[DEBUG] Date check pass: {date_check_pass}")

                    # Store failed rows in debug map
                    violated_rows_by_column[(col_name, 'date_check')] = [
                        row for row in parsed_rows if not row['is_valid']
                    ]

                format_counts = Counter(row['format'] for row in parsed_rows)
                format_df = pd.DataFrame(format_counts.items(), columns=['Format', 'Count']).sort_values(by='Count', ascending=False)
                st.markdown(f"**Date Format Distribution for `{col_name}`**")
                st.dataframe(format_df, use_container_width=True)

        except Exception as e:
            date_violation_count = None
            date_check_pass = f"{FAIL_ICON} ({str(e)})"
            st.write(f"[DEBUG] throwing exception Date check pass: {date_check_pass}")


        try:
            if 'date_logic_check' in tests_for_column:
                start_date_logic = get_column_params(custom_test_params, col_name, 'start_date_logic')
                end_date_logic = get_column_params(custom_test_params, col_name, 'end_date_logic')
                date_logic_violation_count = connector.get_date_logic_violation_count(schema, table, start_date_logic, end_date_logic)
                print(f"[DEBUG] Date violation count: {date_logic_violation_count}")
                
                if date_logic_violation_count == 0:
                    date_logic_check_pass = PASS_ICON
                    print(f"[DEBUG] Date check pass: {date_logic_check_pass}")
                else:
                    
                    violated_rows_by_column[(col_name, 'date_check')] = connector.get_date_logic_violations(
                        schema, table, start_date_logic, end_date_logic
                    )
                    date_logic_check_pass = FAIL_ICON
                    print(f"[DEBUG] Date check pass: {date_logic_check_pass}")

        except Exception as e:
            date_logic_violation_count = None
            date_logic_check_pass = f"{FAIL_ICON} ({str(e)})"
            st.write(f"[DEBUG] throwing exception Date logic pass: {date_logic_check_pass}")


        try:
            if 'date_format_check' in tests_for_column:
                date_format_input = get_column_params(custom_test_params, col_name, 'date_format_input')
                date_format_regex = date_format_to_regex(date_format_input)
                print(date_format_regex)
                date_format_violation_count = connector.get_date_format_violation_count(schema, table, col_name,date_format_regex, limit=100)
                print(date_format_violation_count)
                print(f"[DEBUG] Date format count: {date_format_violation_count}")

                if date_format_violation_count == 0:
                    date_format_pass = PASS_ICON
                    print(f"[DEBUG] Date format pass: {date_format_violation_count}")
                else:

                    violated_rows_by_column[(col_name, 'date_format_check')] = connector.get_date_format_violations(
                        schema, table, col_name,  date_format_regex
                    )
                    date_format_pass = FAIL_ICON
                    print(f"[DEBUG] Date format pass: {date_logic_check_pass}")
                    
        except Exception as e:
            date_format_violation_count = None
            date_format_pass = f"{FAIL_ICON} ({str(e)})"
            st.write(f"[DEBUG] throwing exception Date format pass: {date_logic_check_pass}")

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
            'Letter Violation %': (letter_count / total_rows * 100) if total_rows and letter_count is not None else None,
            'Number Count': number_count,
            'Number Pass': number_pass,
            'Number Violation %': (number_count / total_rows * 100) if total_rows and number_count is not None else None,
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
            'Positive Value Pass': positive_value_pass,
            'TCKN Check Violation Count': tckn_check_violation_count,
            'TCKN Check Pass': tckn_check_pass,
            'Date Check Violation Count': date_violation_count,
            'Date Check Pass': date_check_pass,
            'Date Logic Violation Count': date_logic_violation_count,
            'Date Logic Check Pass': date_logic_check_pass,
            'ENG Numeric Format Violation %': (eng_numeric_format_violation_count / total_rows * 100) if total_rows and eng_numeric_format_violation_count is not None else None,
            'TR Numeric Format Violation %': (tr_numeric_format_violation_count / total_rows * 100) if total_rows and tr_numeric_format_violation_count is not None else None,
            'Case Inconsistency %': (case_inconsistency_count / total_rows * 100) if total_rows and case_inconsistency_count is not None else None,
            'Future Date Violation %': (future_date_violation_count / total_rows * 100) if total_rows and future_date_violation_count is not None else None,
            'Date Range Violation %': (date_range_violation_count / total_rows * 100) if total_rows and date_range_violation_count is not None else None,
            'Special Char Violation %': (special_char_violation_count / total_rows * 100) if total_rows and special_char_violation_count is not None else None,
            'Email Format Violation %': (email_format_violation_count / total_rows * 100) if total_rows and email_format_violation_count is not None else None,
            'Regex Pattern Violation %': (regex_pattern_violation_count / total_rows * 100) if total_rows and regex_pattern_violation_count is not None else None,
            'Positive Value Violation %': (positive_value_violation_count / total_rows * 100) if total_rows and positive_value_violation_count is not None else None,
            'TCKN Check Violation %': (tckn_check_violation_count / total_rows * 100) if total_rows and tckn_check_violation_count is not None else None,
            'Date Check Violation %': (date_violation_count / total_rows * 100) if total_rows and date_violation_count is not None else None,
            'Date Logic Violation %': (date_logic_violation_count / total_rows * 100) if total_rows and date_logic_violation_count is not None else None,
            'Date Format Violation Count': (date_format_violation_count),
            'Date Format Pass': date_format_pass,
            'Date Format Violation %':(date_format_violation_count/total_rows*100) if total_rows and date_format_violation_count is not None else None
        })

    df = pd.DataFrame(metrics)
    st.subheader("Validation Summary")


    per_field_dfs = {}
    for col_name in df['Column']:
        # Seçili testler (ör: ['null_check', 'distinct_check', ...])
        selected_tests = column_test_map.get(col_name, [])

        # Her zaman dahil etmek istediğin kolonlar:
        always_show = ['Column', 'Data Type']

        # Get all unique tests selected across all columns
        all_selected_tests = set()
        for tests in column_test_map.values():
            all_selected_tests.update(tests)

        custom_cols = []
        if 'null_check' in selected_tests:
            custom_cols += ['Null Count', 'Null %', 'Null Pass']
        if 'distinct_check' in selected_tests:
            custom_cols += ['Distinct Count', 'Distinct %', 'Distinct Pass']
        if 'range_check' in selected_tests:
            custom_cols += ['Min', 'Max', 'Range', 'Range Pass']
        if 'length_check' in selected_tests:
            custom_cols += ['Min Length', 'Max Length', 'Length Pass']
        if 'datetime_check' in selected_tests:
            custom_cols += ['Invalid Datetime Count', 'Datetime Pass']
        if 'letter_check' in selected_tests:
            custom_cols += ['Letter Count', 'Letter Pass', 'Letter Violation %']
        if 'number_check' in selected_tests:
            custom_cols += ['Number Count', 'Number Pass', 'Number Violation %']
        if 'allowed_values' in selected_tests:
            custom_cols += ['Allowed Values Violation Count', 'Allowed Values Violation %',
                            'Allowed Values Non Violation Count', 'Allowed Values Pass']
        if 'eng_numeric_format' in selected_tests:
            custom_cols += ['ENG Numeric Format Violation Count', 'ENG Numeric Format Violation %',
                            'ENG Numeric Format Pass']
        if 'tr_numeric_format' in selected_tests:
            custom_cols += ['TR Numeric Format Violation Count', 'TR Numeric Format Violation %', 'TR Numeric Format Pass']
        if 'case_consistency' in selected_tests:
            custom_cols += ['Case Inconsistency Count', 'Case Inconsistency %', 'Case Inconsistency Pass']
        if 'future_date' in selected_tests:
            custom_cols += ['Future Date Violation Count', 'Future Date Violation %', 'Future Date Pass']
        if 'date_range' in selected_tests:
            custom_cols += ['Date Range Violation Count', 'Date Range Violation %', 'Date Range Pass']
        if 'no_special_chars' in selected_tests:
            custom_cols += ['Special Char Violation Count', 'Special Char Violation %', 'Special Char Pass']
        if 'email_format' in selected_tests:
            custom_cols += ['Email Format Violation Count', 'Email Format Violation %', 'Email Format Pass']
        if 'regex_pattern' in selected_tests:
            custom_cols += ['Regex Pattern Violation Count', 'Regex Pattern Violation %', 'Regex Pattern Pass']
        if 'positive_value' in selected_tests:
            custom_cols += ['Positive Value Violation Count', 'Positive Value Violation %', 'Positive Value Pass']
        if 'tckn_check' in selected_tests:
            custom_cols += ['TCKN Check Violation Count', 'TCKN Check Violation %', 'TCKN Check Pass']
        if 'date_check' in selected_tests:
            custom_cols += ['Date Check Violation Count', 'Date Check Violation %', 'Date Check Pass']
        if 'date_logic_check' in selected_tests:
            custom_cols += ['Date Logic Violation Count', 'Date Logic Violation %', 'Date Logic Check Pass']
        if 'date_format_check' in selected_tests:
            custom_cols += ['Date Format Violation Count', 'Date Format Pass', 'Date Format Violation %']


        # Sadece o kolona ait satırı al ve ilgili kolonları seç
        field_df = df[df['Column'] == col_name][always_show + custom_cols]
        per_field_dfs[col_name] = field_df

        # Streamlit'te göstermek için
        st.subheader(f"{col_name}")
        st.dataframe(field_df)

    if violated_rows_by_column:
        st.subheader("Violated Rows Preview")

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

    all_tests = ['null_check', 'distinct_check', 'range_check', 'length_check', 'datetime_check', 
             'letter_check', 'number_check', 'allowed_values', 'eng_numeric_format', 'tr_numeric_format', 
             'case_consistency', 'future_date', 'date_range', 'no_special_chars', 'email_format', 
             'regex_pattern', 'positive_value', 'tckn_check', 'date_check', 'date_logic_check', 'date_format_check']

    column_test_map = {}  # Dict to store selected tests per column
    custom_test_params = {}

    st.subheader("Select Tests to Run for Each Column")

    if selected_columns:
        for col_name in selected_columns:
            custom_test_params[col_name] = {}
            st.markdown(f"### 🧪 Tests for Column: `{col_name}`")

            selected_col_info = next((col for col in columns if col[0] == col_name), None)
            data_type = selected_col_info[1].lower() if selected_col_info else ''

            # Get applicable tests for the column
            available_tests = {
                key: val for key, val in get_available_tests(selected_col_info).items()
                if val['available_for'] == 'all' or data_type in val['available_for']
            }

            if not available_tests:
                st.warning(f"No applicable tests found for column `{col_name}`.")
                continue

            # Show descriptions
            with st.expander("Show Test Descriptions", expanded=False):
                for key, val in available_tests.items():
                    st.markdown(f"- **{val['name']}**: {val['description']}")

            # Show checkboxes to select tests
            selected_tests = []
            for key, val in available_tests.items():
                if st.checkbox(val['name'], key=f"{col_name}_{key}_checkbox"):
                    selected_tests.append(key)

            column_test_map[col_name] = selected_tests

            if selected_tests:  # Only show if tests are selected for this column


                if 'range_check' in selected_tests:
                    st.markdown("**Range Check Settings:**")
                    custom_test_params[col_name]['range_check_min'] = st.number_input(
                        "Minimum acceptable value", 
                        value=0.0,
                        key=f"{col_name}_range_min"
                    )
                    custom_test_params[col_name]['range_check_max'] = st.number_input(
                        "Maximum acceptable value", 
                        value=100.0,
                        key=f"{col_name}_range_max"
                    )

                if 'length_check' in selected_tests:
                    st.markdown("**Length Check Settings:**")
                    custom_test_params[col_name]['length_check_min'] = st.number_input(
                        "Minimum acceptable length", 
                        value=0,
                        key=f"{col_name}_length_min"
                    )
                    custom_test_params[col_name]['length_check_max'] = st.number_input(
                        "Maximum acceptable length", 
                        value=100,
                        key=f"{col_name}_length_max"
                    )

                if 'allowed_values' in selected_tests:
                    custom_test_params[col_name]['allowed_values_str'] = st.text_input(
                        "Allowed values (comma separated):", 
                        value='',
                        key=f"{col_name}_allowed_values"
                    )

                if 'case_consistency' in selected_tests:
                    st.markdown("**Case Consistency Check Settings:**")
                    custom_test_params[col_name]['case_consistency'] = st.selectbox(
                        "Expected case:", 
                        ['upper', 'lower'], 
                        index=0,
                        key=f"{col_name}_case_consistency"
                    )


                if 'date_range' in selected_tests:
                    st.markdown("**Date Range Check Settings:**")
                    custom_test_params[col_name]['start_date'] = st.date_input(
                        "Start date", 
                        value=datetime.now() - timedelta(days=30),
                        key=f"{col_name}_start_date"
                    )
                    custom_test_params[col_name]['end_date'] = st.date_input(
                        "End date", 
                        value=datetime.now(),
                        key=f"{col_name}_end_date"
                    )

                if 'no_special_chars' in selected_tests:
                    st.markdown("**No Special Characters Check Settings:**")
                    custom_test_params[col_name]['allowed_pattern'] = st.text_input(
                        "Allowed pattern (e.g. '^[a-zA-Z0-9]+$'):", 
                        value='^[a-zA-Z0-9]+$',
                        key=f"{col_name}_allowed_pattern"
                    )

                if 'regex_pattern' in selected_tests:
                    st.markdown("**Regex Pattern Check Settings:**")
                    custom_test_params[col_name]['regex_pattern'] = st.text_input(
                        "Regex pattern:", 
                        value='',
                        key=f"{col_name}_regex_pattern"
                    )

                if 'positive_value' in selected_tests:
                    st.markdown("**Positive Value Check Settings:**")
                    custom_test_params[col_name]['strict'] = st.checkbox(
                        "Strict positive value check", 
                        value=False,
                        key=f"{col_name}_strict_positive"
                    )

                if 'date_logic_check' in selected_tests:
                    st.markdown("**Date Logic Settings:**")
                    
                    date_columns = [col[0] for col in columns if 'date' in col[1].lower() or 'time' in col[1].lower()]
                    selectable_columns = date_columns if date_columns else [col[0] for col in columns]
                    
                    custom_test_params[col_name]['start_date_logic'] = st.selectbox(
                        "Select Start Date Column", 
                        selectable_columns,
                        key=f"{col_name}_start_date_col"
                    )
                    custom_test_params[col_name]['end_date_logic'] = st.selectbox(
                        "Select End Date Column", 
                        selectable_columns,
                        key=f"{col_name}_end_date_col"
                    )

                if 'date_format_check' in selected_tests:
                    st.markdown("**Date Format Input**")
                    custom_test_params[col_name]['date_format_input'] = st.text_input(
                        "Please enter date format to match",
                        value='',
                        key=f"{col_name}_date_format_input"
                    )
    else:
        st.info("Please select at least one column to view and assign tests.")

    if st.button("Run Quality Tests"):
        run_quality_tests(
            connector=connector,
            schema=schema,
            table=selected_table,
            column_test_map=column_test_map,
            custom_test_params=custom_test_params
        )



