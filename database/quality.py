import streamlit as st
import pandas as pd
import psycopg2
from typing import List, Dict, Any, Optional
import plotly.express as px

class DataQualityTest:
    def __init__(self, name: str, description: str, severity: str = "warning"):
        self.name = name
        self.description = description
        self.severity = severity  # "error", "warning", or "info"
        self.passed = None
        self.message = None

    def run(self, conn, schema: str, table: str) -> bool:
        raise NotImplementedError("Subclasses must implement run()")

class NotNullTest(DataQualityTest):
    def __init__(self, column: str):
        super().__init__(
            name=f"NotNull Test: {column}",
            description=f"Column {column} should not contain NULL values",
            severity="error"
        )
        self.column = column

    def run(self, conn, schema: str, table: str) -> bool:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM "{schema}"."{table}" 
                WHERE "{self.column}" IS NULL
            """)
            null_count = cursor.fetchone()[0]
            self.passed = null_count == 0
            self.message = f"Found {null_count} NULL values" if not self.passed else "No NULL values found"
            return self.passed

class RowCountRangeTest(DataQualityTest):
    def __init__(self, min_count: int, max_count: int):
        super().__init__(
            name="Row Count Range Test",
            description=f"Table should have between {min_count} and {max_count} rows",
            severity="warning"
        )
        self.min_count = min_count
        self.max_count = max_count

    def run(self, conn, schema: str, table: str) -> bool:
        with conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row_count = cursor.fetchone()[0]
            self.passed = self.min_count <= row_count <= self.max_count
            self.message = f"Row count {row_count} is outside the expected range [{self.min_count}, {self.max_count}]"
            return self.passed

class UniqueValueTest(DataQualityTest):
    def __init__(self, column: str):
        super().__init__(
            name=f"Unique Values Test: {column}",
            description=f"Column {column} should contain unique values",
            severity="error"
        )
        self.column = column

    def run(self, conn, schema: str, table: str) -> bool:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT "{self.column}")
                FROM "{schema}"."{table}"
                WHERE "{self.column}" IS NOT NULL
            """)
            duplicate_count = cursor.fetchone()[0]
            self.passed = duplicate_count == 0
            self.message = f"Found {duplicate_count} duplicate values" if not self.passed else "All values are unique"
            return self.passed

class ValueRangeTest(DataQualityTest):
    def __init__(self, column: str, min_value: float, max_value: float):
        super().__init__(
            name=f"Value Range Test: {column}",
            description=f"Column {column} values should be between {min_value} and {max_value}",
            severity="warning"
        )
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def run(self, conn, schema: str, table: str) -> bool:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM "{schema}"."{table}"
                WHERE "{self.column}" < {self.min_value} OR "{self.column}" > {self.max_value}
            """)
            out_of_range_count = cursor.fetchone()[0]
            self.passed = out_of_range_count == 0
            self.message = f"Found {out_of_range_count} values outside the range [{self.min_value}, {self.max_value}]"
            return self.passed

def run_quality_tests(conn, schema: str, table: str, tests: List[DataQualityTest]) -> pd.DataFrame:
    results = []
    for test in tests:
        test.run(conn, schema, table)
        results.append({
            "Test Name": test.name,
            "Description": test.description,
            "Severity": test.severity,
            "Status": "✅ Passed" if test.passed else "❌ Failed",
            "Message": test.message
        })
    return pd.DataFrame(results)

def show_quality_tests_page(conn, schema: str):
    st.title("Data Quality Tests")
    
    # Get all tables
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        tables = [row[0] for row in cursor.fetchall()]

    # Table selection
    selected_table = st.selectbox("Select Table", tables)
    
    if selected_table:
        st.subheader(f"Quality Tests for {schema}.{selected_table}")
        
        # Get columns for the selected table
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, selected_table))
            columns = cursor.fetchall()

        # Test configuration
        st.subheader("Configure Tests")
        
        tests = []
        
        # Not Null Tests
        st.write("### Not Null Tests")
        not_null_cols = st.multiselect(
            "Select columns that should not be NULL",
            [col[0] for col in columns]
        )
        tests.extend([NotNullTest(col) for col in not_null_cols])

        # Row Count Range Test
        st.write("### Row Count Range Test")
        col1, col2 = st.columns(2)
        with col1:
            min_rows = st.number_input("Minimum Row Count", min_value=0, value=0)
        with col2:
            max_rows = st.number_input("Maximum Row Count", min_value=min_rows, value=1000000)
        if st.button("Add Row Count Test"):
            tests.append(RowCountRangeTest(min_rows, max_rows))

        # Unique Value Tests
        st.write("### Unique Value Tests")
        unique_cols = st.multiselect(
            "Select columns that should have unique values",
            [col[0] for col in columns]
        )
        tests.extend([UniqueValueTest(col) for col in unique_cols])

        # Value Range Tests
        st.write("### Value Range Tests")
        numeric_cols = [col[0] for col in columns if col[1] in ('integer', 'numeric', 'real', 'double precision', 'bigint')]
        if numeric_cols:
            selected_col = st.selectbox("Select numeric column for range test", numeric_cols)
            col1, col2 = st.columns(2)
            with col1:
                min_val = st.number_input("Minimum Value", value=0.0)
            with col2:
                max_val = st.number_input("Maximum Value", value=100.0)
            if st.button("Add Value Range Test"):
                tests.append(ValueRangeTest(selected_col, min_val, max_val))

        # Run tests
        if tests:
            if st.button("Run Quality Tests"):
                results_df = run_quality_tests(conn, schema, selected_table, tests)
                
                # Display results
                st.subheader("Test Results")
                
                # Summary metrics
                total_tests = len(results_df)
                passed_tests = len(results_df[results_df["Status"] == "✅ Passed"])
                failed_tests = total_tests - passed_tests
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Tests", total_tests)
                col2.metric("Passed", passed_tests)
                col3.metric("Failed", failed_tests)
                
                # Results table
                st.dataframe(results_df)
                
                # Visualize results
                fig = px.pie(
                    results_df,
                    names="Status",
                    title="Test Results Distribution",
                    color="Status",
                    color_discrete_map={"✅ Passed": "green", "❌ Failed": "red"}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Export results
                if st.button("Export Results"):
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        "Download Results CSV",
                        csv,
                        "quality_test_results.csv",
                        "text/csv",
                        key='download-csv'
                    )
        else:
            st.info("Please configure at least one test to run.") 