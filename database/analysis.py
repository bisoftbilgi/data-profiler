import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from decimal import Decimal
from database.utils import decimal_to_float
from datetime import datetime



def get_all_tables_and_views(connector, schema):
    """Get all tables and views from the database"""
    return connector.get_all_tables_and_views(schema)

def analyze_table(connector, schema: str, table: str, object_type: str = 'TABLE'):
    """Analyze a specific table or view"""
    try:
        # Get table statistics
        table_stats = connector.get_table_analysis(schema, table)
        
        # Display table statistics
        st.subheader("Table Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Rows", f"{table_stats['row_count']:,}")
            st.metric("Total Size", f"{table_stats['total_size']:.2f} MB")
        
        with col2:
            st.metric("Table Size", f"{table_stats['table_size']:.2f} MB")
            st.metric("Index Size", f"{table_stats['index_size']:.2f} MB")
        
        with col3:
            st.metric("Avg Row Width", f"{table_stats['avg_row_width']:,} bytes")
            last_analyzed = table_stats['last_analyzed']
            if isinstance(last_analyzed, datetime):
                last_analyzed = last_analyzed.strftime('%Y-%m-%d %H:%M:%S')
            st.metric("Last Analyzed", last_analyzed or 'Never')
        
        # Get sample data
        sample_data = connector.get_sample_data(schema, table)
        if sample_data:
            st.subheader("Sample Data")
            # Get column names
            try:
                col_names = [col[0] for col in connector.get_columns(schema, table)]
            except Exception:
                col_names = None
            df_sample = pd.DataFrame(sample_data, columns=col_names if col_names else None)
            # Convert datetime columns to strings
            for col in df_sample.columns:
                if df_sample[col].dtype == 'datetime64[ns]':
                    df_sample[col] = df_sample[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            st.dataframe(df_sample)
        
        # Get column statistics
        st.subheader("Column Statistics")
        columns = connector.get_columns(schema, table)
        
        # Calculate average column widths
        column_widths = {}
        for col in columns:
            col_name = col[0]
            data_type = col[1].lower()
            max_length = col[3]
            precision = col[4]
            scale = col[5]
            
            # Format data type with constraints
            formatted_type = data_type
            if max_length and max_length > 0:
                formatted_type += f"({max_length})"
            elif precision and scale:
                formatted_type += f"({precision},{scale})"
            elif precision:
                formatted_type += f"({precision})"
            
            col_details = connector.get_column_details(schema, table, col_name)
            
            if col_details and 'metrics' in col_details:
                metrics = col_details['metrics']
                
                if data_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext', 'nvarchar', 'nchar', 'ntext']:
                    # For text columns, use average length
                    column_widths[col_name] = metrics.get('avg_length', 0)
                
                elif data_type in ['int', 'bigint', 'smallint', 'tinyint']:
                    # For integer types, use fixed sizes
                    type_sizes = {
                        'tinyint': 1,
                        'smallint': 2,
                        'int': 4,
                        'bigint': 8
                    }
                    column_widths[col_name] = type_sizes.get(data_type, 4)
                
                elif data_type in ['decimal', 'numeric']:
                    # For decimal types, calculate based on precision and scale
                    precision = metrics.get('precision', 0)
                    scale = metrics.get('scale', 0)
                    # Each digit takes 4 bits, plus 1 byte for sign
                    column_widths[col_name] = (precision * 4 + 8) // 8
                
                elif data_type in ['float', 'double']:
                    # For floating point types, use fixed sizes
                    type_sizes = {
                        'float': 4,
                        'double': 8
                    }
                    column_widths[col_name] = type_sizes.get(data_type, 8)
                
                elif data_type in ['date']:
                    column_widths[col_name] = 3  # 3 bytes for date
                
                elif data_type in ['datetime', 'timestamp']:
                    column_widths[col_name] = 8  # 8 bytes for datetime
                
                else:
                    # For other types, use max length if available
                    column_widths[col_name] = metrics.get('max_length', 0)
        
        # Calculate total width
        total_width = sum(column_widths.values())
        
        for col in columns:
            col_name = col[0]
            data_type = col[1].lower()
            max_length = col[3]
            precision = col[4]
            scale = col[5]
            
            # Format data type with constraints
            formatted_type = data_type
            if max_length and max_length > 0:
                formatted_type += f"({max_length})"
            elif precision and scale:
                formatted_type += f"({precision},{scale})"
            elif precision:
                formatted_type += f"({precision})"
            
            st.write(f"### {col_name}")
            
            # Create tabs for each column
            stat_tab, viz_tab = st.tabs(["Statistics", "Visualizations"])
            
            with stat_tab:
                # Get column details
                col_details = connector.get_column_details(schema, table, col_name)
                if not col_details:
                    st.warning(f"Could not get details for column {col_name}")
                    continue
                
                # Display basic statistics
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Data Type", formatted_type)
                    st.metric("Distinct Values", f"{col_details['distinct_count']:,}")
                    st.metric("Unique Values", f"{col_details.get('unique_count', 0):,}")
                with col2:
                    st.metric("Null Values", f"{col_details['null_count']:,}")
                    st.metric("Null Percentage", f"{(col_details['null_count'] / table_stats['row_count'] * 100):.2f}%")
                
                # Display column width information
                col_width = column_widths.get(col_name, 0)
                width_percentage = (col_width / total_width * 100) if total_width > 0 else 0
                
                width_col1, width_col2 = st.columns(2)
                with width_col1:
                    st.metric("Avg Column Width", f"{col_width:.1f} bytes")
                with width_col2:
                    st.metric("Width Ratio", f"{width_percentage:.1f}%")
                
                # Display type-specific metrics
                metrics = col_details['metrics']
                if metrics:
                    st.write("#### Type-specific Metrics")
                    metric_cols = st.columns(len(metrics))
                    for i, (metric_name, value) in enumerate(metrics.items()):
                        if isinstance(value, datetime):
                            value = value.strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(value, float):
                            value = f"{value:.2f}"
                        with metric_cols[i]:
                            st.metric(metric_name.replace('_', ' ').title(), str(value))
            
            with viz_tab:
                # Get column details for visualizations
                col_details = connector.get_column_details(schema, table, col_name)
                if not col_details:
                    st.warning(f"Could not get details for column {col_name}")
                    continue
                
                # Add visualizations based on data type
                data_type = col_details['data_type'].lower()
                
                if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double']:
                    # Get value distribution for numeric columns
                    value_counts = connector.get_value_counts(schema, table, col_name)
                    if value_counts:
                        df_counts = pd.DataFrame(value_counts, columns=['value', 'count'])
                        df_counts['value'] = pd.to_numeric(df_counts['value'])
                        
                    # Height-balanced histogram (quantile-based)
                    # Fetch all values for the column (sample up to 10000 for performance)
                    try:
                        dbtype = 'mysql'
                        if 'postgres' in connector.__class__.__name__.lower():
                            dbtype = 'postgresql'
                        elif 'mssql' in connector.__class__.__name__.lower():
                            dbtype = 'mssql'
                        elif 'oracle' in connector.__class__.__name__.lower():
                            dbtype = 'oracle'
                        quoted_col = sql_quote_identifier(col_name, dbtype)
                        quoted_table = sql_quote_table(schema, table, dbtype)
                        limit_clause = 'LIMIT 10000' if dbtype in ['mysql', 'postgresql'] else ('TOP 10000' if dbtype == 'mssql' else '')
                        if dbtype == 'mssql':
                            query = f"SELECT TOP 10000 {quoted_col} FROM {quoted_table} WHERE {quoted_col} IS NOT NULL"
                        else:
                            query = f"SELECT {quoted_col} FROM {quoted_table} WHERE {quoted_col} IS NOT NULL {limit_clause}"
                        df_col = pd.read_sql(query, connector.connection)
                        if not df_col.empty:
                            bin_edges, counts = height_balanced_histogram(df_col[col_name], n_buckets=10)
                            bin_labels = [f"{bin_edges[i]:.2f} - {bin_edges[i+1]:.2f}" for i in range(len(bin_edges)-1)]
                            fig = px.bar(x=bin_labels, y=counts, labels={'x': 'Value Range', 'y': 'Count'},
                                         title=f"Height-Balanced Histogram for {col_name}")
                            st.plotly_chart(fig)
                    except Exception as e:
                        st.info(f"Could not plot height-balanced histogram: {e}")
                        
                        # Create box plot
                        fig = px.box(df_counts, y='value',
                                    title=f"Box Plot for {col_name}")
                        st.plotly_chart(fig)
                    

                
                elif data_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext', 'nvarchar', 'nchar', 'ntext']:
                    # Get value counts for text columns

                    
                    value_counts = connector.get_value_counts(schema, table, col_name)
                    if value_counts:
                        df_counts = pd.DataFrame(value_counts, columns=['value', 'count'])
                        
                                            # Sort and split
                        df_counts_sorted = df_counts.sort_values('count', ascending=False)
                        top9_df = df_counts_sorted.head(9)
                        others_count = df_counts_sorted['count'][9:].sum()

                    # Append 'Others' if there are more than 9 unique values
                    if others_count > 0:
                        top9_df = pd.concat([top9_df, pd.DataFrame([{'value': 'Diğer', 'count': others_count}])], ignore_index=True)

                    else:
                        # Select top 5 values
                        top9_df = df_counts_sorted.head(10)

                        # Create a matrix-like DataFrame for heatmap
                        # This is a workaround to format data as needed for a heatmap
                    heatmap_data = top9_df.pivot_table(index='value', values='count')

                    # Create heatmap
                    fig = px.imshow(
                        heatmap_data,
                        color_continuous_scale='Viridis',
                        labels=dict(x="Frequency", y=col_name, color="Count"),
                        title=f"Top 10 Values Heatmap for {col_name}"
)
                    st.plotly_chart(fig)
            
            st.write("---")
            
    except Exception as e:
        st.error(f"Error analyzing table: {str(e)}")
        st.write("Debug - Error type:", type(e).__name__)
        st.write("Debug - Error details:", str(e))

def col_analysis(connector, schema, table, col_info):
    """Analyze a specific column"""
    col_name = col_info[0]
    data_type = col_info[1]
    
    # Get column details
    col_details = connector.get_column_details(schema, table, col_name)
    metrics = col_details['metrics']
    
    # Display basic statistics
    st.write(f"### Column: {col_name}")
    st.write(f"**Data Type:** {data_type}")
    st.write(f"**Distinct Values:** {col_details['distinct_count']:,}")
    st.write(f"**Unique Values:** {col_details.get('unique_count', 0):,}")
    st.write(f"**Null Values:** {col_details['null_count']:,}")
    
    # Display type-specific metrics
    if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
        st.write("**Numeric Statistics:**")
        min_val = metrics.get('min', 'N/A')
        max_val = metrics.get('max', 'N/A')
        avg_val = metrics.get('avg', 'N/A')
        median_val = metrics.get('median', 'N/A')
        std_dev_val = metrics.get('std_dev', 'N/A')
        min_val = f"{min_val:.2f}"
        max_val = f"{max_val:.2f}"
        avg_val = f"{avg_val:.2f}"
        median_val = f"{median_val:.2f}"
        std_dev_val = f"{std_dev_val:.2f}"
        st.write(f"- Min Value: {min_val}")
        st.write(f"- Max Value: {max_val}")
        st.write(f"- Average: {avg_val}")
        st.write(f"- Median: {median_val}")
        st.write(f"- Standard Deviation: {std_dev_val}")
    elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
        st.write("**Text Statistics:**")
        min_length = metrics.get('min_length', 'N/A')
        max_length = metrics.get('max_length', 'N/A')
        avg_length = metrics.get('avg_length', 'N/A')
        if isinstance(min_length, (float, Decimal)):
            min_length = f"{min_length:.2f}"
        if isinstance(max_length, (float, Decimal)):
            max_length = f"{max_length:.2f}"
        if isinstance(avg_length, (float, Decimal)):
            avg_length = f"{avg_length:.2f}"
        st.write(f"- Min Length: {min_length}")
        st.write(f"- Max Length: {max_length}")
        st.write(f"- Average Length: {avg_length}")
    elif data_type in ['date', 'datetime', 'datetime2', 'smalldatetime']:
        st.write("**Date Statistics:**")
        st.write(f"- Min Date: {metrics.get('min_date', 'N/A')}")
        st.write(f"- Max Date: {metrics.get('max_date', 'N/A')}")

# Helper for height-balanced histogram
def height_balanced_histogram(series, n_buckets=10):
    series = series.dropna()
    if series.nunique() == 1:
        return [series.min(), series.max()], [len(series)]
    buckets, bin_edges = pd.qcut(series, q=n_buckets, retbins=True, duplicates='drop')
    counts = buckets.value_counts(sort=False)
    return bin_edges, counts.values

# Helper for quoting SQL identifiers (column/table names)
def sql_quote_identifier(identifier, dbtype):
    if dbtype == 'mysql':
        return f'`{identifier}`'
    elif dbtype == 'postgresql':
        return f'"{identifier}"'
    elif dbtype == 'mssql':
        return f'[{identifier}]'
    elif dbtype == 'oracle':
        return f'"{identifier.upper()}"'
    else:
        return identifier

# Helper for quoting full table name
def sql_quote_table(schema, table, dbtype):
    if dbtype == 'mysql':
        return f'`{schema}`.`{table}`'
    elif dbtype == 'postgresql':
        return f'"{schema}"."{table}"'
    elif dbtype == 'mssql':
        return f'[{schema}].[{table}]'
    elif dbtype == 'oracle':
        return f'"{schema.upper()}"."{table.upper()}"'
    else:
        return f'{schema}.{table}'
