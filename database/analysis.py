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
        #st.write("DEBUG: table_stats =", table_stats)
        
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
        
        # Get columns
        columns = connector.get_columns(schema, table)
        #st.write("DEBUG: columns =", columns)
        
        # Get sample data
        sample_data = connector.get_sample_data(schema, table)
        #st.write("DEBUG: sample_data[0:3] =", sample_data[:3] if sample_data else "EMPTY")
        
        # Get column names
        col_names = [col[0] for col in columns]
        #st.write("DEBUG: col_names =", col_names)
        
        # DataFrame creation
        if sample_data:
            # Ensure pyodbc.Row is converted to tuple
            if hasattr(sample_data[0], '__class__') and sample_data[0].__class__.__name__ == 'Row':
                sample_data = [tuple(row) for row in sample_data]
            df_sample = pd.DataFrame(sample_data, columns=col_names)
            #st.write("DEBUG: df_sample.head()", df_sample.head())
            #st.dataframe(df_sample)
        
        # Get column statistics
        st.subheader("Column Statistics")
        # Calculate average column widths
        column_widths = {}

        for col in columns:
            col_name = col[0]
            st.subheader(col_name)


            data_type = (col[1] or "").lower()
            max_length = col[3] or 0
            precision = col[4] or 0
            scale = col[5] or 0


            # Format data type with constraints (safe formatting)
            formatted_type = data_type
            if max_length > 0:
                formatted_type += f"({max_length})"
            elif precision > 0 and scale > 0:
                formatted_type += f"({precision},{scale})"
            elif precision > 0:
                formatted_type += f"({precision})"

            col_details = connector.get_column_details(schema, table, col_name)
            metrics = col_details.get('metrics', {}) if col_details else {}

            if data_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext', 'nvarchar', 'nchar', 'ntext']:
                column_widths[col_name] = metrics.get('avg_length') or 0

            elif data_type in ['int', 'bigint', 'smallint', 'tinyint']:
                type_sizes = {'tinyint': 1, 'smallint': 2, 'int': 4, 'bigint': 8}
                column_widths[col_name] = type_sizes.get(data_type, 4)

            elif data_type in ['decimal', 'numeric']:
                p = precision
                s = scale
                # fallback to metric if defined
                p = metrics.get('precision', p) or 0
                s = metrics.get('scale', s) or 0
                column_widths[col_name] = (p * 4 + 8) // 8 if p else 0

            elif data_type in ['float', 'double']:
                column_widths[col_name] = 4 if data_type == 'float' else 8

            elif data_type == 'date':
                column_widths[col_name] = 3

            elif data_type in ['datetime', 'timestamp']:
                column_widths[col_name] = 8

            else:
                column_widths[col_name] = max_length or metrics.get('max_length', 0)

            
            # Create tabs for each column
            stat_tab, viz_tab = st.tabs(["Statistics", "Visualizations"])
            
            with stat_tab:
                # Get column details
                col_details = connector.get_column_details(schema, table, col_name)
                #st.write(f"DEBUG: col_details for {col_name} (stat_tab) =", col_details)
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
                total_width = sum(v or 0 for v in column_widths.values())

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
                #st.write(f"DEBUG: col_details for {col_name} (viz_tab) =", col_details)
                if not col_details:
                    st.warning(f"Could not get details for column {col_name}")
                    continue
                
                # Add visualizations based on data type
                data_type = col_details['data_type'].lower()
                #st.write(f"DEBUG: data_type for {col_name} (viz_tab) = {data_type}")
                
                if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'number', 'real']:
                    # Get value distribution for numeric columns
                    value_counts = connector.get_value_counts(schema, table, col_name)
                    #st.write(f"DEBUG: value_counts for {col_name} (viz_tab) =", value_counts[:10] if value_counts else "EMPTY")
                    if value_counts:
                        #st.write(f"DEBUG: type(value_counts[0]) = {type(value_counts[0])}, value_counts[0] = {value_counts[0]}")
                        # Flatten if needed
                        if len(value_counts) > 0 and len(value_counts[0]) == 1 and isinstance(value_counts[0][0], tuple):
                            value_counts = [row[0] for row in value_counts]
                            #st.write("DEBUG: value_counts flattened =", value_counts[:10])
                        # Convert pyodbc.Row to tuple if needed
                        if hasattr(value_counts[0], '__class__') and value_counts[0].__class__.__name__ == 'Row':
                            value_counts = [tuple(row) for row in value_counts]
                            #st.write("DEBUG: value_counts converted to tuple =", value_counts[:10])
                        df_counts = pd.DataFrame(value_counts, columns=['value', 'count'])
                        #st.write(f"DEBUG: df_counts for {col_name} (viz_tab) =", df_counts.head())
                        df_counts['value'] = pd.to_numeric(df_counts['value'])
                        # Height-balanced histogram (quantile-based)
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
                                query = f"SELECT {quoted_col} FROM {quoted_table}"
                            df_col = pd.read_sql(query, connector.connection)
                            #st.write(f"DEBUG: df_col for {col_name} (viz_tab) =", df_col.head())
                            if not df_col.empty:
                                bin_edges, counts, bin_labels = height_balanced_histogram(df_col[col_name], n_buckets=10)
                                fig = px.bar(x=bin_labels, y=counts, labels={'x': 'Value Range', 'y': 'Count'},
                                            title=f"Height-Balanced Histogram for {col_name}")
                                st.plotly_chart(fig)

                        except Exception as e:
                            st.info(f"Could not plot height-balanced histogram: {e}")
                            #st.write(f"DEBUG: Exception in histogram for {col_name} (viz_tab):", str(e))
                            # Create box plot
                        fig = px.box(df_counts, y='value',
                                    title=f"Box Plot for {col_name}")
                        #st.write(f"DEBUG: Box Plot for {col_name} (viz_tab) created.")
                        st.plotly_chart(fig)
                elif data_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext', 'nvarchar', 'nchar', 'varchar2', 'ntext', 'character varying']:
                    # Get value counts for text columns
                    value_counts = connector.get_value_counts(schema, table, col_name)
                    #st.write(f"DEBUG: value_counts for {col_name} (viz_tab) =", value_counts[:10] if value_counts else "EMPTY")
                    if value_counts:
                        #st.write(f"DEBUG: type(value_counts[0]) = {type(value_counts[0])}, value_counts[0] = {value_counts[0]}")
                        # Flatten if needed
                        if len(value_counts) > 0 and len(value_counts[0]) == 1 and isinstance(value_counts[0][0], tuple):
                            value_counts = [row[0] for row in value_counts]
                            #st.write("DEBUG: value_counts flattened =", value_counts[:10])
                        # Convert pyodbc.Row to tuple if needed
                        if hasattr(value_counts[0], '__class__') and value_counts[0].__class__.__name__ == 'Row':
                            value_counts = [tuple(row) for row in value_counts]
                            #st.write("DEBUG: value_counts converted to tuple =", value_counts[:10])
                        df_counts = pd.DataFrame(value_counts, columns=['value', 'count'])
                        #st.write(f"DEBUG: df_counts for {col_name} (viz_tab) =", df_counts.head())
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
                        heatmap_data = top9_df.pivot_table(index='value', values='count')
                        #st.write(f"DEBUG: heatmap_data for {col_name} (viz_tab) =", heatmap_data)
                        # Create heatmap
                        fig = px.imshow(
                            heatmap_data,
                            color_continuous_scale='Viridis',
                            labels=dict(x="Frequency", y=col_name, color="Count"),
                            title=f"Top 10 Values Heatmap for {col_name}"
                        )
                        #st.write(f"DEBUG: Heatmap for {col_name} (viz_tab) created.")
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

import pandas as pd
import numpy as np

def height_balanced_histogram(series, n_buckets=10):
    series = series.dropna()
    if series.nunique() == 1:
        return [series.min(), series.max()], [len(series)], [f"{series.min()}"]

    try:
        buckets, bin_edges = pd.qcut(series, q=n_buckets, retbins=True, duplicates='raise')
        counts = buckets.value_counts(sort=False).values
        labels = [f"{bin_edges[i]:.2f} - {bin_edges[i+1]:.2f}" for i in range(len(bin_edges)-1)]
        return bin_edges, counts, labels
    except ValueError as e:
        buckets, bin_edges = pd.qcut(series, q=n_buckets, retbins=True, duplicates='drop')
        counts = buckets.value_counts(sort=False).values

        bin_edges = np.array(bin_edges)
        unique_edges = [bin_edges[0]]
        merged_counts = []
        labels = []

        current_count = counts[0]
        for i in range(1, len(counts)):
            if bin_edges[i] == bin_edges[i + 1]:
                current_count += counts[i]
            else:
                unique_edges.append(bin_edges[i])
                merged_counts.append(current_count)
                labels.append(f"{unique_edges[-2]:.2f} - {unique_edges[-1]:.2f}")
                current_count = counts[i]
        unique_edges.append(bin_edges[-1])
        merged_counts.append(current_count)
        labels.append(f"{unique_edges[-2]:.2f} - {unique_edges[-1]:.2f}")

        return unique_edges, merged_counts, labels



# Helper for quoting SQL identifiers (column/table names)
def sql_quote_identifier(identifier, dbtype):
    if dbtype == 'mysql':
        return f'`{identifier}`'
    elif dbtype == 'postgresql':
        return f'"{identifier}"'
    elif dbtype == 'mssql':
        return f'[{identifier}]'
    elif dbtype == 'oracle':
        return f'"{identifier}"'
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
        return f'"{schema}"."{table}"'
    else:
        return f'{schema}.{table}'
