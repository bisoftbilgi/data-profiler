from abc import ABC, abstractmethod
import psycopg2
import pyodbc
import mysql.connector
import pandas as pd

class DatabaseConnector(ABC):
    """Abstract base class for database connectors"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    @abstractmethod
    def connect(self, config):
        """Connect to the database"""
        pass
    
    @abstractmethod
    def close(self):
        """Close the database connection"""
        pass
    
    @abstractmethod
    def get_all_tables_and_views(self, schema):
        """Get all tables and views in the schema"""
        pass
    
    @abstractmethod
    def get_table_analysis(self, schema, table_name):
        """Get detailed table analysis"""
        pass
    
    @abstractmethod
    def get_columns(self, schema, table_name):
        """Get list of all columns in the table"""
        pass
    
    @abstractmethod
    def get_column_details(self, schema, table_name, column_name):
        """Get detailed column analysis"""
        pass

    @abstractmethod
    def get_primary_keys(self, schema, table_name):
        """Return a list of primary key column names for the table"""
        pass

    @abstractmethod
    def get_foreign_keys(self, schema, table_name):
        """Return a dict mapping column names to (referenced_table, referenced_column)"""
        pass

class PostgresConnector(DatabaseConnector):
    """PostgreSQL database connector"""
    
    def connect(self, config):
        """Connect to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**config)
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Error connecting to PostgreSQL: {str(e)}")
    
    def close(self):
        """Close PostgreSQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def get_all_tables_and_views(self, schema):
        """Get all tables and views in PostgreSQL schema"""
        try:
            self.cursor.execute(f"""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
                AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
            """)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting tables and views: {str(e)}")
    
    def get_table_analysis(self, schema, table_name):
        """Get detailed table analysis for PostgreSQL"""
        try:
            self.cursor.execute(f"""
                SELECT 
                    COUNT(*) as row_count,
                    pg_total_relation_size('"{schema}"."{table_name}"') / 1024.0 / 1024.0 as total_size_mb,
                    pg_relation_size('"{schema}"."{table_name}"') / 1024.0 / 1024.0 as table_size_mb,
                    (pg_total_relation_size('"{schema}"."{table_name}"') - pg_relation_size('"{schema}"."{table_name}"')) / 1024.0 / 1024.0 as index_size_mb,
                    pg_relation_size('"{schema}"."{table_name}"') as total_size_bytes,
                    COUNT(*) as row_count,
                    NULL as last_analyzed
                FROM "{schema}"."{table_name}"
            """)
            result = self.cursor.fetchone()
            
            if result:
                row_count, total_size_mb, table_size_mb, index_size_mb, total_size_bytes, _, last_analyzed = result
                avg_row_width = total_size_bytes / row_count if row_count > 0 else 0
                
                return {
                    'row_count': row_count,
                    'total_size': total_size_mb,
                    'table_size': table_size_mb,
                    'index_size': index_size_mb,
                    'avg_row_width': avg_row_width,
                    'last_analyzed': last_analyzed
                }
            return None
        except Exception as e:
            raise Exception(f"Error analyzing table: {str(e)}")
    
    def get_columns(self, schema, table_name):
        """Get list of all columns in PostgreSQL table"""
        try:
            self.cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable = 'YES' as is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")
    
    def get_column_details(self, schema, table_name, column_name):
        """Get detailed column analysis for PostgreSQL"""
        try:
            # First get column data type
            self.cursor.execute(f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                AND column_name = '{column_name}'
            """)
            data_type = self.cursor.fetchone()[0].lower()
            
            # Common metrics for all types
            base_query = f"""
                SELECT 
                    COUNT(DISTINCT "{column_name}") as distinct_count,
                    SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) as null_count
                FROM "{schema}"."{table_name}"
            """
            
            # Add type-specific metrics
            if data_type in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
                # Numeric type metrics
                query = f"""
                    {base_query},
                    MIN("{column_name}") as min_value,
                    MAX("{column_name}") as max_value,
                    AVG("{column_name}") as avg_value,
                    STDDEV("{column_name}") as std_dev,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column_name}") as median_value
                """
            elif data_type in ['character varying', 'character', 'text']:
                # String type metrics
                query = f"""
                    {base_query},
                    MIN(LENGTH("{column_name}")) as min_length,
                    MAX(LENGTH("{column_name}")) as max_length,
                    AVG(LENGTH("{column_name}")) as avg_length,
                    NULL as min_value,
                    NULL as max_value,
                    NULL as avg_value,
                    NULL as std_dev,
                    NULL as median_value
                """
            elif data_type in ['date', 'timestamp', 'timestamp with time zone']:
                # Date type metrics
                query = f"""
                    {base_query},
                    MIN("{column_name}") as min_value,
                    MAX("{column_name}") as max_value,
                    NULL as avg_value,
                    NULL as std_dev,
                    NULL as median_value
                """
            else:
                # Other types
                query = f"""
                    {base_query},
                    NULL as min_value,
                    NULL as max_value,
                    NULL as avg_value,
                    NULL as std_dev,
                    NULL as median_value
                """
            
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            # Get unique count
            unique_count_query = f'''
                SELECT COUNT(*) FROM (
                    SELECT "{column_name}"
                    FROM "{schema}"."{table_name}"
                    GROUP BY "{column_name}"
                    HAVING COUNT(*) = 1
                ) AS unique_values
            '''
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]
            
            # Return metrics based on data type
            if data_type in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
                return {
                    'distinct_count': result[0],
                    'null_count': result[1],
                    'unique_count': unique_count,
                    'min_value': result[2],
                    'max_value': result[3],
                    'avg_value': result[4],
                    'std_dev': result[5],
                    'median_value': result[6],
                    'type': 'numeric'
                }
            elif data_type in ['character varying', 'character', 'text']:
                return {
                    'distinct_count': result[0],
                    'null_count': result[1],
                    'unique_count': unique_count,
                    'min_length': result[2],
                    'max_length': result[3],
                    'avg_length': result[4],
                    'type': 'string'
                }
            elif data_type in ['date', 'timestamp', 'timestamp with time zone']:
                return {
                    'distinct_count': result[0],
                    'null_count': result[1],
                    'unique_count': unique_count,
                    'min_value': result[2],
                    'max_value': result[3],
                    'type': 'date'
                }
            else:
                return {
                    'distinct_count': result[0],
                    'null_count': result[1],
                    'unique_count': unique_count,
                    'type': 'other'
                }
        except Exception as e:
            raise Exception(f"Error analyzing column: {str(e)}")

    def get_primary_keys(self, schema, table_name):
        self.cursor.execute('''
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
        ''', (schema, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute('''
            SELECT kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
        ''', (schema, table_name))
        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}

class MSSQLConnector(DatabaseConnector):
    """MSSQL database connector"""
    
    def connect(self, config):
        """Connect to MSSQL database"""
        try:
            # Construct the connection string
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={config['host']},{config['port']};"
                f"DATABASE={config['dbname']};"
                f"UID={config['user']};"
                f"PWD={config['password']}"
            )
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Error connecting to MSSQL: {str(e)}")
    
    def close(self):
        """Close MSSQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def get_all_tables_and_views(self, schema):
        """Get all tables and views in MSSQL schema"""
        try:
            self.cursor.execute(f"""
                SELECT 
                    t.name as table_name,
                    CASE WHEN t.type = 'V' THEN 'VIEW' ELSE 'TABLE' END as table_type
                FROM sys.tables t
                WHERE t.schema_id = SCHEMA_ID('{schema}')
                UNION ALL
                SELECT 
                    v.name as table_name,
                    'VIEW' as table_type
                FROM sys.views v
                WHERE v.schema_id = SCHEMA_ID('{schema}')
                ORDER BY table_name
            """)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting tables and views: {str(e)}")
    
    def get_table_analysis(self, schema: str, table: str) -> dict:
        """Get detailed analysis of a table including size, row count, and column information"""
        try:
            # Get table size and row count
            size_query = f"""
                SELECT 
                    SUM(p.rows) as row_count,
                    SUM(a.total_pages) * 8 as total_size_kb,
                    SUM(a.used_pages) * 8 as used_size_kb,
                    SUM(a.data_pages) * 8 as data_size_kb,
                    SUM(a.total_pages - a.used_pages) * 8 as index_size_kb,
                    (SUM(a.total_pages) * 8 * 1024) / NULLIF(SUM(p.rows), 0) as avg_row_width,
                    MAX(STATS_DATE(i.object_id, i.index_id)) as last_analyzed
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE t.name = '{table}'
                AND t.schema_id = SCHEMA_ID('{schema}')
                GROUP BY t.name
            """
            self.cursor.execute(size_query)
            size_info = self.cursor.fetchone()
            
            if not size_info:
                return {
                    'row_count': 0,
                    'total_size': 0,
                    'table_size': 0,
                    'index_size': 0,
                    'avg_row_width': 0,
                    'last_analyzed': None,
                    'columns': []
                }
            
            # Get column information
            column_query = f"""
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    c.is_nullable,
                    c.max_length,
                    c.precision,
                    c.scale
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                WHERE tab.name = '{table}'
                AND tab.schema_id = SCHEMA_ID('{schema}')
                ORDER BY c.column_id
            """
            self.cursor.execute(column_query)
            columns = self.cursor.fetchall()
            
            # Convert Decimal values to float
            total_size = float(size_info[1]) / 1024 if size_info[1] else 0  # Convert to MB
            table_size = float(size_info[3]) / 1024 if size_info[3] else 0  # Convert to MB
            index_size = float(size_info[4]) / 1024 if size_info[4] else 0  # Convert to MB
            
            return {
                'row_count': size_info[0] or 0,
                'total_size': round(total_size, 2),
                'table_size': round(table_size, 2),
                'index_size': round(index_size, 2),
                'avg_row_width': size_info[5] or 0,
                'last_analyzed': size_info[6],
                'columns': columns
            }
            
        except Exception as e:
            raise Exception(f"Error getting table analysis: {str(e)}")
    
    def get_columns(self, schema, table_name):
        """Get list of all columns in MSSQL table"""
        try:
            self.cursor.execute(f"""
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    c.is_nullable
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                WHERE tab.name = '{table_name}'
                AND tab.schema_id = SCHEMA_ID('{schema}')
                ORDER BY c.column_id
            """)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")
    
    def get_column_details(self, schema: str, table: str, column: str) -> dict:
        """Get detailed analysis for a specific column"""
        try:
            # Get column data type
            type_query = f"""
                SELECT DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table}' 
                AND COLUMN_NAME = '{column}'
            """
            self.cursor.execute(type_query)
            data_type = self.cursor.fetchone()[0]
            
            # Base query for distinct and null counts
            base_query = f"""
                SELECT 
                    COUNT(DISTINCT [{column}]) as distinct_count,
                    SUM(CASE WHEN [{column}] IS NULL THEN 1 ELSE 0 END) as null_count
                FROM [{schema}].[{table}]
            """
            self.cursor.execute(base_query)
            distinct_count, null_count = self.cursor.fetchone()
            
            # Get unique count
            unique_count_query = f"""
                SELECT COUNT(*) FROM (
                    SELECT [{column}]
                    FROM [{schema}].[{table}]
                    GROUP BY [{column}]
                    HAVING COUNT(*) = 1
                ) AS unique_values
            """
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]
            
            metrics = {}
            
            # Add type-specific metrics
            if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
                metrics_query = f"""
                    SELECT 
                        MIN(CAST([{column}] AS FLOAT)) as min_val,
                        MAX(CAST([{column}] AS FLOAT)) as max_val,
                        AVG(CAST([{column}] AS FLOAT)) as avg_val,
                        STDEV(CAST([{column}] AS FLOAT)) as std_dev
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                min_val, max_val, avg_val, std_dev = self.cursor.fetchone()
                
                # Calculate median
                median_query = f"""
                    SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST([{column}] AS FLOAT)) 
                    OVER () as median
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                """
                self.cursor.execute(median_query)
                median = self.cursor.fetchone()[0]
                
                metrics.update({
                    'min': min_val,
                    'max': max_val,
                    'avg': avg_val,
                    'median': median,
                    'std_dev': std_dev
                })
                
            elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                metrics_query = f"""
                    SELECT 
                        MIN(LEN([{column}])) as min_length,
                        MAX(LEN([{column}])) as max_length,
                        AVG(CAST(LEN([{column}]) AS FLOAT)) as avg_length
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                min_length, max_length, avg_length = self.cursor.fetchone()
                
                metrics.update({
                    'min_length': min_length,
                    'max_length': max_length,
                    'avg_length': avg_length
                })
                
            elif data_type in ['date', 'datetime', 'datetime2', 'smalldatetime']:
                metrics_query = f"""
                    SELECT 
                        MIN([{column}]) as min_date,
                        MAX([{column}]) as max_date
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                min_date, max_date = self.cursor.fetchone()
                
                metrics.update({
                    'min_date': min_date,
                    'max_date': max_date
                })
            
            return {
                'data_type': data_type,
                'distinct_count': distinct_count,
                'null_count': null_count,
                'unique_count': unique_count,
                'metrics': metrics
            }
            
        except Exception as e:
            raise Exception(f"Error getting column details: {str(e)}")

    def get_sample_data(self, schema: str, table: str, limit: int = 100) -> list:
        """Get sample data from the table"""
        try:
            query = f"""
                SELECT TOP {limit} *
                FROM [{schema}].[{table}]
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting sample data: {str(e)}")

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_primary_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
        """, (schema, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (schema, table_name))
        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}

class MySQLConnector(DatabaseConnector):
    """MySQL database connector implementation"""
    
    def connect(self, config: dict) -> None:
        """Connect to MySQL database"""
        try:
            import mysql.connector
            self.connection = mysql.connector.connect(
                host=config.get('host', 'localhost'),
                port=config.get('port', 3306),
                user=config.get('user'),
                password=config.get('password'),
                database=config.get('dbname')
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Error connecting to MySQL: {str(e)}")
    
    def close(self) -> None:
        """Close MySQL connection"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
    
    def get_all_tables_and_views(self, schema: str) -> list:
        """Get all tables and views from MySQL database"""
        try:
            query = """
                SELECT 
                    table_name,
                    CASE 
                        WHEN table_type = 'VIEW' THEN 'VIEW'
                        ELSE 'TABLE'
                    END as object_type
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
            """
            self.cursor.execute(query, (schema,))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting tables and views: {str(e)}")
    
    def get_table_analysis(self, schema: str, table: str) -> dict:
        """Get detailed analysis of a table including size, row count, and column information"""
        try:
            # Get table size and row count
            size_query = """
                SELECT 
                    table_rows as row_count,
                    data_length / 1024 as data_size_kb,
                    index_length / 1024 as index_size_kb,
                    (data_length + index_length) / 1024 as total_size_kb,
                    avg_row_length as avg_row_width,
                    update_time as last_analyzed
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """
            self.cursor.execute(size_query, (schema, table))
            size_info = self.cursor.fetchone()
            
            if not size_info:
                return {
                    'row_count': 0,
                    'total_size': 0,
                    'table_size': 0,
                    'index_size': 0,
                    'avg_row_width': 0,
                    'last_analyzed': None,
                    'columns': []
                }
            
            # Get column information
            column_query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                ORDER BY ordinal_position
            """
            self.cursor.execute(column_query, (schema, table))
            columns = self.cursor.fetchall()
            
            # Convert sizes to MB
            total_size = float(size_info[3]) / 1024 if size_info[3] else 0
            table_size = float(size_info[1]) / 1024 if size_info[1] else 0
            index_size = float(size_info[2]) / 1024 if size_info[2] else 0
            
            # Convert datetime to string if present
            last_analyzed = size_info[5]
            if last_analyzed:
                last_analyzed = last_analyzed.strftime('%Y-%m-%d %H:%M:%S')
            
            return {
                'row_count': size_info[0] or 0,
                'total_size': round(total_size, 2),
                'table_size': round(table_size, 2),
                'index_size': round(index_size, 2),
                'avg_row_width': size_info[4] or 0,
                'last_analyzed': last_analyzed,
                'columns': columns
            }
            
        except Exception as e:
            raise Exception(f"Error getting table analysis: {str(e)}")
    
    def get_columns(self, schema: str, table: str) -> list:
        """Get column information for a table"""
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s
                ORDER BY ordinal_position
            """
            self.cursor.execute(query, (schema, table))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")
    
    def get_column_details(self, schema: str, table: str, column: str) -> dict:
        """Get detailed column analysis"""
        try:
            # Get basic column information
            query = """
                SELECT 
                    data_type,
                    is_nullable,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = %s 
                AND column_name = %s
            """
            self.cursor.execute(query, (schema, table, column))
            col_info = self.cursor.fetchone()
            
            if not col_info:
                return {
                    'data_type': None,
                    'distinct_count': 0,
                    'null_count': 0,
                    'metrics': {}
                }
            
            # Get distinct and null counts
            count_query = f"""
                SELECT 
                    COUNT(DISTINCT `{column}`) as distinct_count,
                    SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) as null_count
                FROM `{schema}`.`{table}`
            """
            self.cursor.execute(count_query)
            counts = self.cursor.fetchone()

            # Get unique count
            unique_count_query = f"""
                SELECT COUNT(*) FROM (
                    SELECT `{column}`
                    FROM `{schema}`.`{table}`
                    GROUP BY `{column}`
                    HAVING COUNT(*) = 1
                ) AS unique_values
            """
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]
            
            # Get type-specific metrics
            metrics = {}
            data_type = col_info[0].lower()
            
            if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double']:
                # Get numeric metrics
                metrics_query = f"""
                    SELECT 
                        MIN(`{column}`) as min_val,
                        MAX(`{column}`) as max_val,
                        AVG(`{column}`) as avg_val,
                        STDDEV(`{column}`) as std_dev
                    FROM `{schema}`.`{table}`
                    WHERE `{column}` IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                numeric_metrics = self.cursor.fetchone()
                
                if numeric_metrics:
                    metrics.update({
                        'min': numeric_metrics[0],
                        'max': numeric_metrics[1],
                        'avg': numeric_metrics[2],
                        'std_dev': numeric_metrics[3]
                    })
            
            elif data_type in ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext']:
                # Get string metrics
                metrics_query = f"""
                    SELECT 
                        MIN(LENGTH(`{column}`)) as min_length,
                        MAX(LENGTH(`{column}`)) as max_length,
                        AVG(LENGTH(`{column}`)) as avg_length
                    FROM `{schema}`.`{table}`
                    WHERE `{column}` IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                string_metrics = self.cursor.fetchone()
                
                if string_metrics:
                    metrics.update({
                        'min_length': string_metrics[0],
                        'max_length': string_metrics[1],
                        'avg_length': string_metrics[2]
                    })
            
            elif data_type in ['date', 'datetime', 'timestamp']:
                # Get date metrics
                metrics_query = f"""
                    SELECT 
                        MIN(`{column}`) as min_date,
                        MAX(`{column}`) as max_date
                    FROM `{schema}`.`{table}`
                    WHERE `{column}` IS NOT NULL
                """
                self.cursor.execute(metrics_query)
                date_metrics = self.cursor.fetchone()
                
                if date_metrics:
                    # Convert datetime objects to strings
                    min_date = date_metrics[0].strftime('%Y-%m-%d %H:%M:%S') if date_metrics[0] else None
                    max_date = date_metrics[1].strftime('%Y-%m-%d %H:%M:%S') if date_metrics[1] else None
                    metrics.update({
                        'min_date': min_date,
                        'max_date': max_date
                    })
            
            return {
                'data_type': col_info[0],
                'distinct_count': counts[0] if counts else 0,
                'null_count': counts[1] if counts else 0,
                'unique_count': unique_count,
                'metrics': metrics
            }
            
        except Exception as e:
            raise Exception(f"Error getting column details: {str(e)}")
    
    def get_sample_data(self, schema: str, table: str, limit: int = 100) -> list:
        """Get sample data from a table"""
        try:
            query = f"SELECT * FROM `{schema}`.`{table}` LIMIT {limit}"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting sample data: {str(e)}")
    
    def get_value_counts(self, schema: str, table: str, column: str, limit: int = 100) -> list:
        """Get value counts for a column"""
        try:
            query = f"""
                SELECT `{column}`, COUNT(*) as count
                FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL
                GROUP BY `{column}`
                ORDER BY count DESC
                LIMIT {limit}
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting value counts: {str(e)}")

    def get_primary_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
        """, (schema, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (schema, table_name))
        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}

class OracleConnector(DatabaseConnector):
    """Oracle database connector implementation"""
    def connect(self, config: dict) -> None:
        """Connect to Oracle database"""
        try:
            import oracledb
            dsn = f"{config.get('host')}:{config.get('port')}/{config.get('dbname')}"
            self.connection = oracledb.connect(
                user=config.get('user'),
                password=config.get('password'),
                dsn=dsn
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Error connecting to Oracle: {str(e)}")

    def close(self) -> None:
        """Close Oracle connection"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()

    def get_all_tables_and_views(self, schema: str) -> list:
        """Get all tables and views from Oracle database"""
        try:
            query = f"""
                SELECT table_name, 'TABLE' as object_type FROM all_tables WHERE owner = :schema
                UNION ALL
                SELECT view_name as table_name, 'VIEW' as object_type FROM all_views WHERE owner = :schema
                ORDER BY table_name
            """
            self.cursor.execute(query, schema=schema.upper())
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting tables and views: {str(e)}")

    def get_table_analysis(self, schema: str, table: str) -> dict:
        """Get detailed analysis of a table including size, row count, and column information"""
        try:
            # Get row count
            self.cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row_count = self.cursor.fetchone()[0]

            # Get table size (in MB)
            size_query = """
                SELECT NVL(SUM(bytes),0)/1024/1024 AS total_size_mb
                FROM dba_segments
                WHERE owner = :schema AND segment_name = :table AND segment_type = 'TABLE'
            """
            self.cursor.execute(size_query, schema=schema.upper(), table=table.upper())
            total_size_mb = self.cursor.fetchone()[0]

            # Get column information
            column_query = """
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table
                ORDER BY column_id
            """
            self.cursor.execute(column_query, schema=schema.upper(), table=table.upper())
            columns = self.cursor.fetchall()

            return {
                'row_count': row_count,
                'total_size': total_size_mb,
                'table_size': total_size_mb,  # Oracle does not separate index size easily here
                'index_size': 0,
                'avg_row_width': None,
                'last_analyzed': None,
                'columns': columns
            }
        except Exception as e:
            raise Exception(f"Error getting table analysis: {str(e)}")

    def get_columns(self, schema: str, table: str) -> list:
        """Get column information for a table"""
        try:
            query = """
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table
                ORDER BY column_id
            """
            self.cursor.execute(query, schema=schema.upper(), table=table.upper())
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")

    def get_column_details(self, schema: str, table: str, column: str) -> dict:
        """Get detailed column analysis"""
        try:
            # Get column data type
            col_info_query = """
                SELECT data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table AND column_name = :column
            """
            self.cursor.execute(col_info_query, schema=schema.upper(), table=table.upper(), column=column.upper())
            col_info = self.cursor.fetchone()
            if not col_info:
                return {
                    'data_type': None,
                    'distinct_count': 0,
                    'null_count': 0,
                    'metrics': {}
                }
            data_type = col_info[0].lower()

            # Get distinct and null counts
            count_query = f'SELECT COUNT(DISTINCT "{column}") AS distinct_count, SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) AS null_count FROM "{schema}"."{table}"'
            self.cursor.execute(count_query)
            counts = self.cursor.fetchone()

            # Get unique count
            unique_count_query = f'SELECT COUNT(*) FROM (SELECT "{column}" FROM "{schema}"."{table}" GROUP BY "{column}" HAVING COUNT(*) = 1)'
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]

            metrics = {}
            if data_type in ['number', 'float', 'integer', 'decimal']:
                metrics_query = f'SELECT MIN("{column}"), MAX("{column}"), AVG("{column}") FROM "{schema}"."{table}" WHERE "{column}" IS NOT NULL'
                self.cursor.execute(metrics_query)
                min_val, max_val, avg_val = self.cursor.fetchone()
                metrics.update({'min': min_val, 'max': max_val, 'avg': avg_val})
            elif data_type in ['varchar2', 'char', 'nvarchar2', 'nchar', 'clob']:
                metrics_query = f'SELECT MIN(LENGTH("{column}")), MAX(LENGTH("{column}")), AVG(LENGTH("{column}")) FROM "{schema}"."{table}" WHERE "{column}" IS NOT NULL'
                self.cursor.execute(metrics_query)
                min_length, max_length, avg_length = self.cursor.fetchone()
                metrics.update({'min_length': min_length, 'max_length': max_length, 'avg_length': avg_length})
            elif data_type in ['date', 'timestamp']:
                metrics_query = f'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}" WHERE "{column}" IS NOT NULL'
                self.cursor.execute(metrics_query)
                min_date, max_date = self.cursor.fetchone()
                metrics.update({'min_date': str(min_date) if min_date else None, 'max_date': str(max_date) if max_date else None})

            return {
                'data_type': col_info[0],
                'distinct_count': counts[0] if counts else 0,
                'null_count': counts[1] if counts else 0,
                'unique_count': unique_count,
                'metrics': metrics
            }
        except Exception as e:
            raise Exception(f"Error getting column details: {str(e)}")

    def get_sample_data(self, schema: str, table: str, limit: int = 100) -> list:
        """Get sample data from a table"""
        try:
            query = f'SELECT * FROM "{schema}"."{table}" WHERE ROWNUM <= {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting sample data: {str(e)}")

    def get_primary_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT c.name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            WHERE i.is_primary_key = 1 AND t.name = ? AND SCHEMA_NAME(t.schema_id) = ?
        """, (table_name, schema))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT 
                parent_col.name AS column_name,
                ref_table.name AS referenced_table,
                ref_col.name AS referenced_column
            FROM sys.foreign_key_columns fk
            INNER JOIN sys.tables parent_table ON fk.parent_object_id = parent_table.object_id
            INNER JOIN sys.columns parent_col ON fk.parent_object_id = parent_col.object_id AND fk.parent_column_id = parent_col.column_id
            INNER JOIN sys.tables ref_table ON fk.referenced_object_id = ref_table.object_id
            INNER JOIN sys.columns ref_col ON fk.referenced_object_id = ref_col.object_id AND fk.referenced_column_id = ref_col.column_id
            WHERE parent_table.name = ? AND SCHEMA_NAME(parent_table.schema_id) = ?
        """, (table_name, schema))
        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()} 