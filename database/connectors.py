from abc import ABC, abstractmethod
import psycopg2
import pyodbc
import mysql.connector
import oracledb
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

    @abstractmethod
    def get_null_count(self, schema, table, column):
        """Get count of null values in a column"""
        pass

    @abstractmethod
    def get_distinct_count(self, schema, table, column):
        """Get count of distinct values in a column"""
        pass

    @abstractmethod
    def get_min_max_range(self, schema, table, column):
        pass

    @abstractmethod
    def get_allowed_values_violation_count(self, schema, table, column, allowed_values):
        """Get count of values that are not in the allowed values list"""
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
        """Close PostgreSQL connection safely"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            logger.warning(f"PostgreSQL cursor close error: {e}")

        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
        except Exception as e:
            logger.warning(f"PostgreSQL connection close error: {e}")

    def ensure_connected(self, config: dict):
        try:
            self.cursor.execute("SELECT 1")
        except:
            self.connect(config)


    
    def get_all_tables_and_views(self, schema):

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

        try:
            self.cursor.execute(f"""
                SELECT 
                    COUNT(*) as row_count,
                    pg_total_relation_size('"{schema}"."{table_name}"') / 1024.0 / 1024.0 as total_size_mb,
                    pg_relation_size('"{schema}"."{table_name}"') / 1024.0 / 1024.0 as table_size_mb,
                    (pg_total_relation_size('"{schema}"."{table_name}"') - pg_relation_size('"{schema}"."{table_name}"')) / 1024.0 / 1024.0 as index_size_mb,
                    pg_relation_size('"{schema}"."{table_name}"') as total_size_bytes,
                    pg_relation_size('"{schema}"."{table_name}"') / NULLIF(COUNT(*), 0) as avg_row_width,
                    NULL as last_analyzed
                FROM "{schema}"."{table_name}"
            """)
            
            result = self.cursor.fetchone()

            if result:
                self.cursor.execute('''
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                ''', (schema, table_name))
                columns = self.cursor.fetchall()
                return {
                    'row_count': result[0],
                    'total_size': result[1],
                    'table_size': result[2],
                    'index_size': result[3],
                    'avg_row_width': result[5],
                    'last_analyzed': result[6],
                    'columns': columns
                }
            else:
                return None

        except Exception as e:
            raise Exception(f"Error analyzing table: {str(e)}")

    
    def get_columns(self, schema, table_name):
        """Get list of all columns in PostgreSQL table, returning 6 fields for compatibility"""
        try:
            self.cursor.execute('''
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
            ''', (schema, table_name))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")

    
    def get_column_details(self, schema, table_name, column_name):
        """Get detailed column analysis for PostgreSQL"""
        try:
            # Get column data type
            self.cursor.execute('''
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                AND column_name = %s
            ''', (schema, table_name, column_name))
            data_type = self.cursor.fetchone()[0].lower()

            # Common metrics for all types
            base_query = f'''
                SELECT 
                    COUNT(DISTINCT "{column_name}") as distinct_count,
                    SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) as null_count
                FROM "{schema}"."{table_name}"
            '''
            self.cursor.execute(base_query)
            counts = self.cursor.fetchone()

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

            metrics = {}
            if data_type in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
                # Numeric type metrics (including median)
                query = f'''
                    SELECT 
                        MIN("{column_name}") as min_value,
                        MAX("{column_name}") as max_value,
                        AVG("{column_name}") as avg_value,
                        STDDEV("{column_name}") as std_dev,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column_name}") as median_value
                    FROM "{schema}"."{table_name}"
                '''
                self.cursor.execute(query)
                min_value, max_value, avg_value, std_dev, median_value = self.cursor.fetchone()
                metrics.update({
                    'min': min_value,
                    'max': max_value,
                    'avg': avg_value,
                    'std_dev': std_dev,
                    'median': median_value
                })
            elif data_type in ['character varying', 'character', 'text']:
                # String type metrics
                query = f'''
                    SELECT 
                        MIN(LENGTH("{column_name}")) as min_length,
                        MAX(LENGTH("{column_name}")) as max_length,
                        AVG(LENGTH("{column_name}")) as avg_length
                    FROM "{schema}"."{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                '''
                self.cursor.execute(query)
                min_length, max_length, avg_length = self.cursor.fetchone()
                metrics.update({
                    'min_length': min_length,
                    'max_length': max_length,
                    'avg_length': avg_length
                })
            elif data_type in ['date', 'timestamp', 'timestamp with time zone']:
                # Date type metrics
                query = f'''
                    SELECT 
                        MIN("{column_name}") as min_value,
                        MAX("{column_name}") as max_value
                    FROM "{schema}"."{table_name}"
                '''
                self.cursor.execute(query)
                min_value, max_value = self.cursor.fetchone()
                metrics.update({
                    'min_date': min_value,
                    'max_date': max_value
                })
            return {
                'data_type': data_type,
                'distinct_count': counts[0] if counts else 0,
                'null_count': counts[1] if counts else 0,
                'unique_count': unique_count,
                'metrics': metrics
            }
        except Exception as e:
            raise Exception(f"Error getting column details: {str(e)}")

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

    def get_sample_data(self, schema: str, table: str, limit: int = 100) -> list:
        """Get sample data from a PostgreSQL table"""
        try:
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT %s'
            self.cursor.execute(query, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting sample data: {str(e)}")

    def get_value_counts(self, schema: str, table: str, column: str, limit: int = 100) -> list:
        """Get value counts for a column in PostgreSQL"""
        try:
            query = f'''
                SELECT "{column}", COUNT(*) as count
                FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                GROUP BY "{column}"
                ORDER BY count DESC
                LIMIT %s
            '''
            self.cursor.execute(query, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting value counts: {str(e)}")
        
    def get_null_count(self, schema, table, column):
        query = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{column}" IS NULL'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_distinct_count(self, schema, table, column):
        query = f'SELECT COUNT(DISTINCT "{column}") FROM "{schema}"."{table}"'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_null_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT * FROM "{schema}"."{table}" WHERE "{column}" IS NULL LIMIT {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching null violations: {str(e)}")

    def get_non_distinct_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                AND "{column}" IN (
                    SELECT "{column}"
                    FROM "{schema}"."{table}"
                    GROUP BY "{column}"
                    HAVING COUNT(*) > 1
                )
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching non-distinct violations: {str(e)}")
    
    def get_all_tables_and_views(self, schema):
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
        
    def get_char_length_range(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT MIN(LENGTH("{column}")), MAX(LENGTH("{column}"))
                FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
            ''')
            min_len, max_len = self.cursor.fetchone()
            return {'min_length': min_len, 'max_length': max_len}
        except Exception as e:
            raise Exception(f"Error getting character length range: {str(e)}")
        
    def get_invalid_datetime_count(self, schema, table, column, datetime_check_format='YYYY-MM-DD HH24:MI:SS'):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE TO_CHAR("{column}", "{datetime_check_format}") IS NULL
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking datetime format: {str(e)}")
        
    def get_letter_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE CAST("{column}" AS TEXT) ~ '[A-Za-z]'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for letters: {str(e)}")

    def get_min_max_violations(self, schema, table, column, min_val, max_val, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" < {min_val} OR "{column}" > {max_val}
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching min-max violations: {str(e)}")

    def get_char_length_violations(self, schema, table, column, min_len, max_len, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE LENGTH("{column}") < {min_len} OR LENGTH("{column}") > {max_len}
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching character length violations: {str(e)}")

    def get_invalid_datetime_violations(self, schema, table, column, limit=100, datetime_check_format='YYYY-MM-DD HH24:MI:SS'):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE TO_DATE("{column}", "{datetime_check_format}") IS NULL AND "{column}" IS NOT NULL
                FETCH FIRST {limit} ROWS ONLY
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching invalid datetime values: {str(e)}")

    def get_letter_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE(CAST("{column}" AS TEXT), '[A-Za-z]')
                FETCH FIRST {limit} ROWS ONLY
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching letter violations: {str(e)}")

    def get_number_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE CAST("{column}" AS TEXT) ~ '[0-9]'
            '''
            print(f"[DEBUG] Running query:\n{query}")  # print first
            self.cursor.execute(query)

            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for numbers: {str(e)}")
        
    def get_allowed_values_violation_count(self, schema, table, column, allowed_values):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            total_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
            '''
            violation_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({formatted_values})
            '''
            non_violation_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}"  IN ({formatted_values})
            '''
            self.cursor.execute(total_query)
            total = self.cursor.fetchone()[0]
            self.cursor.execute(violation_query)
            violation = self.cursor.fetchone()[0]
            self.cursor.execute(non_violation_query)
            non_violation = self.cursor.fetchone()[0]
            return {
                'total': total,
                'violation': violation,
                'non_violation': non_violation
            }
        except Exception as e:
            raise Exception(f"Error checking allowed values: {str(e)}")
        
    def get_eng_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}"::TEXT LIKE '%%,%%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking ENG format: {str(e)}")

    def get_tr_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}"::TEXT NOT LIKE '%%,%%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking TR format: {str(e)}")
        
    def get_number_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE(CAST("{column}" AS TEXT), '[0-9]')
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching number violations: {str(e)}")

    def get_allowed_values_violations(self, schema, table, column, allowed_values, limit=100):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({formatted_values})
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching allowed values violations: {str(e)}")

    def get_eng_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}"::TEXT LIKE '%%,%%'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching ENG numeric format violations: {str(e)}")

    def get_tr_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}"::TEXT NOT LIKE '%%,%%'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching TR numeric format violations: {str(e)}")
        
    def get_case_inconsistency_count(self, schema, table, column, expected_case):
        try:
            if expected_case == 'upper':
                condition = f'"{column}" != UPPER("{column}")'
            elif expected_case == 'lower':
                condition = f'"{column}" != LOWER("{column}")'
            else:
                raise ValueError("Unsupported case type")

            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND {condition}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking case consistency: {str(e)}")
        
    def get_future_date_violation_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" > CURRENT_DATE
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking future dates: {str(e)}")

    def get_date_range_violation_count(self, schema, table, column, start_date, end_date):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" < DATE '{start_date}' OR "{column}" > DATE '{end_date}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking date range: {str(e)}")

    def get_special_char_violation_count(self, schema, table, column, allowed_pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" !~ '{allowed_pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking special characters: {str(e)}")
        
    def get_case_inconsistency_violations(self, schema, table, column, expected_case, limit=100):
        try:
            if expected_case == 'upper':
                condition = f'"{column}" != UPPER("{column}")'
            elif expected_case == 'lower':
                condition = f'"{column}" != LOWER("{column}")'
            else:
                raise ValueError("Unsupported case type")

            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND {condition}
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching case inconsistency violations: {str(e)}")

    def get_future_date_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" > CURRENT_DATE
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching future date violations: {str(e)}")

    def get_date_range_violations(self, schema, table, column, start_date, end_date, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" < DATE '{start_date}' OR "{column}" > DATE '{end_date}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching date range violations: {str(e)}")

    def get_special_char_violations(self, schema, table, column, allowed_pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" !~ '{allowed_pattern}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching special character violations: {str(e)}")
        


        

    def get_email_format_violation_count(self, schema, table, column):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" !~ '{regex}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking email format: {str(e)}")

    def get_regex_pattern_violation_count(self, schema, table, column, pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" !~ '{pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking regex pattern: {str(e)}")

    def get_positive_value_violation_count(self, schema, table, column, strict):
        try:
            operator = '>' if strict else '>='
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT ("{column}" {operator} 0)
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking positive values: {str(e)}")
    
    def get_email_format_violations(self, schema, table, column, limit=100):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" !~ '{regex}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching email format violations: {str(e)}")

    def get_regex_pattern_violations(self, schema, table, column, pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" !~ '{pattern}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching regex pattern violations: {str(e)}")

    def get_positive_value_violations(self, schema, table, column, strict, limit=100):
        try:
            operator = '>' if strict else '>='
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT ("{column}" {operator} 0)
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching positive value violations: {str(e)}")
        
    def get_min_max_range(self, schema, table, column):
        try:
            query = f'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}"'
            self.cursor.execute(query)
            min_val, max_val = self.cursor.fetchone()
            return {
                'min': min_val,
                'max': max_val,
                'range': max_val - min_val if min_val is not None and max_val is not None else None
            }
        except Exception as e:
            raise Exception(f"Error getting min-max range: {str(e)}")




    def get_tckn_violation_count(self, schema, table, column):
        """Count all invalid TCKN values in the column (pure SQL)"""
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                  AND NOT (
                    LENGTH("{column}") = 11
                    AND "{column}" ~ '^[0-9]+$'
                    AND SUBSTRING("{column}", 1, 1) <> '0'
                    AND (
                        (
                            (
                                CAST(SUBSTRING("{column}",1,1) AS integer) +
                                CAST(SUBSTRING("{column}",3,1) AS integer) +
                                CAST(SUBSTRING("{column}",5,1) AS integer) +
                                CAST(SUBSTRING("{column}",7,1) AS integer) +
                                CAST(SUBSTRING("{column}",9,1) AS integer)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING("{column}",2,1) AS integer) +
                                CAST(SUBSTRING("{column}",4,1) AS integer) +
                                CAST(SUBSTRING("{column}",6,1) AS integer) +
                                CAST(SUBSTRING("{column}",8,1) AS integer)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING("{column}",10,1) AS integer)
                    AND (
                        (
                            CAST(SUBSTRING("{column}",1,1) AS integer) +
                            CAST(SUBSTRING("{column}",2,1) AS integer) +
                            CAST(SUBSTRING("{column}",3,1) AS integer) +
                            CAST(SUBSTRING("{column}",4,1) AS integer) +
                            CAST(SUBSTRING("{column}",5,1) AS integer) +
                            CAST(SUBSTRING("{column}",6,1) AS integer) +
                            CAST(SUBSTRING("{column}",7,1) AS integer) +
                            CAST(SUBSTRING("{column}",8,1) AS integer) +
                            CAST(SUBSTRING("{column}",9,1) AS integer) +
                            CAST(SUBSTRING("{column}",10,1) AS integer)
                        ) % 10
                    ) = CAST(SUBSTRING("{column}",11,1) AS integer)
                  )
            '''
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            return count
        except Exception as e:
            raise Exception(f"Error counting TCKN violations: {str(e)}")

    def get_tckn_violations(self, schema, table, column, limit=100):
        """Get sample rows with invalid TCKN values (pure SQL)"""
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                  AND NOT (
                    LENGTH("{column}") = 11
                    AND "{column}" ~ '^[0-9]+$'
                    AND SUBSTRING("{column}", 1, 1) <> '0'
                    AND (
                        (
                            (
                                CAST(SUBSTRING("{column}",1,1) AS integer) +
                                CAST(SUBSTRING("{column}",3,1) AS integer) +
                                CAST(SUBSTRING("{column}",5,1) AS integer) +
                                CAST(SUBSTRING("{column}",7,1) AS integer) +
                                CAST(SUBSTRING("{column}",9,1) AS integer)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING("{column}",2,1) AS integer) +
                                CAST(SUBSTRING("{column}",4,1) AS integer) +
                                CAST(SUBSTRING("{column}",6,1) AS integer) +
                                CAST(SUBSTRING("{column}",8,1) AS integer)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING("{column}",10,1) AS integer)
                    AND (
                        (
                            CAST(SUBSTRING("{column}",1,1) AS integer) +
                            CAST(SUBSTRING("{column}",2,1) AS integer) +
                            CAST(SUBSTRING("{column}",3,1) AS integer) +
                            CAST(SUBSTRING("{column}",4,1) AS integer) +
                            CAST(SUBSTRING("{column}",5,1) AS integer) +
                            CAST(SUBSTRING("{column}",6,1) AS integer) +
                            CAST(SUBSTRING("{column}",7,1) AS integer) +
                            CAST(SUBSTRING("{column}",8,1) AS integer) +
                            CAST(SUBSTRING("{column}",9,1) AS integer) +
                            CAST(SUBSTRING("{column}",10,1) AS integer)
                        ) % 10
                    ) = CAST(SUBSTRING("{column}",11,1) AS integer)
                  )
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            raise Exception(f"Error fetching TCKN violations: {str(e)}")

    def get_date_logic_violation_count(self, schema, table, start_date_col, end_date_col):
        """Count rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT COUNT(*) 
                FROM "{schema}"."{table}"
                WHERE "{start_date_col}" IS NOT NULL
                AND "{end_date_col}" IS NOT NULL
                AND "{start_date_col}" >= "{end_date_col}"
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error counting date logic violations: {str(e)}")

    def get_date_logic_violations(self, schema, table, start_date_col, end_date_col, limit=100):
        """Get sample rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT * 
                FROM "{schema}"."{table}"
                WHERE "{start_date_col}" IS NOT NULL
                AND "{end_date_col}" IS NOT NULL
                AND "{start_date_col}" >= "{end_date_col}"
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching date logic violations: {str(e)}")
        
    def get_text_column_date_formats(self, schema, table, column_name, limit=1000):
        try:
            query = f"""
                SELECT *,

                    CASE
                        WHEN {column_name} ~ '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$' THEN 'DD.MM.YYYY'
                        WHEN {column_name} ~ '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$' THEN 'YYYY-MM-DD'
                        WHEN {column_name} ~ '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$' THEN 'MM/DD/YYYY'
                        WHEN {column_name} ~ '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$' THEN 'DD/MM/YYYY'
                        WHEN {column_name} ~ '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$' THEN 'YYYY.MM.DD'
                        ELSE 'Unknown'
                    END AS format,

                    CASE
                        WHEN {column_name} ~ '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$' AND TO_DATE({column_name}, 'DD.MM.YYYY') IS NOT NULL THEN TRUE
                        WHEN {column_name} ~ '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$' AND TO_DATE({column_name}, 'YYYY-MM-DD') IS NOT NULL THEN TRUE
                        WHEN {column_name} ~ '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$' AND TO_DATE({column_name}, 'MM/DD/YYYY') IS NOT NULL THEN TRUE
                        WHEN {column_name} ~ '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$' AND TO_DATE({column_name}, 'DD/MM/YYYY') IS NOT NULL THEN TRUE
                        WHEN {column_name} ~ '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$' AND TO_DATE({column_name}, 'YYYY.MM.DD') IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END AS is_valid,

                    COALESCE(
                        CASE WHEN {column_name} ~ '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$' THEN TO_DATE({column_name}, 'DD.MM.YYYY') END,
                        CASE WHEN {column_name} ~ '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$' THEN TO_DATE({column_name}, 'YYYY-MM-DD') END,
                        CASE WHEN {column_name} ~ '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$' THEN TO_DATE({column_name}, 'MM/DD/YYYY') END,
                        CASE WHEN {column_name} ~ '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$' THEN TO_DATE({column_name}, 'DD/MM/YYYY') END,
                        CASE WHEN {column_name} ~ '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$' THEN TO_DATE({column_name}, 'YYYY.MM.DD') END
                    ) AS parsed_date

                FROM "{schema}"."{table}"
                WHERE {column_name} IS NOT NULL
                LIMIT {limit};
            """

            self.cursor.execute(query)

            # ✅ Convert rows to list of dictionaries
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            raise Exception(f"Error fetching date logic violations: {str(e)}")

    def get_date_format_violation_count(self, schema, table, column_name, date_format_regex, limit=100):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE NOT "{column_name}" ~ %s
            '''
            self.cursor.execute(query, (date_format_regex,))

            print("DEBUG SQL preview:")
            print(query.replace("%s", f"'{date_format_regex}'"))  # Sadece görsel amaçlı

            return self.cursor.fetchone()[0]



        except Exception as e:
            raise Exception(f"Error counting date format violation count: {str(e)}")

    def get_date_format_violations(self, schema, table, column_name, date_format_regex, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE  NOT "{column_name}" ~ %s
            '''
            self.cursor.execute(query, (date_format_regex,))
            return self.cursor.fetchall()

        except Exception as e:
            raise Exception(f"Error counting date format violation count: {str(e)}")


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
        """Close MSSQL connection safely"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            logger.warning(f"MSSQL cursor close error: {e}")

        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
        except Exception as e:
            logger.warning(f"MSSQL connection close error: {e}")

    def ensure_connected(self, config: dict):
        try:
            # lightweight test
            self.cursor.execute("SELECT 1")
            _ = self.cursor.fetchone()
        except pyodbc.Error:
            # if that fails, reconnect
            self.connect(config)

    
    def get_all_tables_and_views(self, schema: str) -> list:
        """Get all tables and views from MSSQL database"""
        try:
            query = f'''
                SELECT t.name as table_name, 
                       CASE WHEN t.type = 'V' THEN 'VIEW' ELSE 'TABLE' END as object_type
                FROM sys.objects t
                WHERE (t.type = 'U' OR t.type = 'V')
                  AND SCHEMA_NAME(t.schema_id) = ?
                ORDER BY t.name
            '''
            self.cursor.execute(query, (schema,))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting tables and views: {str(e)}")
    
    def get_table_analysis(self, schema: str, table: str) -> dict:
        """Get detailed analysis of a table including size, row count, and column information"""
        try:
            # Get table size and row count
            size_query = f'''
                SELECT 
                    SUM(p.rows) as row_count,
                    SUM(a.total_pages) * 8 as data_size_kb,
                    SUM(a.used_pages) * 8 as used_size_kb,
                    (SUM(a.total_pages) * 8) as total_size_kb,
                    (SUM(a.total_pages) * 8 * 1024) / NULLIF(SUM(p.rows), 0) as avg_row_width,
                    MAX(STATS_DATE(i.object_id, i.index_id)) as last_analyzed
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE t.name = ?
                AND SCHEMA_NAME(t.schema_id) = ?
                GROUP BY t.name
            '''
            self.cursor.execute(size_query, (table, schema))
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
            column_query = f'''
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                    c.max_length,
                    c.precision,
                    c.scale
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                WHERE tab.name = ?
                AND tab.schema_id = SCHEMA_ID(?)
                ORDER BY c.column_id
            '''
            self.cursor.execute(column_query, (table, schema))
            columns = self.cursor.fetchall()
            # Convert sizes to MB
            total_size = float(size_info[2]) / 1024 if size_info[2] else 0
            table_size = float(size_info[1]) / 1024 if size_info[1] else 0
            index_size = float(size_info[2] - size_info[1]) / 1024 if size_info[2] and size_info[1] else 0
            last_analyzed = size_info[5]
            if last_analyzed:
                try:
                    last_analyzed = last_analyzed.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass
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
            query = f'''
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                    c.max_length,
                    c.precision,
                    c.scale
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                WHERE tab.name = ?
                AND tab.schema_id = SCHEMA_ID(?)
                ORDER BY c.column_id
            '''
            self.cursor.execute(query, (table, schema))
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting columns: {str(e)}")
    
    def get_column_details(self, schema: str, table: str, column: str) -> dict:
        """Get detailed column analysis"""
        try:
            # Get basic column information
            query = f'''
                SELECT 
                    t.name as data_type,
                    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
                    c.max_length,
                    c.precision,
                    c.scale
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                WHERE tab.name = ?
                AND tab.schema_id = SCHEMA_ID(?)
                AND c.name = ?
            '''
            self.cursor.execute(query, (table, schema, column))
            col_info = self.cursor.fetchone()
            if not col_info:
                return {
                    'data_type': None,
                    'distinct_count': 0,
                    'null_count': 0,
                    'metrics': {}
                }
            # Get distinct and null counts
            count_query = f'''
                SELECT 
                    COUNT(DISTINCT [{column}]) as distinct_count,
                    SUM(CASE WHEN [{column}] IS NULL THEN 1 ELSE 0 END) as null_count
                FROM [{schema}].[{table}]
            '''
            self.cursor.execute(count_query)
            counts = self.cursor.fetchone()
            # Get unique count
            unique_count_query = f'''
                SELECT COUNT(*) FROM (
                    SELECT [{column}]
                    FROM [{schema}].[{table}]
                    GROUP BY [{column}]
                    HAVING COUNT(*) = 1
                ) AS unique_values
            '''
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]
            # Get type-specific metrics
            metrics = {}
            data_type = col_info[0].lower()
            if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'double', 'real', 'money', 'smallmoney']:
                metrics_query = f'''
                    SELECT 
                        MIN([{column}]) as min_val,
                        MAX([{column}]) as max_val,
                        AVG([{column}]) as avg_val,
                        STDEV([{column}]) as std_dev
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                '''
                self.cursor.execute(metrics_query)
                numeric_metrics = self.cursor.fetchone()
                if numeric_metrics:
                    metrics.update({
                        'min': numeric_metrics[0],
                        'max': numeric_metrics[1],
                        'avg': numeric_metrics[2],
                        'std_dev': numeric_metrics[3]
                    })
            elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                metrics_query = f'''
                    SELECT 
                        MIN(LEN([{column}])) as min_length,
                        MAX(LEN([{column}])) as max_length,
                        AVG(CAST(LEN([{column}]) AS FLOAT)) as avg_length
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                '''
                self.cursor.execute(metrics_query)
                string_metrics = self.cursor.fetchone()
                if string_metrics:
                    metrics.update({
                        'min_length': string_metrics[0],
                        'max_length': string_metrics[1],
                        'avg_length': string_metrics[2]
                    })
            elif data_type in ['date', 'datetime', 'datetime2', 'smalldatetime']:
                metrics_query = f'''
                    SELECT 
                        MIN([{column}]) as min_date,
                        MAX([{column}]) as max_date
                    FROM [{schema}].[{table}]
                    WHERE [{column}] IS NOT NULL
                '''
                self.cursor.execute(metrics_query)
                date_metrics = self.cursor.fetchone()
                if date_metrics:
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
            query = f"SELECT TOP {limit} * FROM [{schema}].[{table}]"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting sample data: {str(e)}")

    def get_value_counts(self, schema: str, table: str, column: str, limit: int = 100) -> list:
        """Get value counts for a column in MSSQL"""
        try:
            query = f'''
                SELECT [{column}], COUNT(*) as count
                FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL
                GROUP BY [{column}]
                ORDER BY count DESC
                OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error getting value counts: {str(e)}")

    def get_primary_keys(self, schema, table_name):
        self.cursor.execute('''
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
        ''', (schema, table_name))
        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute('''
        SELECT 
            fk_cols.COLUMN_NAME, 
            pk.TABLE_NAME AS REFERENCED_TABLE_NAME, 
            pk_cols.COLUMN_NAME AS REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_cols
            ON fk_cols.CONSTRAINT_CATALOG = rc.CONSTRAINT_CATALOG
            AND fk_cols.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            AND fk_cols.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_cols
            ON pk_cols.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG
            AND pk_cols.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
            AND pk_cols.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
            AND pk_cols.ORDINAL_POSITION = fk_cols.ORDINAL_POSITION
        INNER JOIN INFORMATION_SCHEMA.TABLES pk
            ON pk.TABLE_NAME = pk_cols.TABLE_NAME
        WHERE fk_cols.TABLE_SCHEMA = ? AND fk_cols.TABLE_NAME = ?
        ''', (schema, table_name))
        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}
    
    def get_null_count(self, schema, table, column):
        query = f'SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{column}] IS NULL'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_distinct_count(self, schema, table, column):
        query = f'SELECT COUNT(DISTINCT [{column}]) FROM [{schema}].[{table}]'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_null_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT TOP {limit} * FROM [{schema}].[{table}] WHERE [{column}] IS NULL'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching null violations: {str(e)}")

    def get_non_distinct_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL
                AND [{column}] IN (
                    SELECT [{column}]
                    FROM [{schema}].[{table}]
                    GROUP BY [{column}]
                    HAVING COUNT(*) > 1
                )
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching non-distinct violations: {str(e)}")
    
    def get_min_max_range(self, schema, table, column):
        try:
            query = f'SELECT MIN([{column}]), MAX([{column}]) FROM [{schema}].[{table}]'
            self.cursor.execute(query)
            min_val, max_val = self.cursor.fetchone()
            return {'min': min_val, 'max': max_val, 'range': max_val - min_val if min_val is not None and max_val is not None else None}
        except Exception as e:
            raise Exception(f"Error getting min-max range: {str(e)}")
        
    def get_char_length_range(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT MIN(LEN([{column}])), MAX(LEN([{column}]))
                FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL
            ''')
            min_len, max_len = self.cursor.fetchone()
            return {'min_length': min_len, 'max_length': max_len}
        except Exception as e:
            raise Exception(f"Error getting character length range: {str(e)}")
        
    def get_invalid_datetime_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE TRY_CONVERT(datetime, [{column}]) IS NULL AND [{column}] IS NOT NULL
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking datetime format: {str(e)}")

    def get_letter_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] LIKE '%[A-Za-z]%'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for letters: {str(e)}")
        
    def get_min_max_violations(self, schema, table, column, min_val, max_val, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] < {min_val} OR [{column}] > {max_val}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching min-max violations: {str(e)}")

    def get_char_length_violations(self, schema, table, column, min_len, max_len, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE LEN([{column}]) < {min_len} OR LEN([{column}]) > {max_len}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching character length violations: {str(e)}")

    def get_invalid_datetime_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE TRY_CONVERT(datetime, [{column}]) IS NULL AND [{column}] IS NOT NULL
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching invalid datetime values: {str(e)}")

    def get_letter_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] LIKE '%[A-Za-z]%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching letter violations: {str(e)}")

    def get_number_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] LIKE '%[0-9]%'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for numbers: {str(e)}")
        
    def get_allowed_values_violation_count(self, schema, table, column, allowed_values):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            total_query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}]
            '''
            violation_query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT IN ({formatted_values})
            '''
            non_violation_query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] IN ({formatted_values})
            '''
            self.cursor.execute(total_query)
            total = self.cursor.fetchone()[0]
            self.cursor.execute(violation_query)
            violation = self.cursor.fetchone()[0]
            self.cursor.execute(non_violation_query)
            non_violation = self.cursor.fetchone()[0]
            return {
                'total': total,
                'violation': violation,
                'non_violation': non_violation
            }
        except Exception as e:
            raise Exception(f"Error checking allowed values: {str(e)}")
        
    def get_eng_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND CONVERT(VARCHAR, [{column}]) LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking ENG format: {str(e)}")

    def get_tr_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND CONVERT(VARCHAR, [{column}]) NOT LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking TR format: {str(e)}")
        
    def get_number_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] LIKE '%[0-9]%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching number violations: {str(e)}")

    def get_allowed_values_violations(self, schema, table, column, allowed_values, limit=100):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT IN ({formatted_values})
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching allowed values violations: {str(e)}")

    def get_eng_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE CONVERT(VARCHAR, [{column}]) LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching ENG numeric format violations: {str(e)}")

    def get_tr_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE CONVERT(VARCHAR, [{column}]) NOT LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching TR numeric format violations: {str(e)}")

        
    def get_case_inconsistency_count(self, schema, table, column, expected_case):
        try:
            if expected_case == 'upper':
                condition = f'[{column}] COLLATE Latin1_General_CS_AS != UPPER([{column}])'
            elif expected_case == 'lower':
                condition = f'[{column}] COLLATE Latin1_General_CS_AS != LOWER([{column}])'
            else:
                raise ValueError("Unsupported case type")

            query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND {condition}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking case consistency: {str(e)}")
        
    def get_future_date_violation_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] > GETDATE()
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking future dates: {str(e)}")

    def get_date_range_violation_count(self, schema, table, column, start_date, end_date):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] < '{start_date}' OR [{column}] > '{end_date}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking date range: {str(e)}")

    def get_special_char_violation_count(self, schema, table, column, allowed_pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '{allowed_pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking special characters: {str(e)}")
        
    def get_case_inconsistency_violations(self, schema, table, column, expected_case, limit=100):
        try:
            if expected_case == 'upper':
                condition = f'[{column}] COLLATE Latin1_General_CS_AS != UPPER([{column}])'
            elif expected_case == 'lower':
                condition = f'[{column}] COLLATE Latin1_General_CS_AS != LOWER([{column}])'
            else:
                raise ValueError("Unsupported case type")

            query = f'SELECT TOP {limit} * FROM [{schema}].[{table}] WHERE {condition}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching case inconsistency violations: {str(e)}")

    def get_future_date_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT TOP {limit} * FROM [{schema}].[{table}] WHERE [{column}] > GETDATE()'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching future date violations: {str(e)}")

    def get_date_range_violations(self, schema, table, column, start_date, end_date, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] < '{start_date}' OR [{column}] > '{end_date}'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching date range violations: {str(e)}")

    def get_special_char_violations(self, schema, table, column, allowed_pattern, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '{allowed_pattern}'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching special character violations: {str(e)}")
        
    def get_email_format_violation_count(self, schema, table, column):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '%@%.%'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking email format: {str(e)}")

    def get_regex_pattern_violation_count(self, schema, table, column, pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '{pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking regex pattern: {str(e)}")

    def get_positive_value_violation_count(self, schema, table, column, strict):
        try:
            operator = '>' if strict else '>='
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND NOT ([{column}] {operator} 0)
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking positive values: {str(e)}")
        
    def get_email_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '%@%.%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching email format violations: {str(e)}")

    def get_regex_pattern_violations(self, schema, table, column, pattern, limit=100):
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND [{column}] NOT LIKE '{pattern}'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching regex pattern violations: {str(e)}")

    def get_positive_value_violations(self, schema, table, column, strict, limit=100):
        try:
            operator = '>' if strict else '>='
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL AND NOT ([{column}] {operator} 0)
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching positive value violations: {str(e)}")

    def get_tckn_violation_count(self, schema, table, column):
        """Count invalid TCKN values (MSSQL)"""
        try:
            query = f'''
                SELECT COUNT(*) FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL
                  AND NOT (
                    LEN([{column}]) = 11
                    AND [{column}] NOT LIKE '0%'
                    AND [{column}] LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                    AND (
                        (
                            (
                                CAST(SUBSTRING([{column}],1,1) AS int) +
                                CAST(SUBSTRING([{column}],3,1) AS int) +
                                CAST(SUBSTRING([{column}],5,1) AS int) +
                                CAST(SUBSTRING([{column}],7,1) AS int) +
                                CAST(SUBSTRING([{column}],9,1) AS int)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING([{column}],2,1) AS int) +
                                CAST(SUBSTRING([{column}],4,1) AS int) +
                                CAST(SUBSTRING([{column}],6,1) AS int) +
                                CAST(SUBSTRING([{column}],8,1) AS int)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING([{column}],10,1) AS int)
                    AND (
                        (
                            CAST(SUBSTRING([{column}],1,1) AS int) +
                            CAST(SUBSTRING([{column}],2,1) AS int) +
                            CAST(SUBSTRING([{column}],3,1) AS int) +
                            CAST(SUBSTRING([{column}],4,1) AS int) +
                            CAST(SUBSTRING([{column}],5,1) AS int) +
                            CAST(SUBSTRING([{column}],6,1) AS int) +
                            CAST(SUBSTRING([{column}],7,1) AS int) +
                            CAST(SUBSTRING([{column}],8,1) AS int) +
                            CAST(SUBSTRING([{column}],9,1) AS int) +
                            CAST(SUBSTRING([{column}],10,1) AS int)
                        ) % 10
                    ) = CAST(SUBSTRING([{column}],11,1) AS int)
                  )
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"MSSQL TCKN violation count error: {str(e)}")

    def get_tckn_violations(self, schema, table, column, limit=100):
        """Get invalid TCKN rows (MSSQL)"""
        try:
            query = f'''
                SELECT TOP {limit} * FROM [{schema}].[{table}]
                WHERE [{column}] IS NOT NULL
                  AND NOT (
                    LEN([{column}]) = 11
                    AND [{column}] NOT LIKE '0%'
                    AND [{column}] LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                    AND (
                        (
                            (
                                CAST(SUBSTRING([{column}],1,1) AS int) +
                                CAST(SUBSTRING([{column}],3,1) AS int) +
                                CAST(SUBSTRING([{column}],5,1) AS int) +
                                CAST(SUBSTRING([{column}],7,1) AS int) +
                                CAST(SUBSTRING([{column}],9,1) AS int)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING([{column}],2,1) AS int) +
                                CAST(SUBSTRING([{column}],4,1) AS int) +
                                CAST(SUBSTRING([{column}],6,1) AS int) +
                                CAST(SUBSTRING([{column}],8,1) AS int)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING([{column}],10,1) AS int)
                    AND (
                        (
                            CAST(SUBSTRING([{column}],1,1) AS int) +
                            CAST(SUBSTRING([{column}],2,1) AS int) +
                            CAST(SUBSTRING([{column}],3,1) AS int) +
                            CAST(SUBSTRING([{column}],4,1) AS int) +
                            CAST(SUBSTRING([{column}],5,1) AS int) +
                            CAST(SUBSTRING([{column}],6,1) AS int) +
                            CAST(SUBSTRING([{column}],7,1) AS int) +
                            CAST(SUBSTRING([{column}],8,1) AS int) +
                            CAST(SUBSTRING([{column}],9,1) AS int) +
                            CAST(SUBSTRING([{column}],10,1) AS int)
                        ) % 10
                    ) = CAST(SUBSTRING([{column}],11,1) AS int)
                  )
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"MSSQL get TCKN violations error: {str(e)}")

    def get_date_logic_violation_count(self, schema, table, start_date_col, end_date_col):
        """MSSQL: Count rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT COUNT(*) 
                FROM [{schema}].[{table}]
                WHERE [{start_date_col}] IS NOT NULL
                AND [{end_date_col}] IS NOT NULL
                AND [{start_date_col}] >= [{end_date_col}]
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"MSSQL error counting date logic violations: {str(e)}")

    def get_date_logic_violations(self, schema, table, start_date_col, end_date_col, limit=100):
        """MSSQL: Get sample rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT TOP {limit} * 
                FROM [{schema}].[{table}]
                WHERE [{start_date_col}] IS NOT NULL
                AND [{end_date_col}] IS NOT NULL
                AND [{start_date_col}] >= [{end_date_col}]
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"MSSQL error fetching date logic violations: {str(e)}")
        
    def get_text_column_date_formats_mssql(self, schema, table, column_name, limit=1000):
        try:
            query = f"""
                SELECT TOP {limit} *, 

                    CASE
                        WHEN [{column_name}] LIKE '[0-3][0-9].[0-1][0-9].[1-2][0-9][0-9][0-9]' THEN 'DD.MM.YYYY'
                        WHEN [{column_name}] LIKE '[1-2][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]' THEN 'YYYY-MM-DD'
                        WHEN [{column_name}] LIKE '[0-1][0-9]/[0-3][0-9]/[1-2][0-9][0-9][0-9]' THEN 'MM/DD/YYYY'
                        WHEN [{column_name}] LIKE '[0-3][0-9]/[0-1][0-9]/[1-2][0-9][0-9][0-9]' THEN 'DD/MM/YYYY'
                        WHEN [{column_name}] LIKE '[1-2][0-9][0-9][0-9].[0-1][0-9].[0-3][0-9]' THEN 'YYYY.MM.DD'
                        ELSE 'Unknown'
                    END AS format,

                    CASE
                        WHEN TRY_CONVERT(DATE, [{column_name}], 104) IS NOT NULL THEN 1  -- DD.MM.YYYY
                        WHEN TRY_CONVERT(DATE, [{column_name}], 120) IS NOT NULL THEN 1  -- YYYY-MM-DD
                        WHEN TRY_CONVERT(DATE, [{column_name}], 101) IS NOT NULL THEN 1  -- MM/DD/YYYY
                        WHEN TRY_CONVERT(DATE, [{column_name}], 103) IS NOT NULL THEN 1  -- DD/MM/YYYY
                        WHEN TRY_CONVERT(DATE, [{column_name}], 102) IS NOT NULL THEN 1  -- YYYY.MM.DD (fallback)
                        ELSE 0
                    END AS is_valid,

                    COALESCE(
                        TRY_CONVERT(DATE, [{column_name}], 104),
                        TRY_CONVERT(DATE, [{column_name}], 120),
                        TRY_CONVERT(DATE, [{column_name}], 101),
                        TRY_CONVERT(DATE, [{column_name}], 103),
                        TRY_CONVERT(DATE, [{column_name}], 102)
                    ) AS parsed_date

                FROM [{schema}].[{table}]
                WHERE [{column_name}] IS NOT NULL;
            """
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            raise Exception(f"MSSQL error fetching date formats: {str(e)}")

        





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
        """Close MySQL connection safely"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            logger.warning(f"MySQL cursor close error: {e}")

        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
        except Exception as e:
            logger.warning(f"MySQL connection close error: {e}")

    def ensure_connected(self, config: dict):
        try:
            # mysql-connector supports ping with auto-reconnect
            self.cursor.execute("SELECT 1")
        except:
            # ping might not exist or connection is broken
            self.connect(config)

    
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
            raise Exception(f"Error getting column details: {str(e)}")
    
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
    
    def get_null_count(self, schema, table, column):
        query = f'SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE `{column}` IS NULL'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_distinct_count(self, schema, table, column):
        query = f'SELECT COUNT(DISTINCT `{column}`) FROM `{schema}`.`{table}`'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_null_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT * FROM `{schema}`.`{table}` WHERE `{column}` IS NULL LIMIT {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching null violations: {str(e)}")

    def get_non_distinct_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL
                AND `{column}` IN (
                    SELECT `{column}`
                    FROM `{schema}`.`{table}`
                    GROUP BY `{column}`
                    HAVING COUNT(*) > 1
                )
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching non-distinct violations: {str(e)}")
    
    def get_min_max_range(self, schema, table, column):
        try:
            query = f'SELECT MIN(`{column}`), MAX(`{column}`) FROM `{schema}`.`{table}`'
            self.cursor.execute(query)
            min_val, max_val = self.cursor.fetchone()
            return {'min': min_val, 'max': max_val, 'range': max_val - min_val if min_val is not None and max_val is not None else None}
        except Exception as e:
            raise Exception(f"Error getting min-max range: {str(e)}")
    
    def get_char_length_range(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT MIN(CHAR_LENGTH(`{column}`)), MAX(CHAR_LENGTH(`{column}`))
                FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL
            ''')
            min_len, max_len = self.cursor.fetchone()
            return {'min_length': min_len, 'max_length': max_len}
        except Exception as e:
            raise Exception(f"Error getting character length range: {str(e)}")
        
    def get_invalid_datetime_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE STR_TO_DATE(`{column}`, '%Y-%m-%d %H:%i:%s') IS NULL AND `{column}` IS NOT NULL
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking datetime format: {str(e)}")
        
    def get_letter_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` REGEXP '[A-Za-z]'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for letters: {str(e)}")
        
    def get_min_max_violations(self, schema, table, column, min_val, max_val, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` < {min_val} OR `{column}` > {max_val}
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching min-max violations: {str(e)}")

    def get_char_length_violations(self, schema, table, column, min_len, max_len, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE CHAR_LENGTH(`{column}`) < {min_len} OR CHAR_LENGTH(`{column}`) > {max_len}
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching character length violations: {str(e)}")

    def get_invalid_datetime_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE STR_TO_DATE(`{column}`, '%Y-%m-%d %H:%i:%s') IS NULL AND `{column}` IS NOT NULL
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching invalid datetime values: {str(e)}")

    def get_letter_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` REGEXP '[A-Za-z]'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching letter violations: {str(e)}")

    def get_number_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` REGEXP '[0-9]'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for numbers: {str(e)}")
        
    def get_allowed_values_violation_count(self, schema, table, column, allowed_values):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            total_query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}`
            '''
            violation_query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT IN ({formatted_values})
            '''
            non_violation_query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` IN ({formatted_values})
            '''
            self.cursor.execute(total_query)
            total = self.cursor.fetchone()[0]
            self.cursor.execute(violation_query)
            violation = self.cursor.fetchone()[0]
            self.cursor.execute(non_violation_query)
            non_violation = self.cursor.fetchone()[0]
            return {
                'total': total,
                'violation': violation,
                'non_violation': non_violation
            }
        except Exception as e:
            raise Exception(f"Error checking allowed values: {str(e)}")
        
    def get_eng_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking ENG format: {str(e)}")

    def get_tr_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT LIKE '%,%'
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking TR format: {str(e)}")
        
    def get_number_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` REGEXP '[0-9]'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching number violations: {str(e)}")

    def get_allowed_values_violations(self, schema, table, column, allowed_values, limit=100):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT IN ({formatted_values})
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching allowed values violations: {str(e)}")

    def get_eng_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` LIKE '%,%'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching ENG numeric format violations: {str(e)}")

    def get_tr_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT LIKE '%,%'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching TR numeric format violations: {str(e)}")

        
    def get_case_inconsistency_count(self, schema, table, column, expected_case):
        try:
            if expected_case == 'upper':
                condition = f'BINARY `{column}` != BINARY UPPER(`{column}`)'
            elif expected_case == 'lower':
                condition = f'BINARY `{column}` != BINARY LOWER(`{column}`)'
            else:
                raise ValueError("Unsupported case type")

            query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND {condition}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking case consistency: {str(e)}")
        
    def get_future_date_violation_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` > CURDATE()
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking future dates: {str(e)}")

    def get_date_range_violation_count(self, schema, table, column, start_date, end_date):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` < '{start_date}' OR `{column}` > '{end_date}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking date range: {str(e)}")

    def get_special_char_violation_count(self, schema, table, column, allowed_pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{allowed_pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking special characters: {str(e)}")
        
    def get_case_inconsistency_violations(self, schema, table, column, expected_case, limit=100):
        try:
            if expected_case == 'upper':
                condition = f'BINARY `{column}` != BINARY UPPER(`{column}`)'
            elif expected_case == 'lower':
                condition = f'BINARY `{column}` != BINARY LOWER(`{column}`)'
            else:
                raise ValueError("Unsupported case type")

            query = f'SELECT * FROM `{schema}`.`{table}` WHERE {condition} LIMIT {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching case inconsistency violations: {str(e)}")

    def get_future_date_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT * FROM `{schema}`.`{table}` WHERE `{column}` > CURDATE() LIMIT {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching future date violations: {str(e)}")

    def get_date_range_violations(self, schema, table, column, start_date, end_date, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` < '{start_date}' OR `{column}` > '{end_date}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching date range violations: {str(e)}")

    def get_special_char_violations(self, schema, table, column, allowed_pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{allowed_pattern}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching special character violations: {str(e)}")
        
    def get_email_format_violation_count(self, schema, table, column):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{regex}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking email format: {str(e)}")

    def get_regex_pattern_violation_count(self, schema, table, column, pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{pattern}'
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking regex pattern: {str(e)}")

    def get_positive_value_violation_count(self, schema, table, column, strict):
        try:
            operator = '>' if strict else '>='
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND NOT (`{column}` {operator} 0)
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking positive values: {str(e)}")
        
    def get_email_format_violations(self, schema, table, column, limit=100):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{regex}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching email format violations: {str(e)}")

    def get_regex_pattern_violations(self, schema, table, column, pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND `{column}` NOT REGEXP '{pattern}'
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching regex pattern violations: {str(e)}")

    def get_positive_value_violations(self, schema, table, column, strict, limit=100):
        try:
            operator = '>' if strict else '>='
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL AND NOT (`{column}` {operator} 0)
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching positive value violations: {str(e)}")

    def get_tckn_violation_count(self, schema, table, column):
        """Count invalid TCKN values (MySQL)"""
        try:
            query = f'''
                SELECT COUNT(*) FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL
                  AND NOT (
                    CHAR_LENGTH(`{column}`) = 11
                    AND `{column}` REGEXP '^[0-9]+$'
                    AND SUBSTRING(`{column}`, 1, 1) <> '0'
                    AND (
                        (
                            (
                                CAST(SUBSTRING(`{column}`,1,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,3,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,5,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,7,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,9,1) AS UNSIGNED)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING(`{column}`,2,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,4,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,6,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,8,1) AS UNSIGNED)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING(`{column}`,10,1) AS UNSIGNED)
                    AND (
                        (
                            CAST(SUBSTRING(`{column}`,1,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,2,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,3,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,4,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,5,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,6,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,7,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,8,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,9,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,10,1) AS UNSIGNED)
                        ) % 10
                    ) = CAST(SUBSTRING(`{column}`,11,1) AS UNSIGNED)
                  )
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"MySQL TCKN violation count error: {str(e)}")

    def get_tckn_violations(self, schema, table, column, limit=100):
        """Get invalid TCKN rows (MySQL)"""
        try:
            query = f'''
                SELECT * FROM `{schema}`.`{table}`
                WHERE `{column}` IS NOT NULL
                  AND NOT (
                    CHAR_LENGTH(`{column}`) = 11
                    AND `{column}` REGEXP '^[0-9]+$'
                    AND SUBSTRING(`{column}`, 1, 1) <> '0'
                    AND (
                        (
                            (
                                CAST(SUBSTRING(`{column}`,1,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,3,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,5,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,7,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,9,1) AS UNSIGNED)
                            ) * 7
                            -
                            (
                                CAST(SUBSTRING(`{column}`,2,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,4,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,6,1) AS UNSIGNED) +
                                CAST(SUBSTRING(`{column}`,8,1) AS UNSIGNED)
                            )
                        ) % 10
                    ) = CAST(SUBSTRING(`{column}`,10,1) AS UNSIGNED)
                    AND (
                        (
                            CAST(SUBSTRING(`{column}`,1,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,2,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,3,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,4,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,5,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,6,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,7,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,8,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,9,1) AS UNSIGNED) +
                            CAST(SUBSTRING(`{column}`,10,1) AS UNSIGNED)
                        ) % 10
                    ) = CAST(SUBSTRING(`{column}`,11,1) AS UNSIGNED)
                  )
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"MySQL get TCKN violations error: {str(e)}")

    def get_date_logic_violation_count(self, schema, table, start_date_col, end_date_col):
        """MySQL: Count rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT COUNT(*) 
                FROM `{schema}`.`{table}`
                WHERE `{start_date_col}` IS NOT NULL
                AND `{end_date_col}` IS NOT NULL
                AND `{start_date_col}` >= `{end_date_col}`
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"MySQL error counting date logic violations: {str(e)}")

    def get_date_logic_violations(self, schema, table, start_date_col, end_date_col, limit=100):
        """MySQL: Get sample rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT * 
                FROM `{schema}`.`{table}`
                WHERE `{start_date_col}` IS NOT NULL
                AND `{end_date_col}` IS NOT NULL
                AND `{start_date_col}` >= `{end_date_col}`
                LIMIT {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"MySQL error fetching date logic violations: {str(e)}")
        
    def get_text_column_date_formats_mysql(self, schema, table, column_name, limit=1000):
        try:
            query = f"""
                SELECT *, 

                    CASE
                        WHEN `{column_name}` REGEXP '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$' THEN 'DD.MM.YYYY'
                        WHEN `{column_name}` REGEXP '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$' THEN 'YYYY-MM-DD'
                        WHEN `{column_name}` REGEXP '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$' THEN 'MM/DD/YYYY'
                        WHEN `{column_name}` REGEXP '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$' THEN 'DD/MM/YYYY'
                        WHEN `{column_name}` REGEXP '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$' THEN 'YYYY.MM.DD'
                        ELSE 'Unknown'
                    END AS format,

                    CASE
                        WHEN STR_TO_DATE({column_name}, '%d.%m.%Y') IS NOT NULL THEN 1
                        WHEN STR_TO_DATE({column_name}, '%Y-%m-%d') IS NOT NULL THEN 1
                        WHEN STR_TO_DATE({column_name}, '%m/%d/%Y') IS NOT NULL THEN 1
                        WHEN STR_TO_DATE({column_name}, '%d/%m/%Y') IS NOT NULL THEN 1
                        WHEN STR_TO_DATE({column_name}, '%Y.%m.%d') IS NOT NULL THEN 1
                        ELSE 0
                    END AS is_valid,

                    COALESCE(
                        STR_TO_DATE({column_name}, '%d.%m.%Y'),
                        STR_TO_DATE({column_name}, '%Y-%m-%d'),
                        STR_TO_DATE({column_name}, '%m/%d/%Y'),
                        STR_TO_DATE({column_name}, '%d/%m/%Y'),
                        STR_TO_DATE({column_name}, '%Y.%m.%d')
                    ) AS parsed_date

                FROM `{schema}`.`{table}`
                WHERE `{column_name}` IS NOT NULL
                LIMIT {limit};
            """
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            raise Exception(f"MySQL error fetching date formats: {str(e)}")


    

import logging

# Configure logging at the top of your module
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class OracleConnector(DatabaseConnector):
    """Oracle database connector implementation"""

    def connect(self, config: dict) -> None:
        """Connect to Oracle database"""
        try:
            import oracledb
            dsn = f"{config.get('host')}:{config.get('port')}/{config.get('dbname')}"
            logger.debug(f"Connecting to Oracle with DSN: {dsn}, User: {config.get('user')}")
            self.connection = oracledb.connect(
                user=config.get('user'),
                password=config.get('password'),
                dsn=dsn
            )
            self.cursor = self.connection.cursor()
            logger.info("Oracle connection established successfully.")
        except Exception as e:
            logger.exception("Error connecting to Oracle")
            raise Exception(f"Error connecting to Oracle: {str(e)}")

    def close(self):
        """Safely close Oracle connection and cursor"""
        import oracledb

        # Close cursor
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
                logger.debug("Cursor closed.")
        except oracledb.InterfaceError:
            logger.warning("Cursor already closed.")
        except Exception as e:
            logger.warning(f"Unexpected error closing cursor: {e}")

        # Close connection
        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
                logger.debug("Connection closed.")
        except oracledb.InterfaceError as e:
            if "DPY-1001" in str(e):
                logger.warning("Connection already disconnected.")
            else:
                logger.warning(f"InterfaceError closing connection: {e}")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")



    def ensure_connected(self, config: dict):
        try:
            self.cursor.execute("SELECT 1 FROM DUAL")
        except:
            self.connect(config)



    def get_all_tables_and_views(self, schema: str) -> list:
        """Get all tables and views from Oracle database"""
        try:
            logger.debug(f"Fetching all tables and views for schema: {schema}")
            query = """
                SELECT table_name, 'TABLE' as object_type 
                FROM all_tables 
                WHERE owner = :schema
                UNION ALL
                SELECT view_name as table_name, 'VIEW' as object_type 
                FROM all_views 
                WHERE owner = :schema
                ORDER BY table_name
            """

            self.cursor.execute(query, {"schema": schema.upper()})

            results = self.cursor.fetchall()
            logger.debug(f"Fetched {len(results)} objects from schema {schema}")
            return results
        except Exception as e:
            logger.exception("Error getting tables and views")
            raise Exception(f"Error getting tables and views: {str(e)}")

    def get_table_analysis(self, schema: str, table: str) -> dict:
        """Get detailed analysis of a table including size, row count, and column information"""
        try:
            # Get row count
            row_count_query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            self.cursor.execute(row_count_query)
            row_count = self.cursor.fetchone()[0]
            logger.debug(f"Row count for {schema}.{table}: {row_count}")

            # Get table and index size (in MB)
            size_query = f'''
                SELECT NVL(SUM(bytes),0)/1024/1024 AS total_size_mb
                FROM dba_segments
                WHERE owner = '{schema}' AND segment_name = '{table}' AND segment_type = 'TABLE'
            '''
            logger.debug(f"size Query: {size_query}")
            logger.debug(f"Params: schema={schema}, table={table}")

            self.cursor.execute(size_query)
            total_size = self.cursor.fetchone()[0] or 0

            index_size_query = f'''
                SELECT NVL(SUM(bytes),0)/1024/1024 AS index_size_mb
                FROM dba_segments
                WHERE owner = '{schema}' AND segment_name = '{table}' AND segment_type = 'INDEX'
            '''

            logger.debug(f"index Query: {index_size_query}")
            logger.debug(f"Params: schema={schema}, table={table}")

            self.cursor.execute(index_size_query)
            index_size = self.cursor.fetchone()[0] or 0

            # For Oracle, table_size is total_size minus index_size
            table_size = total_size - index_size if total_size and index_size else total_size

            # Get average row length
            avg_row_query = f'''
                SELECT AVG_ROW_LEN FROM dba_tables WHERE owner = '{schema}' AND table_name = '{table}'
            '''
            logger.debug(f"avgrow Query: {avg_row_query}")
            logger.debug(f"Params: schema='{schema}', table='{table}'")

            self.cursor.execute(avg_row_query)
            avg_row = self.cursor.fetchone()
            avg_row_width = avg_row[0] if avg_row is not None else 0

            logger.debug(avg_row_width)


            # Get last analyzed date
            last_analyzed_query = f'''
                SELECT LAST_ANALYZED FROM dba_tables WHERE owner = '{schema}' AND table_name = '{table}'
            '''
            logger.debug(f"last Query: {last_analyzed_query}")
            logger.debug(f"Params: schema='{schema}', table='{table}'")

            self.cursor.execute(last_analyzed_query)
            last_analyzed = self.cursor.fetchone()
            last_analyzed = last_analyzed[0] if last_analyzed else None
            if last_analyzed:
                try:
                    last_analyzed = last_analyzed.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    last_analyzed = str(last_analyzed)

            # Get column information
            column_query = f'''
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = '{schema}' AND table_name = '{table}'
                ORDER BY column_id
            '''
            logger.debug(f"column Query: {column_query}")
            logger.debug(f"Params: schema='{schema}', table='{table}'")

            self.cursor.execute(column_query)
            columns = self.cursor.fetchall()
            logger.debug(f"columns: {columns}")

            return {
                'row_count': row_count,
                'total_size': round(total_size, 2),
                'table_size': round(table_size, 2),
                'index_size': round(index_size, 2),
                'avg_row_width': avg_row_width,
                'last_analyzed': last_analyzed,
                'columns': columns
            }


        except Exception as e:
            raise Exception(f"Error getting table analysis: {str(e)}")

    def safe_lob_to_str(val):
        try:
            return str(val.read()) if hasattr(val, 'read') else val
        except Exception:
            return str(val) if val is not None else None

    def get_columns(self, schema: str, table: str) -> list:
        """Get column information for a table"""
        try:
            logger.debug(f"Getting columns for table: {schema}.{table}")
            query = f'''
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = '{schema}' AND table_name = '{table}'
                ORDER BY column_id
            '''
            self.cursor.execute(query)
            columns = self.cursor.fetchall()
            logger.debug(f"Fetched {len(columns)} columns from {schema}.{table}")
            return columns
        except Exception as e:
            logger.exception(f"Error getting columns for {schema}.{table}")
            raise Exception(f"Error getting columns: {str(e)}")


    import logging

    logger = logging.getLogger(__name__)

    def get_column_details(self, schema: str, table: str, column: str) -> dict:
        """Get detailed column analysis"""
        try:
            logger.debug(f"Analyzing column: {schema}.{table}.{column}")

            # Get column data type and metadata
            col_info_query = f'''
                SELECT data_type, nullable, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = '{schema}' AND table_name = '{table}' AND column_name = '{column}'
            '''
            logger.debug(f"Column info query:\n{col_info_query}")
            self.cursor.execute(col_info_query)
            col_info = self.cursor.fetchone()
            logger.debug(f"Column info result: {col_info}")

            if not col_info:
                logger.warning(f"Column {schema}.{table}.{column} not found.")
                return {
                    'data_type': None,
                    'distinct_count': 0,
                    'null_count': 0,
                    'unique_count': 0,
                    'metrics': {}
                }

            data_type = col_info[0].lower()
            if data_type == "clob":
                logger.warning(f"Skipping CLOB column: {schema}.{table}.{column}")
                return {
                    'data_type': data_type,
                    'distinct_count': 0,
                    'null_count': 0,
                    'unique_count': 0,
                    'metrics': {}
                }

            # Get distinct and null counts
            count_query = f'''
                SELECT 
                    COUNT(DISTINCT "{column}") AS distinct_count, 
                    SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) AS null_count 
                FROM "{schema}"."{table}"
            '''
            logger.debug(f"Count query:\n{count_query}")
            self.cursor.execute(count_query)
            counts = self.cursor.fetchone()
            logger.debug(f"Distinct/null count result: {counts}")

            # Get unique count
            unique_count_query = f'''
                SELECT COUNT(*) FROM (
                    SELECT "{column}" 
                    FROM "{schema}"."{table}" 
                    GROUP BY "{column}" 
                    HAVING COUNT(*) = 1
                )
            '''
            logger.debug(f"Unique count query:\n{unique_count_query}")
            self.cursor.execute(unique_count_query)
            unique_count = self.cursor.fetchone()[0]
            logger.debug(f"Unique count result: {unique_count}")

            metrics = {}

            if data_type in ['number', 'float', 'integer', 'decimal']:
                metrics_query = f'''
                    SELECT 
                        MIN("{column}"), 
                        MAX("{column}"), 
                        AVG("{column}") 
                    FROM "{schema}"."{table}" 
                    WHERE "{column}" IS NOT NULL
                '''
                logger.debug(f"Numeric metrics query:\n{metrics_query}")
                self.cursor.execute(metrics_query)
                min_val, max_val, avg_val = self.cursor.fetchone()
                logger.debug(f"Numeric metrics: min={min_val}, max={max_val}, avg={avg_val}")
                metrics.update({'min': min_val, 'max': max_val, 'avg': avg_val})

            elif data_type in ['varchar2', 'char', 'nvarchar2', 'nchar', 'clob']:
                metrics_query = f'''
                    SELECT 
                        MIN(LENGTH("{column}")), 
                        MAX(LENGTH("{column}")), 
                        AVG(LENGTH("{column}")) 
                    FROM "{schema}"."{table}" 
                    WHERE "{column}" IS NOT NULL
                '''
                logger.debug(f"String length metrics query:\n{metrics_query}")
                self.cursor.execute(metrics_query)
                min_length, max_length, avg_length = self.cursor.fetchone()
                logger.debug(f"String length metrics: min={min_length}, max={max_length}, avg={avg_length}")
                metrics.update({'min_length': min_length, 'max_length': max_length, 'avg_length': avg_length})

            elif data_type in ['date', 'timestamp']:
                metrics_query = f'''
                    SELECT 
                        MIN("{column}"), 
                        MAX("{column}") 
                    FROM "{schema}"."{table}" 
                    WHERE "{column}" IS NOT NULL
                '''
                logger.debug(f"Date metrics query:\n{metrics_query}")
                self.cursor.execute(metrics_query)
                min_date, max_date = self.cursor.fetchone()
                logger.debug(f"Date metrics: min_date={min_date}, max_date={max_date}")
                metrics.update({
                    'min_date': str(min_date) if min_date else None,
                    'max_date': str(max_date) if max_date else None
                })

            return {
                'data_type': col_info[0],
                'distinct_count': counts[0] if counts else 0,
                'null_count': counts[1] if counts else 0,
                'unique_count': unique_count,
                'metrics': metrics
            }

        except Exception as e:
            logger.exception(f"Error getting column details for {schema}.{table}.{column}")
            raise Exception(f"Error getting column details: {str(e)}")
        
    def get_value_counts(self, schema: str, table: str, column: str) -> list:
        """Get value counts for a column in Oracle"""
        try:
            query = f'''
                SELECT "{column}", COUNT(*) AS count
                FROM "{schema}"."{table}"
                GROUP BY "{column}"
                ORDER BY count DESC

            '''
            logger.debug(f"Value counts query:\n{query}")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            logger.debug(f"Fetched {len(results)} value counts for {schema}.{table}.{column}")
            return results
        except Exception as e:
            logger.exception(f"Error getting value counts for {schema}.{table}.{column}")
            raise Exception(f"Error getting value counts: {str(e)}")


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
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
            WHERE cons.constraint_type = 'P'
              AND cons.owner = :schema_name
              AND cons.table_name = :table_name
        """, {"schema_name": schema, "table_name": table_name})

        return [row[0] for row in self.cursor.fetchall()]

    def get_foreign_keys(self, schema, table_name):
        self.cursor.execute("""
            SELECT
                acc.column_name,
                rcons.table_name AS referenced_table,
                racc.column_name AS referenced_column
            FROM all_constraints cons
            JOIN all_cons_columns acc ON cons.constraint_name = acc.constraint_name AND cons.owner = acc.owner
            JOIN all_constraints rcons ON cons.r_constraint_name = rcons.constraint_name AND cons.owner = rcons.owner
            JOIN all_cons_columns racc ON rcons.constraint_name = racc.constraint_name AND rcons.owner = racc.owner AND acc.position = racc.position
            WHERE cons.constraint_type = 'R'
              AND cons.owner = :schema_name
              AND cons.table_name = :table_name
        """, {"schema_name": schema.upper(), "table_name": table_name.upper()})

        return {row[0]: (row[1], row[2]) for row in self.cursor.fetchall()}

    def get_null_count(self, schema, table, column):
        query = f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{column}" IS NULL'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_distinct_count(self, schema, table, column):
        query = f'SELECT COUNT(DISTINCT \"{column}\") FROM \"{schema}\".\"{table}\"'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_null_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT * FROM "{schema}"."{table}" WHERE "{column}" IS NULL AND ROWNUM <= {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching null violations: {str(e)}")

    def get_non_distinct_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                AND "{column}" IN (
                    SELECT "{column}"
                    FROM "{schema}"."{table}"
                    GROUP BY "{column}"
                    HAVING COUNT(*) > 1
                ) AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching non-distinct violations: {str(e)}")
    
    def get_min_max_range(self, schema, table, column):
        try:
            query = f'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}"'
            self.cursor.execute(query)
            min_val, max_val = self.cursor.fetchone()
            return {'min': min_val, 'max': max_val, 'range': max_val - min_val if min_val is not None and max_val is not None else None}
        except Exception as e:
            raise Exception(f"Error getting min-max range: {str(e)}")
    
    def get_char_length_range(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT MIN(LENGTH("{column}")), MAX(LENGTH("{column}"))
                FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
            ''')
            min_len, max_len = self.cursor.fetchone()
            return {'min_length': min_len, 'max_length': max_len}
        except Exception as e:
            raise Exception(f"Error getting character length range: {str(e)}")
        
    def get_invalid_datetime_count(self, schema, table, column, datetime_check_format, datetime_check_regex=r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'):
        try:
            query = f'''
                 SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                AND REGEXP_LIKE(TO_CHAR("{column}", '{datetime_check_format}'), '{datetime_check_regex}')
            '''
            print("Executing SQL datetime format check using regex:")
            print("Format:", datetime_check_format)
            print("Regex :", datetime_check_format)
            print("Query :", query)

            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking datetime format via regex: {str(e)}")

            
    def get_letter_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[A-Za-z]')
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for letters: {str(e)}")
        
    def get_min_max_violations(self, schema, table, column, min_val, max_val, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" < {min_val} OR "{column}" > {max_val} AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching min-max violations: {str(e)}")

    def get_char_length_violations(self, schema, table, column, min_len, max_len, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE LENGTH("{column}") < {min_len} OR LENGTH("{column}") > {max_len} AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching character length violations: {str(e)}")

    def get_invalid_datetime_violations(self, schema, table, column, limit=100, datetime_check_format='YYYY-MM-DD HH24:MI:SS.FF3', datetime_check_regex=r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT REGEXP_LIKE(TO_CHAR("{column}", '{datetime_check_format}'), '{datetime_check_regex}')
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching invalid datetime values: {str(e)}")

    def get_letter_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[A-Za-z]') AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching letter violations: {str(e)}")


    def get_number_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[0-9]')
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking for numbers: {str(e)}")
    def get_allowed_values_violation_count(self, schema, table, column, allowed_values):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            total_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({formatted_values})
            '''
            violation_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({formatted_values})
            '''
            non_violation_query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" IN ({formatted_values})
            '''
            self.cursor.execute(total_query)
            total = self.cursor.fetchone()[0]
            self.cursor.execute(violation_query)
            violation = self.cursor.fetchone()[0]
            self.cursor.execute(non_violation_query)
            non_violation = self.cursor.fetchone()[0]
            return {
                'total': total,
                'violation': violation,
                'non_violation': non_violation
            }
        except Exception as e:
            raise Exception(f"Error checking allowed values: {str(e)}")
    def get_eng_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND INSTR(TO_CHAR("{column}"), ',') > 0
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking ENG format: {str(e)}")

    def get_tr_numeric_format_violation_count(self, schema, table, column):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND INSTR(TO_CHAR("{column}"), ',') = 0
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking TR format: {str(e)}")
        

    def get_number_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[0-9]') AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching number violations: {str(e)}")

    def get_allowed_values_violations(self, schema, table, column, allowed_values, limit=100):
        try:
            formatted_values = ', '.join(f"'{val}'" for val in allowed_values)
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({formatted_values}) AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching allowed values violations: {str(e)}")

    def get_eng_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND INSTR(TO_CHAR("{column}"), ',') > 0 AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching ENG numeric format violations: {str(e)}")

    def get_tr_numeric_format_violations(self, schema, table, column, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND INSTR(TO_CHAR("{column}"), ',') = 0 AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching TR numeric format violations: {str(e)}")

    def get_case_inconsistency_count(self, schema, table, column, expected_case):
        try:
            if expected_case == 'upper':
                condition = f'"{column}" != UPPER("{column}")'
            elif expected_case == 'lower':
                condition = f'"{column}" != LOWER("{column}")'
            else:
                raise ValueError("Unsupported case type")

            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND {condition}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking case consistency: {str(e)}")

    def get_future_date_violation_count(self, schema, table, column):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" > SYSDATE
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking future dates: {str(e)}")

    def get_date_range_violation_count(self, schema, table, column, start_date, end_date):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" < TO_DATE('{start_date}', 'YYYY-MM-DD') OR "{column}" > TO_DATE('{end_date}', 'YYYY-MM-DD')
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking date range: {str(e)}")

    def get_special_char_violation_count(self, schema, table, column, allowed_pattern):
        try:
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[^ {allowed_pattern}]')
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking special characters: {str(e)}")
        
    def get_case_inconsistency_violations(self, schema, table, column, expected_case, limit=100):
        try:
            if expected_case == 'upper':
                condition = f'"{column}" != UPPER("{column}")'
            elif expected_case == 'lower':
                condition = f'"{column}" != LOWER("{column}")'
            else:
                raise ValueError("Unsupported case type")

            query = f'SELECT * FROM "{schema}"."{table}" WHERE {condition} AND ROWNUM <= {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching case inconsistency violations: {str(e)}")

    def get_future_date_violations(self, schema, table, column, limit=100):
        try:
            query = f'SELECT * FROM "{schema}"."{table}" WHERE "{column}" > SYSDATE AND ROWNUM <= {limit}'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching future date violations: {str(e)}")

    def get_date_range_violations(self, schema, table, column, start_date, end_date, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" < TO_DATE('{start_date}', 'YYYY-MM-DD') OR "{column}" > TO_DATE('{end_date}', 'YYYY-MM-DD')
                AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching date range violations: {str(e)}")

    def get_special_char_violations(self, schema, table, column, allowed_pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '[^ {allowed_pattern}]') AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching special character violations: {str(e)}")

    def get_email_format_violation_count(self, schema, table, column):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT REGEXP_LIKE("{column}", '{regex}')
            '''
            print("Executing SQL Query:")
            print(query)

            self.cursor.execute(query)
            result = self.cursor.fetchone()[0]
            print("Email format violation count:", result)
            return result
        except Exception as e:
            print(f"[ERROR] get_email_format_violation_count failed: {e}")
            raise


            
        except Exception as e:
            raise Exception(f"Error checking email format: {str(e)}")

    def get_regex_pattern_violation_count(self, schema, table, column, pattern):
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND  REGEXP_LIKE("{column}", '{pattern}')
            '''
            print("Executing SQL Query:")
            print(query)
            print(pattern)

            self.cursor.execute(query)
            result = self.cursor.fetchone()[0]
            print("Format violation count:", result)
            return result
        except Exception as e:
            print(f"[ERROR] get_regex_format_violation_count failed: {e}")
            raise


    def get_positive_value_violation_count(self, schema, table, column, strict):
        try:
            operator = '>' if strict else '>='
            self.cursor.execute(f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT ("{column}" {operator} 0)
            ''')
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Error checking positive values: {str(e)}")
        
    def get_email_format_violations(self, schema, table, column, limit=100):
        try:
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE REGEXP_LIKE("{column}", '{regex}') = 0 AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching email format violations: {str(e)}")

    def get_regex_pattern_violations(self, schema, table, column, pattern, limit=100):
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND REGEXP_LIKE("{column}", '{pattern}') AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching regex pattern violations: {str(e)}")

    def get_positive_value_violations(self, schema, table, column, strict, limit=100):
        try:
            operator = '>' if strict else '>='
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL AND NOT ("{column}" {operator} 0) AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error fetching positive value violations: {str(e)}")

    def get_tckn_violation_count(self, schema, table, column):
        """Count invalid TCKN values (Oracle)"""
        try:
            query = f'''
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                  AND NOT (
                    LENGTH("{column}") = 11
                    AND REGEXP_LIKE("{column}", '^[0-9]+$')
                    AND SUBSTR("{column}", 1, 1) <> '0'
                    AND (
                        (
                            (
                                TO_NUMBER(SUBSTR("{column}",1,1)) +
                                TO_NUMBER(SUBSTR("{column}",3,1)) +
                                TO_NUMBER(SUBSTR("{column}",5,1)) +
                                TO_NUMBER(SUBSTR("{column}",7,1)) +
                                TO_NUMBER(SUBSTR("{column}",9,1))
                            ) * 7
                            -
                            (
                                TO_NUMBER(SUBSTR("{column}",2,1)) +
                                TO_NUMBER(SUBSTR("{column}",4,1)) +
                                TO_NUMBER(SUBSTR("{column}",6,1)) +
                                TO_NUMBER(SUBSTR("{column}",8,1))
                            )
                        ) MOD 10
                    ) = TO_NUMBER(SUBSTR("{column}",10,1))
                    AND (
                        (
                            TO_NUMBER(SUBSTR("{column}",1,1)) +
                            TO_NUMBER(SUBSTR("{column}",2,1)) +
                            TO_NUMBER(SUBSTR("{column}",3,1)) +
                            TO_NUMBER(SUBSTR("{column}",4,1)) +
                            TO_NUMBER(SUBSTR("{column}",5,1)) +
                            TO_NUMBER(SUBSTR("{column}",6,1)) +
                            TO_NUMBER(SUBSTR("{column}",7,1)) +
                            TO_NUMBER(SUBSTR("{column}",8,1)) +
                            TO_NUMBER(SUBSTR("{column}",9,1)) +
                            TO_NUMBER(SUBSTR("{column}",10,1))
                        ) MOD 10
                    ) = TO_NUMBER(SUBSTR("{column}",11,1))
                  )
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Oracle TCKN violation count error: {str(e)}")

    def get_tckn_violations(self, schema, table, column, limit=100):
        """Get invalid TCKN rows (Oracle)"""
        try:
            query = f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE "{column}" IS NOT NULL
                  AND NOT (
                    LENGTH("{column}") = 11
                    AND REGEXP_LIKE("{column}", '^[0-9]+$')
                    AND SUBSTR("{column}", 1, 1) <> '0'
                    AND (
                        (
                            (
                                TO_NUMBER(SUBSTR("{column}",1,1)) +
                                TO_NUMBER(SUBSTR("{column}",3,1)) +
                                TO_NUMBER(SUBSTR("{column}",5,1)) +
                                TO_NUMBER(SUBSTR("{column}",7,1)) +
                                TO_NUMBER(SUBSTR("{column}",9,1))
                            ) * 7
                            -
                            (
                                TO_NUMBER(SUBSTR("{column}",2,1)) +
                                TO_NUMBER(SUBSTR("{column}",4,1)) +
                                TO_NUMBER(SUBSTR("{column}",6,1)) +
                                TO_NUMBER(SUBSTR("{column}",8,1))
                            )
                        ) MOD 10
                    ) = TO_NUMBER(SUBSTR("{column}",10,1))
                    AND (
                        (
                            TO_NUMBER(SUBSTR("{column}",1,1)) +
                            TO_NUMBER(SUBSTR("{column}",2,1)) +
                            TO_NUMBER(SUBSTR("{column}",3,1)) +
                            TO_NUMBER(SUBSTR("{column}",4,1)) +
                            TO_NUMBER(SUBSTR("{column}",5,1)) +
                            TO_NUMBER(SUBSTR("{column}",6,1)) +
                            TO_NUMBER(SUBSTR("{column}",7,1)) +
                            TO_NUMBER(SUBSTR("{column}",8,1)) +
                            TO_NUMBER(SUBSTR("{column}",9,1)) +
                            TO_NUMBER(SUBSTR("{column}",10,1))
                        ) MOD 10
                    ) = TO_NUMBER(SUBSTR("{column}",11,1))
                  )
                FETCH FIRST {limit} ROWS ONLY
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Oracle get TCKN violations error: {str(e)}")

    def get_date_logic_violation_count(self, schema, table, start_date_col, end_date_col):
        """Oracle: Count rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT COUNT(*) 
                FROM "{schema}"."{table}"
                WHERE "{start_date_col}" IS NOT NULL
                AND "{end_date_col}" IS NOT NULL
                AND "{start_date_col}" >= "{end_date_col}"
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            raise Exception(f"Oracle error counting date logic violations: {str(e)}")

    def get_date_logic_violations(self, schema, table, start_date_col, end_date_col, limit=100):
        """Oracle: Get sample rows where start_date >= end_date"""
        try:
            query = f'''
                SELECT * 
                FROM "{schema}"."{table}"
                WHERE "{start_date_col}" IS NOT NULL
                AND "{end_date_col}" IS NOT NULL
                AND "{start_date_col}" >= "{end_date_col}"
                AND ROWNUM <= {limit}
            '''
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(f"Oracle error fetching date logic violations: {str(e)}")
        
    def get_text_column_date_formats_oracle(self, schema, table, column_name, limit=1000):
        try:
            query = f"""
                SELECT *,

                    CASE
                        WHEN REGEXP_LIKE({column_name}, '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$') THEN 'DD.MM.YYYY'
                        WHEN REGEXP_LIKE({column_name}, '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$') THEN 'YYYY-MM-DD'
                        WHEN REGEXP_LIKE({column_name}, '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$') THEN 'MM/DD/YYYY'
                        WHEN REGEXP_LIKE({column_name}, '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$') THEN 'DD/MM/YYYY'
                        WHEN REGEXP_LIKE({column_name}, '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$') THEN 'YYYY.MM.DD'
                        ELSE 'Unknown'
                    END AS format,

                    CASE
                        WHEN TO_DATE({column_name}, 'DD.MM.YYYY') IS NOT NULL THEN 1
                        WHEN TO_DATE({column_name}, 'YYYY-MM-DD') IS NOT NULL THEN 1
                        WHEN TO_DATE({column_name}, 'MM/DD/YYYY') IS NOT NULL THEN 1
                        WHEN TO_DATE({column_name}, 'DD/MM/YYYY') IS NOT NULL THEN 1
                        WHEN TO_DATE({column_name}, 'YYYY.MM.DD') IS NOT NULL THEN 1
                        ELSE 0
                    END AS is_valid,

                    COALESCE(
                        CASE WHEN REGEXP_LIKE({column_name}, '^[0-3][0-9]\\.[0-1][0-9]\\.[1-2][0-9]{{3}}$') THEN TO_DATE({column_name}, 'DD.MM.YYYY') END,
                        CASE WHEN REGEXP_LIKE({column_name}, '^[1-2][0-9]{{3}}-[0-1][0-9]-[0-3][0-9]$') THEN TO_DATE({column_name}, 'YYYY-MM-DD') END,
                        CASE WHEN REGEXP_LIKE({column_name}, '^[0-1][0-9]/[0-3][0-9]/[1-2][0-9]{{3}}$') THEN TO_DATE({column_name}, 'MM/DD/YYYY') END,
                        CASE WHEN REGEXP_LIKE({column_name}, '^[0-3][0-9]/[0-1][0-9]/[1-2][0-9]{{3}}$') THEN TO_DATE({column_name}, 'DD/MM/YYYY') END,
                        CASE WHEN REGEXP_LIKE({column_name}, '^[1-2][0-9]{{3}}\\.[0-1][0-9]\\.[0-3][0-9]$') THEN TO_DATE({column_name}, 'YYYY.MM.DD') END
                    ) AS parsed_date

                FROM "{schema}"."{table}"
                WHERE {column_name} IS NOT NULL
                AND ROWNUM <= {limit}
            """
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            raise Exception(f"Oracle error fetching date formats: {str(e)}")





