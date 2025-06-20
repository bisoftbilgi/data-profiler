from database.connectors import MSSQLConnector, MySQLConnector, OracleConnector, PostgresConnector

class DatabaseFactory:
    """Factory class for creating database connectors"""
    
    @staticmethod
    def create_connector(db_type: str):
        """Create a database connector based on the database type"""
        if db_type.lower() == 'mssql':
            return MSSQLConnector()
        elif db_type.lower() == 'mysql':
            return MySQLConnector()
        elif db_type.lower() in ('postgres', 'postgresql'):
            return PostgresConnector()
        elif db_type.lower() == 'oracle':
            return OracleConnector()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")