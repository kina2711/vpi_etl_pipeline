import psycopg2
import logging
from typing import Tuple, List
from ..config.settings import DatabaseConfig

logger = logging.getLogger(__name__)

class PostgresConnector:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection = None
        self._cursor = None
        logger.debug(f"PostgresConnector initialized for db: {config.dbname}")

    def __enter__(self):
        try:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            self._cursor = self._connection.cursor()
            logger.debug(f"DB connection established to {self.config.host}.")
            return self
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database {self.config.dbname} at {self.config.host}: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        logger.debug(f"DB connection to {self.config.host} closed.")

    def execute_query(self, query: str) -> Tuple[List[tuple], int]:
        if not self._connection or not self._cursor:
            raise ConnectionError("Database connection is not open. Use 'with' statement.")

        try:
            self._cursor.execute(query)
            result = self._cursor.fetchall()
            num_columns = len(self._cursor.description) if self._cursor.description else 0
            logger.debug(f"Query returned {len(result)} rows and {num_columns} columns.")
            return result, num_columns
        except psycopg2.Error as e:
            logger.error(f"Error executing SQL query: {e}", exc_info=True)
            self._connection.rollback()
            raise