
import psycopg2
from psycopg2 import OperationalError
from abc import ABC, abstractmethod
import mysql.connector
from mysql.connector import Error


class ConnectionSystem(ABC):
    connections = {}

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self, conn):
        pass

    @abstractmethod
    def checkConnection(self):
        pass


class PostgresConnectionSystem(ConnectionSystem):
    def checkConnection(self):
        pass
    def connect(self, host: object, dbname: object, user: object, password: object, port: object = "5432") -> object:
        try:
            conn = psycopg2.connect(
                host=host,
                dbname=dbname,
                user=user,
                password=password,
                port=port
            )
            return conn
        except OperationalError as e:
            print(f"Error: Unable to connect to the database\n{e}")
            return None

    def disconnect(self, conn):
        if conn:
            conn.close()
        else:
            print("No connection to close")


class MySQLConnectionSystem(ConnectionSystem):
    def checkConnection(self):
        pass

    def connect(self, host: str, dbname: str, user: str, password: str, port: str = "3306"):
        try:
            conn = mysql.connector.connect(
                host=host,
                database=dbname,
                user=user,
                password=password,
                port=port
            )
            if conn.is_connected():
                return conn
        except Error as e:
            print(f"Error: Unable to connect to the database\n{e}")
            return None

    def disconnect(self, conn):
        if conn and conn.is_connected():
            conn.close()
        else:
            print("No connection to close")


