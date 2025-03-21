import time
from DBConnectionSystem.connect import PostgresConnectionSystem
from DBConnectionSystem.connect import MySQLConnectionSystem

my_sql_DB = MySQLConnectionSystem()
postgres_db = PostgresConnectionSystem()

postgres_connection = postgres_db.connect(
    "database-2.cl6wms8owvi7.eu-north-1.rds.amazonaws.com",
    "test",
    "postgres",
    "nivishay12345",
    "5432"
)

mysql_connection = my_sql_DB.connect(
    "database-4.cl6wms8owvi7.eu-north-1.rds.amazonaws.com",
    "test",
    "admin",
    "y7Wc2qT1TK3bwyySTzQc",
    "3306"
)

postgres_cursor = postgres_connection.cursor()
my_sql_cursor = mysql_connection.cursor()

query = 'SELECT title FROM netflix_titles WHERE title LIKE %s'

start_time_postgres = time.time()
postgres_cursor.execute(query, ('%Heavy Rescue: 401%',))
postgres_results = postgres_cursor.fetchall()
end_time_postgres = time.time()

start_time_mysql = time.time()
my_sql_cursor.execute(query, ('%Heavy Rescue: 401%',))
mysql_results = my_sql_cursor.fetchall()
end_time_mysql = time.time()

print("PostgreSQL Results:")
for row in postgres_results:
    print(f"Title: {row[0]}")

print("\nMySQL Results:")
for row in mysql_results:
    print(f"Title: {row[0]}")

postgres_query_time = end_time_postgres - start_time_postgres
mysql_query_time = end_time_mysql - start_time_mysql

print("\nQuery Execution Time Comparison:")
print(f"PostgreSQL query time: {postgres_query_time:.6f} seconds")
print(f"MySQL query time: {mysql_query_time:.6f} seconds")

postgres_db.disconnect(postgres_connection)
my_sql_DB.disconnect(mysql_connection)
