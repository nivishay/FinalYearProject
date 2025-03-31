import mysql.connector
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.schema import CreateTable
from sqlalchemy import Text

# --- MySQL connection ---
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="lior",
    database="world",
    port=3306
)
mysql_cursor = mysql_conn.cursor(dictionary=True)

# --- PostgreSQL connection ---
pg_conn = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Khturer7",
    dbname="postgres",
    port=5432
)
pg_cursor = pg_conn.cursor()

# --- SQLAlchemy engines ---
mysql_engine = create_engine("mysql+mysqlconnector://root:lior@localhost:3306/world")
pg_engine = create_engine("postgresql+psycopg2://postgres:Khturer7@localhost:5432/postgres")

# Reflect metadata from MySQL
mysql_metadata = MetaData()
mysql_metadata.reflect(bind=mysql_engine)

for table_name, table in mysql_metadata.tables.items():
    print(f"🔄 Processing table: {table_name}")

    # Remove foreign keys
    table.constraints = {c for c in table.constraints if not c.__class__.__name__.startswith('ForeignKey')}

    # Convert ENUM columns to TEXT
    for col in table.columns:
        if col.type.__class__.__name__ == 'ENUM':
            col.type = Text()

    # Create table in PostgreSQL
    pg_create_table = str(CreateTable(table).compile(pg_engine))
    pg_create_table = pg_create_table.replace("TINYINT", "SMALLINT")

    pg_cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
    pg_cursor.execute(pg_create_table)
    pg_conn.commit()

    # Copy data
    mysql_cursor.execute(f"SELECT * FROM {table_name}")
    rows = mysql_cursor.fetchall()
    if rows:
        columns = rows[0].keys()
        values = [[row[col] for col in columns] for row in rows]

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        pg_cursor.executemany(insert_query.as_string(pg_conn), values)
        pg_conn.commit()

    print(f"✅ Done: {table_name}")

# Cleanup
mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("🎉 All tables copied successfully (without foreign keys for now).")
