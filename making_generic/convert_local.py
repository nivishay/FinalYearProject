# making_generic/convert_local.py

import mysql.connector
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Text
from sqlalchemy.schema import CreateTable
from making_generic.config import MYSQL_CONFIG, POSTGRES_CONFIG

mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

mysql_cursor = mysql_conn.cursor(dictionary=True)
pg_cursor = pg_conn.cursor()

# Create SQLAlchemy engines
mysql_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
)
pg_engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
)

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

    pg_create_table = str(CreateTable(table).compile(pg_engine)).replace("TINYINT", "SMALLINT")
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

mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("🎉 All tables copied successfully (without foreign keys for now).")

