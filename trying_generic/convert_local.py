import re
import mysql.connector
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.types import (
    NullType, Text, Integer, SmallInteger, DateTime, LargeBinary
)
from geoalchemy2 import Geometry
from sqlalchemy.schema import CreateTable
from mysql_postgresql_convertion.config import MYSQL_CONFIG, POSTGRES_CONFIG

# שלב 1: חיבור למסדי הנתונים
mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

mysql_cursor = mysql_conn.cursor(dictionary=True)
pg_cursor = pg_conn.cursor()

# הפעלת PostGIS (אם טרם הופעל) והגדרת search_path
pg_cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
pg_cursor.execute("SET search_path TO public;")
pg_conn.commit()

# מנועי SQLAlchemy
mysql_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
    f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
)
pg_engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
)

mysql_metadata = MetaData()
mysql_metadata.reflect(bind=mysql_engine)


def convert_type(orig_type):
    """ ממיר את הטיפוס של העמודה לסוג מתאים ב-PostgreSQL (כולל PostGIS). """
    tname = orig_type.__class__.__name__.upper()
    if tname == "TINYINT":
        return SmallInteger()
    elif tname == "MEDIUMINT":
        return Integer()
    elif tname == "YEAR":
        return SmallInteger()
    elif tname == "DATETIME":
        return DateTime()
    elif tname == "ENUM":
        return Text()
    elif tname == "SET":
        # עמודת SET תהפוך לטקסט (נשמור כרשימת ערכים מופרדת בפסיק)
        return Text()
    elif tname == "GEOMETRY":
        return Geometry("GEOMETRY", srid=4326)
    elif tname == "BLOB":
        return LargeBinary()
    elif isinstance(orig_type, NullType):
        return Text()
    else:
        return orig_type

def clone_column(orig_col):
    new_type = convert_type(orig_col.type)
    return Column(
        orig_col.name,
        new_type,
        primary_key=orig_col.primary_key,
        autoincrement=orig_col.autoincrement,
        nullable=orig_col.nullable,
        default=orig_col.default,
        index=orig_col.index,
        unique=orig_col.unique,
        comment=orig_col.comment
    )

for table_name, table in mysql_metadata.tables.items():
    print(f"🔄 Processing table: {table_name}")

    # הסרת FK-ים כדי למנוע בעיית סדר יצירת טבלאות
    table.constraints = {
        c for c in table.constraints
        if not c.__class__.__name__.startswith('ForeignKey')
    }

    new_cols = [clone_column(col) for col in table.columns]
    new_table = Table(table.name, MetaData(), *new_cols)

    try:
        pg_create_table = str(CreateTable(new_table).compile(pg_engine))
        # הסרת סינטקס MySQL שלא נתמך ב-PostgreSQL
        pg_create_table = pg_create_table.replace("ON UPDATE CURRENT_TIMESTAMP", "")
        pg_create_table = re.sub(r'COLLATE\s+"[^"]+"', '', pg_create_table)
    except Exception as e:
        print(f"❌ Skipping table {table_name} due to compile error: {e}")
        continue

    pg_cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
    pg_cursor.execute(pg_create_table)
    pg_conn.commit()

    mysql_cursor.execute(f"SELECT * FROM {table_name}")
    rows = mysql_cursor.fetchall()

    if rows:
        col_names = [col.name for col in new_cols]

        # הופכים ערכי Set למחרוזת פסיקים
        converted_rows = []
        for r in rows:
            # באובייקט row -> {col_name: value, ...}
            row_data = []
            for coln in col_names:
                val = r.get(coln)
                # אם הערך הוא set ב-פייתון -> נהפוך אותו למחרוזת
                if isinstance(val, set):
                    val = ",".join(val)
                row_data.append(val)
            converted_rows.append(row_data)

        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, col_names)),
            sql.SQL(', ').join(sql.Placeholder() * len(col_names))
        )
        pg_cursor.executemany(insert_query.as_string(pg_conn), converted_rows)
        pg_conn.commit()

    print(f"✅ Done: {table_name}")

mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("🎉 All tables copied successfully (including SET -> text, geometry, etc)!")

