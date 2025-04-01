# mysql_postgresql_convertion/add_foreign_keys_to_db.py

import mysql.connector
import psycopg2
from mysql_postgresql_convertion.config import MYSQL_CONFIG, POSTGRES_CONFIG

mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
pg_conn = psycopg2.connect(**POSTGRES_CONFIG)

mysql_cursor = mysql_conn.cursor(dictionary=True)
pg_cursor = pg_conn.cursor()

mysql_cursor.execute(f"""
    SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME,
           REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE REFERENCED_TABLE_NAME IS NOT NULL
      AND TABLE_SCHEMA = '{MYSQL_CONFIG["database"]}';
""")

foreign_keys = mysql_cursor.fetchall()

for fk in foreign_keys:
    alter_sql = f'''
        ALTER TABLE "{fk['TABLE_NAME']}"
        ADD CONSTRAINT "{fk['CONSTRAINT_NAME']}"
        FOREIGN KEY ("{fk['COLUMN_NAME']}")
        REFERENCES "{fk['REFERENCED_TABLE_NAME']}" ("{fk['REFERENCED_COLUMN_NAME']}");
    '''
    try:
        print(f"🔗 Adding FK on {fk['TABLE_NAME']}: {fk['COLUMN_NAME']} → {fk['REFERENCED_TABLE_NAME']}({fk['REFERENCED_COLUMN_NAME']})")
        pg_cursor.execute(alter_sql)
        pg_conn.commit()
    except Exception as e:
        print(f"❌ Failed to add FK {fk['CONSTRAINT_NAME']}: {e}")
        pg_conn.rollback()

mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("✅ Foreign key process finished.")
