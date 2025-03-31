import mysql.connector
import psycopg2

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

# --- Get foreign key constraints from MySQL ---
mysql_cursor.execute("""
    SELECT
        TABLE_NAME,
        CONSTRAINT_NAME,
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE
        REFERENCED_TABLE_NAME IS NOT NULL
        AND TABLE_SCHEMA = 'world';
""")

foreign_keys = mysql_cursor.fetchall()

# --- Add constraints in PostgreSQL ---
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
        pg_conn.rollback()  # reset the transaction so future ones don't fail

# --- Cleanup ---
mysql_cursor.close()
mysql_conn.close()
pg_cursor.close()
pg_conn.close()

print("✅ Foreign key process finished.")
