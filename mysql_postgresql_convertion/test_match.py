# mysql_postgresql_convertion/test_match.py

import mysql.connector
import psycopg2
import random
from mysql_postgresql_convertion.config import MYSQL_CONFIG, POSTGRES_CONFIG

def get_common_tables(mysql_cursor, pg_cursor):
    mysql_cursor.execute(f"""
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = '{MYSQL_CONFIG["database"]}'
    """)
    pg_cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    return sorted(set(row[0] for row in mysql_cursor.fetchall()) &
                  set(row[0] for row in pg_cursor.fetchall()))

def compare_row_counts(mysql_cursor, pg_cursor, tables):
    print("\n📦 Comparing row counts:")
    for table in tables:
        mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        pg_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        mysql_count = mysql_cursor.fetchone()[0]
        pg_count = pg_cursor.fetchone()[0]
        status = "✅" if mysql_count == pg_count else "❌"
        print(f"{status} {table}: MySQL={mysql_count}, PostgreSQL={pg_count}")

def get_primary_keys(cursor, dbtype, tables):
    if dbtype == 'mysql':
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE CONSTRAINT_NAME = 'PRIMARY'
              AND TABLE_SCHEMA = '{MYSQL_CONFIG["database"]}'
              AND TABLE_NAME IN ({','.join(f"'{t}'" for t in tables)})
        """)
    else:
        cursor.execute(f"""
            SELECT t.relname AS table_name, a.attname AS column_name
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p' AND n.nspname = 'public'
              AND t.relname IN ({','.join(f"'{t}'" for t in tables)})
        """)
    return sorted(cursor.fetchall())

def compare_primary_keys(mysql_cursor, pg_cursor, tables):
    print("\n🔐 Comparing primary keys:")
    mysql_pks = get_primary_keys(mysql_cursor, 'mysql', tables)
    pg_pks = get_primary_keys(pg_cursor, 'postgres', tables)
    if mysql_pks == pg_pks:
        print("✅ Primary keys match:")
        for pk in mysql_pks:
            print(f"   • {pk[0]}.{pk[1]}")
    else:
        print("❌ Primary key mismatch.")
        print("MySQL:", mysql_pks)
        print("PostgreSQL:", pg_pks)

def get_foreign_keys(cursor, dbtype, tables):
    if dbtype == 'mysql':
        cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{MYSQL_CONFIG["database"]}'
              AND REFERENCED_TABLE_NAME IS NOT NULL
              AND TABLE_NAME IN ({','.join(f"'{t}'" for t in tables)})
        """)
    else:
        cursor.execute(f"""
            SELECT
                rel_t.relname AS table_name,
                a.attname AS column_name,
                rel_ref.relname AS referenced_table,
                a_ref.attname AS referenced_column
            FROM pg_constraint c
            JOIN pg_class rel_t ON c.conrelid = rel_t.oid
            JOIN pg_namespace n ON rel_t.relnamespace = n.oid
            JOIN pg_class rel_ref ON c.confrelid = rel_ref.oid
            JOIN pg_attribute a ON a.attrelid = rel_t.oid AND a.attnum = ANY(c.conkey)
            JOIN pg_attribute a_ref ON a_ref.attrelid = rel_ref.oid AND a_ref.attnum = ANY(c.confkey)
            WHERE c.contype = 'f'
              AND n.nspname = 'public'
              AND rel_t.relname IN ({','.join(f"'{t}'" for t in tables)})
        """)
    return sorted(cursor.fetchall())

def compare_foreign_keys(mysql_cursor, pg_cursor, tables):
    print("\n🔗 Comparing foreign keys:")
    mysql_fks = get_foreign_keys(mysql_cursor, 'mysql', tables)
    pg_fks = get_foreign_keys(pg_cursor, 'postgres', tables)
    if mysql_fks == pg_fks:
        print("✅ Foreign keys match:")
        for fk in mysql_fks:
            print(f"   • {fk[0]}.{fk[1]} → {fk[2]}.{fk[3]}")
    else:
        print("❌ Foreign key mismatch.")
        print("MySQL:", mysql_fks)
        print("PostgreSQL:", pg_fks)

def compare_column_names(mysql_cursor, pg_cursor, tables):
    print("\n📋 Comparing column names:")
    mysql_cursor.execute(f"""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '{MYSQL_CONFIG["database"]}'
          AND TABLE_NAME IN ({','.join(f"'{t}'" for t in tables)})
    """)
    pg_cursor.execute(f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ({','.join(f"'{t}'" for t in tables)})
    """)
    mysql_cols = sorted(mysql_cursor.fetchall())
    pg_cols = sorted(pg_cursor.fetchall())

    if mysql_cols == pg_cols:
        print("✅ Column names match.")
    else:
        print("❌ Column name mismatch.")
        print("MySQL:", mysql_cols)
        print("PostgreSQL:", pg_cols)

def normalize_row(row):
    return tuple(str(col).strip() if isinstance(col, str) else str(col) for col in row) if row else None

def compare_sample_data(mysql_cursor, pg_cursor, tables):
    print("\n🔎 Comparing one sample row from each table:")
    for table in tables:
        offset = random.randint(0, 3)
        mysql_cursor.execute(f"SELECT * FROM {table} LIMIT 1 OFFSET {offset}")
        pg_cursor.execute(f'SELECT * FROM "{table}" LIMIT 1 OFFSET {offset}')
        mysql_row = normalize_row(mysql_cursor.fetchone())
        pg_row = normalize_row(pg_cursor.fetchone())
        status = "✅" if mysql_row == pg_row else "⚠️"
        print(f"{status} Sample from {table} (row #{offset}):\n   MySQL: {mysql_row}\n   PostgreSQL: {pg_row}")

def main():
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    pg_cursor = pg_conn.cursor()

    tables = get_common_tables(mysql_cursor, pg_cursor)
    print(f"\n🗂️ Common tables detected: {tables}")

    compare_row_counts(mysql_cursor, pg_cursor, tables)
    compare_primary_keys(mysql_cursor, pg_cursor, tables)
    compare_foreign_keys(mysql_cursor, pg_cursor, tables)
    compare_column_names(mysql_cursor, pg_cursor, tables)
    compare_sample_data(mysql_cursor, pg_cursor, tables)

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
