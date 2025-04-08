import time
import psycopg2
import mysql.connector

# Connection details (change accordingly)
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "sakila"
}

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "test_user",
    "password": "Nivishay12345",
    "database": "sakila"
}
# List of test queries
queries = {
    "simple_select": "SELECT * FROM customer WHERE active = TRUE;",

    "order_by_limit": "SELECT * FROM film ORDER BY rental_rate DESC LIMIT 10;",

    "join_film_language": """
        SELECT f.title, l.name AS language
        FROM film f
        JOIN language l ON f.language_id = l.language_id;
    """,

    "agg_rentals_per_customer": """
        SELECT customer_id, COUNT(*) AS rental_count
        FROM rental
        GROUP BY customer_id
        ORDER BY rental_count DESC
        LIMIT 10;
    """,

    "multi_join_film_actor_category": """
        SELECT f.title, a.first_name, c.name AS category
        FROM film f
        JOIN film_actor fa ON f.film_id = fa.film_id
        JOIN actor a ON fa.actor_id = a.actor_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LIMIT 50;
    """,

    "subquery_inventory": """
        SELECT title
        FROM film
        WHERE film_id IN (
            SELECT film_id FROM inventory WHERE store_id = 1
        );
    """,

    "count_films_per_category": """
        SELECT c.name AS category, COUNT(f.film_id) AS film_count
        FROM category c
        JOIN film_category fc ON c.category_id = fc.category_id
        JOIN film f ON f.film_id = fc.film_id
        GROUP BY c.name;
    """,

    "film_length_over_100": "SELECT title FROM film WHERE length > 100;",

    "longest_films": "SELECT title, length FROM film ORDER BY length DESC LIMIT 5;",

    "top_actors_by_film": """
        SELECT a.actor_id, a.first_name, a.last_name, COUNT(*) AS films
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        GROUP BY a.actor_id
        ORDER BY films DESC
        LIMIT 10;
    """,

    "top_paying_customers": """
        SELECT customer_id, SUM(amount) AS total_paid
        FROM payment
        GROUP BY customer_id
        ORDER BY total_paid DESC
        LIMIT 5;
    """,

    "rentals_per_month": """
        SELECT DATE_TRUNC('month', rental_date) AS month, COUNT(*) AS total
        FROM rental
        GROUP BY month
        ORDER BY month;
    """,

    "active_staff": "SELECT staff_id, first_name FROM staff WHERE active = TRUE;",

    "inventory_per_store": """
        SELECT store_id, COUNT(*) AS inventory_count
        FROM inventory
        GROUP BY store_id;
    """,

    "most_rented_films": """
        SELECT f.title, COUNT(*) AS times_rented
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        GROUP BY f.title
        ORDER BY times_rented DESC
        LIMIT 10;
    """,

    "duplicate_customer_lastnames": """
        SELECT last_name, COUNT(*) AS count
        FROM customer
        GROUP BY last_name
        HAVING COUNT(*) > 1
        ORDER BY count DESC;
    """,

    "last_5_payments": "SELECT * FROM payment ORDER BY payment_date DESC LIMIT 5;",

    "films_not_in_inventory": """
        SELECT f.title
        FROM film f
        WHERE f.film_id NOT IN (SELECT DISTINCT film_id FROM inventory);
    """,

    "customers_no_rentals": """
        SELECT c.first_name, c.last_name
        FROM customer c
        LEFT JOIN rental r ON c.customer_id = r.customer_id
        WHERE r.rental_id IS NULL;
    """,

    "actor_film_categories": """
        SELECT a.first_name, a.last_name, COUNT(DISTINCT fc.category_id) AS categories
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN film_category fc ON fa.film_id = fc.film_id
        GROUP BY a.actor_id;
    """,

    "top_grossing_actor": """
        SELECT a.actor_id, a.first_name, a.last_name, SUM(p.amount) AS total_revenue
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN inventory i ON fa.film_id = i.film_id
        JOIN rental r ON i.inventory_id = r.inventory_id
        JOIN payment p ON r.rental_id = p.rental_id
        GROUP BY a.actor_id, a.first_name, a.last_name
        ORDER BY total_revenue DESC
        LIMIT 1;
    """,

    "rented_all_in_category": """
        SELECT c.customer_id, c.first_name, c.last_name
        FROM customer c
        WHERE NOT EXISTS (
            SELECT 1
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            WHERE fc.category_id = 1
            AND NOT EXISTS (
                SELECT 1
                FROM rental r
                JOIN inventory i ON r.inventory_id = i.inventory_id
                WHERE i.film_id = f.film_id AND r.customer_id = c.customer_id
            )
        );
    """,

    "most_rented_monthly": """
        SELECT *
        FROM (
            SELECT f.title,
                   DATE_TRUNC('month', r.rental_date) AS rental_month,
                   COUNT(*) AS rental_count,
                   RANK() OVER (PARTITION BY DATE_TRUNC('month', r.rental_date) ORDER BY COUNT(*) DESC) AS rank
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            GROUP BY f.title, rental_month
        ) ranked
        WHERE rank = 1;
    """
    ,"better for mySQL":"""SELECT payment_id, amount, payment_date
    FROM payment
    WHERE customer_id = 1
    ORDER BY payment_date DESC
    LIMIT 10;
    """
}
def connect_to_databases():
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_cursor = pg_conn.cursor()

    print("\nConnecting to MySQL...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()

    return pg_conn, pg_cursor, mysql_conn, mysql_cursor
def run_query(cursor, query):
    start = time.time()
    cursor.execute(query)
    _ = cursor.fetchall()
    return time.time() - start

def benchmark_db(cursor, label, conn):
    print(f"\n=== {label} Results ===")
    times = []
    for name, query in queries:
        total_time = 0
        success = True
        for _ in range(100):
            try:
                start = time.time()
                cursor.execute(query)
                _ = cursor.fetchall()
                end = time.time()
                total_time += (end - start)
            except Exception as e:
                conn.rollback()
                print(f"{name:<35} | ERROR: {str(e)}")
                success = False
                break  # Stop this query if it fails once

        if success:
            avg_time = total_time / 100
            print(f"{name:<35} | {avg_time:.4f} sec (avg of 100 runs)")
            times.append(avg_time)
        else:
            times.append(None)

    return times

def overhall_test():
    queries = sakila_test.queries
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(**sakila_test.PG_CONFIG)
    pg_cursor = pg_conn.cursor()
    pg_times = sakila_test.benchmark_db(pg_cursor, "PostgreSQL",pg_conn)

    print("\nConnecting to MySQL...")
    mysql_conn = mysql.connector.connect(**sakila_test.MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    mysql_times = sakila_test.benchmark_db(mysql_cursor, "MySQL",mysql_conn)

    print("\n=== Summary (in seconds) ===")
    print(f"{'Query':<35} | {'PostgreSQL':>12} | {'MySQL':>12}")
    print("-" * 65)
    for i, (name, _) in enumerate(queries):
        pg_time = f"{pg_times[i]:.4f}" if pg_times[i] is not None else "ERROR"
        my_time = f"{mysql_times[i]:.4f}" if mysql_times[i] is not None else "ERROR"
        print(f"{name:<35} | {pg_time:>12} | {my_time:>12}")
    mysql_conn.disconnect()

import time

def test_specific(query_key):
    if query_key not in queries:
        print(f"Query key '{query_key}' not found.")
        return

    sql = queries[query_key]

    # Connect to both databases
    pg_conn, pg_cursor, mysql_conn, mysql_cursor = connect_to_databases()

    # PostgreSQL Execution
    print(f"\n🔵 PostgreSQL - Running: {query_key}")
    try:
        start_pg = time.time()
        pg_cursor.execute(sql)
        _ = pg_cursor.fetchall()
        end_pg = time.time()
        pg_time = end_pg - start_pg
        print(f"PostgreSQL execution time: {pg_time:.4f} seconds")
    except Exception as e:
        pg_conn.rollback()
        print(f"PostgreSQL error: {e}")
        pg_time = None

    # MySQL Execution
    print(f"\n🟡 MySQL - Running: {query_key}")
    try:
        start_my = time.time()
        mysql_cursor.execute(sql)
        _ = mysql_cursor.fetchall()
        end_my = time.time()
        my_time = end_my - start_my
        print(f"MySQL execution time: {my_time:.4f} seconds")
    except Exception as e:
        print(f"MySQL error: {e}")
        my_time = None

    # Close connections
    pg_cursor.close()
    pg_conn.close()
    mysql_cursor.close()
    mysql_conn.close()

    # Summary
    print("\n🧾 Summary:")
    print(f"{'Query':<30} | {'PostgreSQL':>12} | {'MySQL':>12}")
    print("-" * 60)
    pg_disp = f"{pg_time:.4f}" if pg_time is not None else "ERROR"
    my_disp = f"{my_time:.4f}" if my_time is not None else "ERROR"
    print(f"{query_key:<30} | {pg_disp:>12} | {my_disp:>12}")



def explain_query(query_key):
    if query_key not in queries:
        print(f"Query key '{query_key}' not found.")
        return

    sql = queries[query_key]

    pg_conn, pg_cursor, mysql_conn, mysql_cursor = connect_to_databases()

    print(f"\n🔍 PostgreSQL EXPLAIN for: {query_key}")
    try:
        pg_cursor.execute("EXPLAIN " + sql)
        pg_plan = pg_cursor.fetchall()
        for row in pg_plan:
            print(row[0])
    except Exception as e:
        pg_conn.rollback()
        print("PostgreSQL EXPLAIN error:", e)

    print(f"\n🔍 MySQL EXPLAIN for: {query_key}")
    try:
        mysql_cursor.execute("EXPLAIN " + sql)
        columns = [desc[0] for desc in mysql_cursor.description]
        rows = mysql_cursor.fetchall()
        print(" | ".join(columns))
        for row in rows:
            print(" | ".join(str(col) for col in row))
    except Exception as e:
        print("MySQL EXPLAIN error:", e)

    pg_cursor.close()
    pg_conn.close()
    mysql_cursor.close()
    mysql_conn.close()


