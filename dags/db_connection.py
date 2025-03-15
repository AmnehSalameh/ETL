import mysql.connector
import logging 

"""Execute an SQL query (INSERT, UPDATE, DELETE, SELECT)"""
def execute_query(query, params={},host="host.docker.internal", user="root", password="root", database="ETL"):
    try:
        # Establishing connection
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query, params or ())
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
        else:
            conn.commit()
            result =  cursor.lastrowid
        return result  # or any other useful value

    except mysql.connector.Error as err:
        logging.error(f"Error executing query: {err}")
        return None

          