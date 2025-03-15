from db_connection import execute_query
import logging

def insert_ip_details(ip,country,city, state, postcode, latitude, longitude):
    """Insert IP details the database."""
    #db = MySQLConnection()
    #if db.conn:
    query = "INSERT INTO ip_details (IP,country,city, state, postcode, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s,%s)"
    execute_query(query, (ip,country,city, state, postcode, latitude, longitude))
    print("-->>>>> ",query)
        #db.close_connection()
    logging.info(f"Inserted IP {ip}") 
