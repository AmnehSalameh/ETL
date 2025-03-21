from db_connection import execute_query
import logging

def insert_ip_details(ip,country,city, state, postcode, latitude, longitude):
    """Insert IP details to the database."""
    #db = MySQLConnection()
    #if db.conn:
    query = "INSERT INTO ip_details (IP,country,city, state, postcode, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s,%s)"
    execute_query(query, (ip,country,city, state, postcode, latitude, longitude))
    #db.close_connection()
    logging.info(f"Inserted IP {ip}") 


def insert_date_details(date,year,month,day,weekday,quarter):
    """Insert date to the database."""
    #db = MySQLConnection()
    #if db.conn:
    query = "INSERT INTO ip_details (date,year,month,day,weekday,quarter) VALUES (%s, %s, %s, %s, %s, %s)"
    execute_query(query, (date,year,month,day,weekday,quarter))
    #db.close_connection()
    logging.info(f"Inserted date {date}") 

def insert_user_agent_details(browser,version_string,os,device_type,engine):
    """Insert user agent to the database."""
    query = "INSERT INTO user_agent (browser,version_string,os,device_type,engine) VALUES (%s, %s, %s, %s, %s)"
    execute_query(query, (browser,version_string,os,device_type,engine))
    #db.close_connection()
    logging.info(f"Inserted date {browser}")        

    
def insert_time_details(time,hour,minute,second):
    """Insert date to the database."""
    #db = MySQLConnection()
    #if db.conn:
    query = "INSERT INTO ip_details (time,hour,minute,second) VALUES (%s, %s, %s, %s)"
    execute_query(query, (time,hour,minute,second))
    #db.close_connection()
    logging.info(f"Inserted date {time}") 