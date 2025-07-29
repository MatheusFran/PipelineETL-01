import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import pymysql


load_dotenv()

# Recupera as variáveis
CHARSET = os.getenv("DB_CHARSET")
USER = os.getenv("DB_USER")
PORT = os.getenv("DB_PORT")
PASSWORD = os.getenv("DB_PASSWORD")
DATABASE = os.getenv("DB_NAME")
HOST = os.getenv("DB_HOST")


timeout = 10

def select_data():
    connection = pymysql.connect(
        charset=CHARSET,
        connect_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor,
        db=DATABASE,
        host=HOST,
        password=PASSWORD,
        read_timeout=timeout,
        port=PORT,
        user=USER,
        write_timeout=timeout,
    )

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT * FROM winners 
        """)
        result = cursor.fetchall()
        return result
    finally:
        connection.close()