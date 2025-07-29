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

def LoadData(**kwargs):
    data = kwargs['data']
    list_data = data.xcom_pull(task_id='transform_data')
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
        #Criação de tabelas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS area (
            id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
            name VARCHAR(255) NOT NULL,
            code VARCHAR(255) NOT NULL, 
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS winners (
            id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
            name VARCHAR(255) NOT NULL,
            shortName VARCHAR(255) NOT NULL,
            tla VARCHAR(50) NOT NULL,
            crest VARCHAR(255) NOT NULL,
            address VARCHAR(255) NOT NULL,
            website VARCHAR(255) NOT NULL,
            founded INTEGER NOT NULL,
            clubColors VARCHAR(255) NOT NULL,
            venue VARCHAR(255) NOT NULL,
            lastUpdated DATETIME NOT NULL,
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS winners (
            id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
            name VARCHAR(255) NOT NULL,
            code VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            emblem VARCHAR(255) NOT NULL,
            plan VARCHAR(255) NOT NULL,
            numberOfAvailableSeasons INT NOT NULL,
            lastUpdated DATETIME NOT NULL,
            area.flag VARCHAR(255) NOT NULL,
            currentSeason.winner VARCHAR(255) NOT NULL,
            currentSeason VARCHAR(255) NOT NULL,
            area_id FOREIGN KEY (area_id) REFERENCES areas (id),
            current_season_id FOREIGN KEY (current_season_id) REFERENCES seasons (id),
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seasons (
            id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
            start_date DATETIME NOT NULL,
            end_date DATETIME NOT NULL,
            current_matchday FLOAT NOT NULL,
            winner_id FOREIGN KEY (winner_id) REFERENCES winners (id), 
            )
        """)
        for _, row in list_data[0]:
            cursor.execute("""
                    INSERT INTO area (name, code) VALUES (%s, %s)
                """, (row['name'], row['code']))

        for _, row in list_data[1]:
            cursor.execute("""
                INSERT INTO winners (
                    name, shortName, tla, crest, address, website,
                    founded, clubColors, venue, lastUpdated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['name'], row['shortName'], row['tla'], row['crest'],
                row['address'], row['website'], row['founded'], row['clubColors'],
                row['venue'], row['lastUpdated']
            ))
        for _, row in list_data[2]:
            cursor.execute("""
                INSERT INTO seasons (
                    start_date, end_date, current_matchday, winner_id
                ) VALUES (%s, %s, %s, %s)
            """, (
                row['startDate'], row['endDate'], row['currentMatchday'], row['winnerId']
            ))

        for _, row in list_data[3]:
            cursor.execute("""
                INSERT INTO competitions (
                    name, code, type, emblem, plan,
                    numberOfAvailableSeasons, lastUpdated,
                    area_flag, currentSeason_winner, currentSeason,
                    area_id, current_season_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['name'], row['code'], row['type'], row['emblem'], row['plan'],
                row['numberOfAvailableSeasons'], row['lastUpdated'],
                row['area.flag'], row['currentSeason.winner'], row['currentSeason'],
                row['area_id'], row['current_season_id']
            ))
        connection.commit()
    finally:
        connection.close()