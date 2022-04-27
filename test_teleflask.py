from telethon import TelegramClient, sync
from telethon.errors.rpcerrorlist import SessionPasswordNeededError
import configparser
import logging
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template

import sqlite3
import time


logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.INFO)

config = configparser.ConfigParser()
config.read("config.ini")

api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
phone = '+79124044916'
client = TelegramClient(username, api_id, api_hash)
client.connect()

app = Flask(__name__)


def create_db():
    try:
        sqlite_connection = sqlite3.connect('telethon_db.db')
        sqlite_create_table_query = '''CREATE TABLE telethon_comments (
                                    id INTEGER PRIMARY KEY,
                                    user TEXT NOT NULL,
                                    text text NOT NULL UNIQUE,
                                    data datetime);'''
        cursor = sqlite_connection.cursor()
        print("База данных подключена к SQLite")
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        print("Таблица SQLite создана")
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite:", error)

        
def get_db_connection():
    conn = sqlite3.connect('telethon_db.db')
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/')
def hello():
    conn = get_db_connection()
    posts = conn.execute('SELECT * FROM telethon_comments').fetchall()
    conn.close()
    return render_template('index.html', posts = posts)
    

def parse_comments(name_channel):
    COUNT = 0
    sqlite_connection = sqlite3.connect('telethon_db.db')
    cursor = sqlite_connection.cursor()
    logging.info("Подключен к SQLite")
    for message in client.iter_messages(name_channel, limit = 1):
        try:
            for comment in client.iter_messages(name_channel, reply_to = message.id):
                try:
                    sqlite_insert_with_param = """INSERT INTO telethon_comments
                                        (user, text, data)
                                        VALUES (?, ?, ?);"""
                    data_tuple = (comment.sender_id, comment.text, comment.date)
                    cursor.execute(sqlite_insert_with_param, data_tuple)
                    sqlite_connection.commit()
                except sqlite3.Error as error:
                    logging.info("Ошибка при работе с SQLite '%s'", error)
                COUNT += 1
            logging.info("Downloaded '%s' comments", COUNT)
        except: 
                logging.info("The message '%s' is empty", message.id)
    cursor.close()
    client.disconnect()
    app.run(debug=False)


if __name__ == "__main__":
    logging.info("Parser started!")
    HOURS_FOR_PARSE = 24
    NOW = datetime.utcnow().replace(tzinfo=None)
    name_channel = 'mudak'
    create_db()
    parse_comments(name_channel)    