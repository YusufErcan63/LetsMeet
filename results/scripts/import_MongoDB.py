import psycopg2
from pymongo import MongoClient
import random
import string

# PostgreSQL Verbindunng
DB_HOST = "localhost"
DB_NAME = "lf8_lets_meet_db"
DB_USER = "user"
DB_PASS = "secret"

# MongoDB Verbindung
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["LetsMeet"]
mongo_collection = mongo_db["users"]

# Verbindung zu PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cursor = conn.cursor()

# Funktion zur Generierung eines zufälligen Strings für den password_hash 
def generate_random_string(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# MongoDB-Benutzer durchlaufen und Daten verarbeiten
for mongo_user in mongo_collection.find():

    # (Datenextraktion aus MongoDB)
    user_email = mongo_user.get("_id")
    first_name = mongo_user.get("name", "").split(", ")[1]
    last_name = mongo_user.get("name", "").split(", ")[0]
    created_at = mongo_user.get("createdAt")
    updated_at = mongo_user.get("updatedAt")
    gender = mongo_user.get("gender")
    interested_in = mongo_user.get("interestedIn")
    birth_date = mongo_user.get("birthDate")
    phone = mongo_user.get("phone")

    # (Prüfen, ob der Benutzer bereits in der users-Tabelle existiert)
    cursor.execute("SELECT user_id FROM users WHERE email = %s", (user_email,))
    existing_user = cursor.fetchone()

    if existing_user:
        print(f"Benutzer mit E-Mail {user_email} existiert bereits. Daten werden aktualisiert.")
        user_id = existing_user[0]
    else:
        # (Einfügen in die 'users' Tabelle)
        cursor.execute("""
            INSERT INTO users (first_name, last_name, email, phone, gender, interested_in, birth_date, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING user_id
        """, (first_name, last_name, user_email, phone, gender, interested_in, birth_date, created_at, updated_at))
        user_id = cursor.fetchone()[0]

    # (Verarbeitung der Adresse und Einfügen in die 'address' Tabelle)
    if "address" in mongo_user:
        address = mongo_user["address"]
        cursor.execute("""
            INSERT INTO address (address_id, street, city, postal_code, user_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (address.get("address_id"), address.get("street"), address.get("city"), address.get("postal_code"), user_id))

    # (Verarbeitung der Hobbys und Einfügen in die 'user_hobby' Tabelle)
    for hobby_name in mongo_user.get("hobbies", []):
        # (Findet die hobby_id aus der 'hobby' Tabelle oder fügt es neu ein)
        cursor.execute("SELECT hobby_id FROM hobby WHERE hobby_name = %s", (hobby_name,))
        hobby_id_result = cursor.fetchone()
        if hobby_id_result:
            hobby_id = hobby_id_result[0]
        else:
            # (Neues Hobby einfügen, falls es nicht existiert)
            cursor.execute("INSERT INTO hobby (hobby_name) VALUES (%s) RETURNING hobby_id", (hobby_name,))
            hobby_id = cursor.fetchone()[0]

        # (Einfügen der Beziehung zwischen Benutzer und Hobby in die 'user_hobby' Tabelle)
        cursor.execute("""
            INSERT INTO user_hobby (user_id, hobby_id)
            VALUES (%s, %s) ON CONFLICT (user_id, hobby_id) DO NOTHING
        """, (user_id, hobby_id))

    # (Verarbeitung der Freundschaften und Einfügen in die 'friends' Tabelle)
    for friend_email in mongo_user.get("friends", []):
        # (Holt die user_id des Freundes)
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (friend_email,))
        friend_user_id = cursor.fetchone()
        if friend_user_id:
            friend_user_id = friend_user_id[0]
            # (Stellt sicher, dass user_id < friend_user_id ist, um Duplikate zu vermeiden)
            if user_id < friend_user_id:
                cursor.execute("""
                    INSERT INTO friends (user_id, friend_user_id)
                    VALUES (%s, %s) ON CONFLICT (user_id, friend_user_id) DO NOTHING
                """, (user_id, friend_user_id))
            else:
                cursor.execute("""
                    INSERT INTO friends (user_id, friend_user_id)
                    VALUES (%s, %s) ON CONFLICT (user_id, friend_user_id) DO NOTHING
                """, (friend_user_id, user_id))

    # (Verarbeitung von Likes und Einfügen in die 'like' Tabelle)
    for like in mongo_user.get("likes", []):
        liked_email = like["liked_email"]
        status = like["status"]
        timestamp = like["timestamp"]
        
        # (Holt die user_id des gelikten Benutzers)
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (liked_email,))
        liked_user_id_result = cursor.fetchone()

        if liked_user_id_result:
            liked_user_id = liked_user_id_result[0]

            # (Einfügen des Likes in die 'like' Tabelle)
            cursor.execute("""
                INSERT INTO like (user_id, liked_user_id, timestamp, status)
                VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, liked_user_id) DO UPDATE SET status = EXCLUDED.status, timestamp = EXCLUDED.timestamp
            """, (user_id, liked_user_id, timestamp, status))


    # (Verarbeitung von Nachrichten und Einfügen in die 'message' Tabelle)
    for message in mongo_user.get("messages", []):
        receiver_email = message["receiver_email"]
        message_text = message["message"]
        timestamp = message["timestamp"]

        # (Holt die user_id des Empfängers)
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (receiver_email,))
        receiver_user_id_result = cursor.fetchone()

        if receiver_user_id_result:
            receiver_user_id = receiver_user_id_result[0]
            
            # (Einfügen der Nachricht in die 'message' Tabelle)
            cursor.execute("""
                INSERT INTO message (sender_user_id, receiver_user_id, message_text, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (user_id, receiver_user_id, message_text, timestamp))

# Commit und trennung der Verbindung 
conn.commit()
cursor.close()
conn.close()

print("Datenimport aus MongoDB abgeschlossen!")
