import pandas as pd
import psycopg2
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from pymongo import MongoClient

## Verbindung von Import von Excel, MongoDB und XML in PostgreSQL
EXCEL_FILE = "Lets Meet DB Dump.xlsx"
XML_FILE   = "Lets_Meet_Hobbies.xml"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "LetsMeet"
MONGO_COLLECTION = "users"

POSTGRES_HOST = "localhost"
POSTGRES_DB   = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PWD  = "secret"
POSTGRES_PORT = 5432


def main():
    """
    Hauptprogramm:
     1) Verbindung zur Postgres-DB herstellen
     2) Excel importieren
     3) MongoDB importieren
     4) XML importieren
     5) Verbindung schließen
    """
    # 1) PostgreSQL-Verbindung
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD,
        port=POSTGRES_PORT
    )
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()

    # 2) Excel-Daten importieren
    import_from_excel(cursor, conn)

    # 3) MongoDB-Daten importieren
    import_from_mongo(cursor, conn)

    # 4) XML-Daten importieren
    import_from_xml(cursor, conn)

    # 5) Verbindung schließen
    cursor.close()
    conn.close()
    print("Alle Importe (Excel, MongoDB, XML) erfolgreich abgeschlossen.")
    
    def import_from_excel(cursor, conn):
    """
    Liest die Excel-Datei:
     1) Nachname, Vorname
     2) Straße Nr, PLZ Ort
     3) Telefon
     4) Hobbies (z.B. "Kochen %80%; Joggen %20%; ...")
     5) E-Mail
     6) Geschlecht (m / w / nicht binär / ...)
     7) Interessiert an (wird hier ignoriert)
     8) Geburtsdatum (z.B. 07.03.1959)

    und speichert direkt in addresses, users, hobbies, user_hobbies.
    Doppelte E-Mails werden verhindert ("ON CONFLICT").
    Gleiche Hobbies -> "name UNIQUE" + ON CONFLICT.
    Gleiche Addressen -> get_or_create_address(...).
    """
    print("Starte Excel-Import...")
    df = pd.read_excel(EXCEL_FILE, sheet_name=0)

    # Spalten umbenennen
    df.columns = [
        "nachname_vorname",
        "strasse_plz_ort",
        "telefon",
        "hobbies_raw",
        "email",
        "geschlecht",
        "interessiert_an",
        "geburtsdatum"
    ]

    for _, row in df.iterrows():
        #Name
        name_str = str(row["nachname_vorname"]) if pd.notnull(row["nachname_vorname"]) else ""
        first_name, last_name = split_name_simple(name_str)

        #Adresse
        addr_str = str(row["strasse_plz_ort"]) if pd.notnull(row["strasse_plz_ort"]) else ""
        street, house_no, zip_code, city = parse_address(addr_str)

        # -> get_or_create_address
        address_id = get_or_create_address(cursor, street, house_no, zip_code, city)

        #elefon bereinigen
        row_telefon = str(row["telefon"]) if pd.notnull(row["telefon"]) else None
        if row_telefon:
            row_telefon = re.sub(r"[^0-9+]", "", row_telefon)

        #Geschlecht + Geburtsdatum
        gender = str(row["geschlecht"]) if pd.notnull(row["geschlecht"]) else None
        birth_date = parse_date_ddmmYYYY(str(row["geburtsdatum"]))

        #E-Mail
        email = str(row["email"]) if pd.notnull(row["email"]) else None

        #Interessiert an
        interested_in_value = str(row["interessiert_an"]) if pd.notnull(row["interessiert_an"]) else None

        #Hobbys
        hobbies_str = str(row["hobbies_raw"]) if pd.notnull(row["hobbies_raw"]) else ""
        hobby_entries = [h.strip() for h in hobbies_str.split(";") if h.strip()]
        

        #User anlegen
        user_id = get_or_create_user(
            cursor=cursor,
            first_name=first_name,
            last_name=last_name,
            phone=row_telefon,
            email=email,
            gender=gender,
            birth_date=birth_date,
            address_id=address_id,
            interested_in=interested_in_value
        )
        if not user_id:
            # z.B. E-Mail leer oder bereits existierend -> next
            continue

        #user_hobbies
        for hpart in hobby_entries:
            match = re.search(r"(.*?)%(\d+)%", hpart)
            if match:
                hobby_name = match.group(1).strip()
                priority_val = int(match.group(2))
                hobby_id = get_or_create_hobby(cursor, hobby_name)
                # user_hobbies
                insert_user_hobbies = """
                    INSERT INTO user_hobbies (user_id, hobby_id, priority)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(insert_user_hobbies, (user_id, hobby_id, priority_val))
            else:
                #Falls kein %NN% => priority=0
                hobby_name = hpart
                if hobby_name:
                    hobby_id = get_or_create_hobby(cursor, hobby_name)
                    insert_user_hobbies = """
                        INSERT INTO user_hobbies (user_id, hobby_id, priority)
                        VALUES (%s, %s, 0)
                        ON CONFLICT DO NOTHING
                    """
                    cursor.execute(insert_user_hobbies, (user_id, hobby_id))

    conn.commit()
    print("Excel-Import abgeschlossen.")

