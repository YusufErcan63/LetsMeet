import xml.etree.ElementTree as ET
import psycopg2
import random
from typing import Optional, Tuple

# (Konfiguration der PostgreSQL-Datenbank)
DB_HOST = "localhost"
DB_NAME = "lf8_lets_meet_db"
DB_USER = "user"
DB_PASS = "secret"

def get_or_create_user(email: str, full_name: str, conn) -> int:
    """
    (Findet oder erstellt einen Benutzer basierend auf der E-Mail-Adresse und gibt die user_id zurück)
    """
    with conn.cursor() as cur:
        # (Prüfen, ob der Benutzer bereits existiert)
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        user = cur.fetchone()

        if user:
            return user[0]

        # (Namen aufteilen)
        if "," in full_name:
            last_name, first_name = map(str.strip, full_name.split(",", 1))
        else:
            first_name = full_name.strip()
            last_name = "Platzhalter"  # (Falls kein Nachname vorhanden ist)

        # (Neuen Benutzer in die 'users'-Tabelle einfügen, passend zu deinem Modell)
        cur.execute(
            """
            INSERT INTO users (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING user_id
            """,
            (first_name, last_name, email),
        )
        user_id = cur.fetchone()[0]

        conn.commit()
        return user_id

def insert_hobby_if_not_exists(hobby_name: str, conn) -> int:
    """
    (Prüft, ob ein Hobby existiert, fügt es ggf. hinzu und gibt die hobby_id zurück)
    """
    with conn.cursor() as cur:
        # (Prüfen, ob das Hobby existiert)
        cur.execute("SELECT hobby_id FROM hobby WHERE hobby_name = %s", (hobby_name,))
        hobby = cur.fetchone()

        if hobby:
            return hobby[0]

        # (Neues Hobby hinzufügen, passend zu deinem Modell)
        cur.execute("INSERT INTO hobby (hobby_name) VALUES (%s) RETURNING hobby_id", (hobby_name,))
        hobby_id = cur.fetchone()[0]

        conn.commit()
        return hobby_id

def insert_user_hobbies(user_id: int, hobbies: list[str], conn):
    """
    (Fügt die Beziehungen zwischen dem Benutzer und seinen Hobbys in die user_hobby-Tabelle ein)
    """
    with conn.cursor() as cur:
        for hobby_name in hobbies:
            hobby_id = insert_hobby_if_not_exists(hobby_name, conn)

            # (Prüfen, ob die Beziehung bereits existiert)
            cur.execute(
                "SELECT 1 FROM user_hobby WHERE user_id = %s AND hobby_id = %s",
                (user_id, hobby_id),
            )
            if not cur.fetchone():
                priority = random.randint(1, 10) # (Zufällige Priorität, da in XML nicht vorhanden)
                cur.execute(
                    "INSERT INTO user_hobby (user_id, hobby_id, priority) VALUES (%s, %s, %s)",
                    (user_id, hobby_id, priority),
                )

        conn.commit()

def process_xml_data():
    XML_FILE = "Lets_Meet_Hobbies.xml"
    
    try:
        tree = ET.parse(XML_FILE)
        root = tree.getroot()
    except FileNotFoundError:
        print(f"Fehler: Die Datei {XML_FILE} wurde nicht gefunden.")
        return
    except ET.ParseError as e:
        print(f"Fehler beim Parsen der XML-Datei: {e}")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        
        print("Starte den XML-Datenimport...")

        for user_element in root.findall("user"):
            email_element = user_element.find("email")
            name_element = user_element.find("name")
            hobbies_elements = user_element.findall("hobbies/hobby")

            email = email_element.text if email_element is not None else None
            full_name = name_element.text if name_element is not None else None
            hobbies = [h.text for h in hobbies_elements if h.text]

            if email and full_name:
                user_id = get_or_create_user(email, full_name, conn)
                insert_user_hobbies(user_id, hobbies, conn)

        print("XML-Import abgeschlossen.")


if __name__ == "__main__":
    main()
    print("Datenimport abgeschlossen.")
