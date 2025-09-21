import pandas as pd
import psycopg2
from datetime import datetime

# (Konfiguration für die PostgreSQL-Datenbankverbindung)
DB_HOST = "localhost"
DB_NAME = "lf8_lets_meet_db"
DB_USER = "user"
DB_PASS = "secret"

# (Pfad zur Excel-Datei)
EXCEL_FILE_PATH = "/workspace/LetsMeet/Lets Meet DB Dump.xlsx"

def parse_name(full_name):
    #Funktion, um Namen aus dem Format 'Nachname, Vorname' zu trennen.

    if not full_name or not isinstance(full_name, str) or ',' not in full_name:
        return (None, None)
    try:
        last_name, first_name = map(str.strip, full_name.split(',', 1))
        return (last_name, first_name)
    except:
        return (None, None)

def parse_gender(gender_str):
    #Funktion, um den Geschlechts-String zu normalisieren.
    
    if not isinstance(gender_str, str):
        return None
    gender_str = gender_str.strip().lower()
    if gender_str in ['m', 'w', 'nb']:
        return gender_str
    return None

def parse_interested_in(value):
    #Funktion, um den String 'Interessiert an' zu verarbeiten.
    
    if not isinstance(value, str):
        return None
    return value.strip()

def parse_birthday(birth_date):
    #Funktion, um das Geburtsdatum zu parsen.
    
    if pd.isnull(birthday):
        return None
    try:
        birthday_parsed = pd.to_datetime(birthday, dayfirst=True, errors='coerce')
        if pd.isnull(birthday_parsed):
            return None
        return birthday_parsed.date()
    except:
        return None

def parse_hobbies(hobbies_str):
    #Funktion, um Hobbys und deren Priorität aus dem String 'Hobby1%Prio;Hobby2%Prio;...' zu extrahieren.
    
    if not hobbies_str or not isinstance(hobbies_str, str):
        return []
    items = hobbies_str.split(';')
    result = []
    for item in items:
        parts = item.split('%')
        if len(parts) < 2:
            continue
        hobby_name = parts[0].strip()
        prio_val = 0
        if len(parts) >= 2:
            try:
                prio_val = int(parts[1])
            except:
                prio_val = 0
        if hobby_name:
            result.append((hobby_name, prio_val))
    return result

def main():
    conn = None
    cursor = None

    try:
        # (Verbindung zur PostgreSQL-Datenbank herstellen)
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = False
        cursor = conn.cursor()
        print("Verbindung zu PostgreSQL erfolgreich hergestellt.")

        # (Liest die Excel-Datei in einen pandas DataFrame)
        df = pd.read_excel(EXCEL_FILE_PATH)
        df.columns = df.columns.str.strip()
        print("Excel-Datei erfolgreich gelesen.")

        # (Durchläuft über jede Zeile des DataFrames)
        for index, row in df.iterrows():
            try:
                # (Extrahiert Daten aus der aktuellen Zeile)
                full_name = row.get('Nachname, Vorname')
                last_name, first_name = parse_name(full_name)
                
                email = row.get('E-Mail')
                if not isinstance(email, str) or not first_name:
                    print(f"Zeile {index}: Ungültiger Name oder E-Mail. Wird übersprungen.")
                    continue
                email = email.strip()

                gender_raw = row.get('Geschlecht (m/w/nonbinary)')
                gender = parse_gender(gender_raw)

                interested_in = parse_interested_in(row.get('Interessiert an'))
                birthday = parse_birthday(row.get('Geburtsdatum'))
                
                hobbies_str = row.get('Hobbys')
                hobbies = parse_hobbies(hobbies_str)

                # (Prüft, ob der Benutzer bereits in der users-Tabelle existiert)
                cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
                existing_user = cursor.fetchone()

                if existing_user:
                    # (Wenn der Benutzer existiert, werden seine Daten aktualisert)
                    user_id = existing_user[0]
                    update_users_sql = """
                       UPDATE users
                          SET gender = %s,
                              interested_in = %s,
                              birth_date = %s,
                              updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                        """
                    cursor.execute(update_users_sql, (
                        gender,
                        interested_in,
                        birthday,
                        user_id
                    ))
                    conn.commit()
                    # (zur nächsten Zeile, da der Benutzer bereits vorhanden ist)
                    continue

                # (Wenn der Benutzer nicht existiert, fügt ihn neu ein)
                insert_user_sql = """
                    INSERT INTO users (first_name, last_name, email, gender, interested_in, birth_date, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING user_id
                """
                cursor.execute(insert_user_sql, (
                    first_name,
                    last_name,
                    email,
                    gender,
                    interested_in,
                    birthday
                ))
                user_id = cursor.fetchone()[0]

                # (Fügt Hobbys in die 'hobby' und 'user_hobby' Tabellen ein)
                for (hobby_name, prio) in hobbies:
                    # (Prüfe, ob das Hobby schon existiert)
                    cursor.execute("SELECT hobby_id FROM hobby WHERE hobby_name = %s", (hobby_name,))
                    row_hobby = cursor.fetchone()
                    if row_hobby:
                        hobby_id = row_hobby[0]
                    else:
                        # (Wenn nicht, fügt das Hobby hinzu)
                        cursor.execute("INSERT INTO hobby (hobby_name) VALUES (%s) RETURNING hobby_id",
                                       (hobby_name,))
                        hobby_id = cursor.fetchone()[0]

                    # (Fügt die Beziehung zwischen Benutzer und Hobby in die user_hobby Tabelle ein)
                    cursor.execute("""
                        INSERT INTO user_hobby (user_id, hobby_id, priority)
                        VALUES (%s, %s, %s) ON CONFLICT (user_id, hobby_id) DO UPDATE SET priority = EXCLUDED.priority
                    """, (user_id, hobby_id, prio))
                
                conn.commit()

        

if __name__ == "__main__":
    main()
    print("Datenimport abgeschlossen.")
