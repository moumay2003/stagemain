import pandas as pd
import pyodbc
from datetime import date, datetime,timedelta

# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage3;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def get_latest_date(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM NOPJ")
    latest_date = cursor.fetchone()[0]
    cursor.close()
    return latest_date

def compare_and_insert(conn, start_date):
    cursor = conn.cursor()
    
    # Récupérer la date la plus récente
    end_date = get_latest_date(conn)
    
    # Récupérer tous les id_service distincts
    cursor.execute("SELECT DISTINCT id_service FROM NOPJ")
    services = [row.id_service for row in cursor.fetchall()]
    
    # Boucle sur toutes les dates de start_date à end_date
    current_date = start_date
    while current_date <= end_date:
        for service in services:
            # Récupérer la prédiction pour le service et la date donnée
            cursor.execute("SELECT predicted_operations FROM predictions WHERE date = ? AND id_service = ?", (current_date, service))
            prediction = cursor.fetchone()
            
            if prediction:
                predicted_value = prediction[0]
                
                # Récupérer la valeur réelle pour le service et la date donnée
                cursor.execute("SELECT nombre_operations FROM NOPJ WHERE date = ? AND id_service = ?", (current_date, service))
                actual = cursor.fetchone()
                
                if actual:
                    actual_value = actual[0]
                    
                    if actual_value < predicted_value:
                        cursor.execute("""
                        INSERT INTO operations_alert (date, id_service, actual_operations, predicted_operations)
                        VALUES (?, ?, ?, ?)
                        """, (current_date, service, actual_value, predicted_value))
        
        # Passer au jour suivant
        current_date += timedelta(days=1)
    
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    # Définissez ici la date de début pour la vérification
    start_date = date(2024, 7, 9)  # Par exemple, à partir du 1er janvier 2024
    compare_and_insert(conn, start_date)
    conn.close()