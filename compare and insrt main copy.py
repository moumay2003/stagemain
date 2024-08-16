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
    
    # Calculer la moyenne correcte pour chaque service
    cursor.execute("""
    SELECT 
        id_service, 
        CAST(SUM(nombre_operations) AS FLOAT) / COUNT(DISTINCT date) as avg_operations
    FROM NOPJ
    GROUP BY id_service
    """)
    service_averages = {row.id_service: row.avg_operations for row in cursor.fetchall()}
    
    # Créer la nouvelle table si elle n'existe pas
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='operations_below_average' AND xtype='U')
    CREATE TABLE operations_below_average (
        date DATE,
        id_service INT,
        actual_operations INT,
        predicted_operations INT,
        service_average FLOAT
    )
    """)
    
    # Boucle sur toutes les dates de start_date à end_date
    current_date = start_date
    while current_date <= end_date:
        for service, service_average in service_averages.items():
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
                    
                    if actual_value < service_average:
                        cursor.execute("""
                        INSERT INTO operations_below_average (date, id_service, actual_operations, predicted_operations, service_average)
                        VALUES (?, ?, ?, ?, ?)
                        """, (current_date, service, actual_value, predicted_value, service_average))
        
        # Passer au jour suivant
        current_date += timedelta(days=1)
    
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    # Définissez ici la date de début pour la vérification
    start_date = date(2024, 7, 9)  # Par exemple, à partir du 9 juillet 2024
    compare_and_insert(conn, start_date)
    conn.close()