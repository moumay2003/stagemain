import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from datetime import datetime

# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage3;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def plot_operations_per_hour(date, service_id):
    # Requête SQL pour obtenir les données filtrées
    query = f"""
    SELECT DATEPART(HOUR, heure) as hour, COUNT(*) as count
    FROM transactionsmain2
    WHERE date = '{date}' AND id_service = {service_id}
    GROUP BY DATEPART(HOUR, heure)
    ORDER BY DATEPART(HOUR, heure)
    """
    
    # Exécuter la requête et récupérer les résultats
    df = pd.read_sql(query, conn)
    
    # Créer un DataFrame avec toutes les heures (0-23)
    all_hours = pd.DataFrame({'hour': range(24)})
    df = all_hours.merge(df, on='hour', how='left').fillna(0)
    
    # Tracer le graphique en bâtons
    plt.figure(figsize=(12, 6))
    plt.bar(df['hour'], df['count'], width=0.8)
    plt.title(f"Nombre d'opérations par heure pour le service {service_id} le {date}")
    plt.xlabel("Heure")
    plt.ylabel("Nombre d'opérations")
    plt.xticks(range(24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Ajouter les valeurs au-dessus des bâtons
    for i, v in df.iterrows():
        plt.text(v['hour'], v['count'], str(int(v['count'])), ha='center', va='bottom')
    
    plt.tight_layout()
    plt.show()

# Exemple d'utilisation
date = "2023-08-23"  # Format: AAAA-MM-JJ (adapté au format SQL Server)
service_id = 1       # ID du service

plot_operations_per_hour(date, service_id)

# Fermer la connexion
conn.close()