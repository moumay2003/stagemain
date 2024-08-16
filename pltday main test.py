import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from datetime import datetime, timedelta

conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage4;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def plot_operations_per_hour(date, service_id):
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 profondeur_moyenne FROM transactionsmain3 WHERE id_service = ?", service_id)
    result = cursor.fetchone()
    if result:
        profondeur_moyenne = result[0]
    else:
        print(f"Aucune profondeur moyenne trouvée pour le service {service_id}")
        return

    date_debut = date_obj - timedelta(days=30*profondeur_moyenne)
    nombre_jours = (date_obj - date_debut).days + 1

    query_jour = f"""
    SELECT DATEPART(HOUR, heure) as hour, COUNT(*) as count
    FROM transactionsmain3
    WHERE date = '{date}' AND id_service = {service_id}
    GROUP BY DATEPART(HOUR, heure)
    """

    query_periode = f"""
    SELECT DATEPART(HOUR, heure) as hour, COUNT(*) as count
    FROM transactionsmain3
    WHERE date BETWEEN '{date_debut.strftime('%Y-%m-%d')}' AND '{date}'
    AND id_service = {service_id}
    GROUP BY DATEPART(HOUR, heure)
    """

    df_jour = pd.read_sql(query_jour, conn)
    df_periode = pd.read_sql(query_periode, conn)

    df_moyenne = df_periode.groupby('hour')['count'].sum().reset_index()
    df_moyenne['count_moyenne'] = df_moyenne['count'] / nombre_jours

    all_hours = pd.DataFrame({'hour': range(24)})
    df_jour = all_hours.merge(df_jour, on='hour', how='left').fillna(0)
    df_moyenne = all_hours.merge(df_moyenne, on='hour', how='left').fillna(0)

    plt.figure(figsize=(14, 7))

    bars = plt.bar(df_jour['hour'], df_jour['count'], width=0.8, label='Nombre d\'opérations')

    plt.plot(df_moyenne['hour'], df_moyenne['count_moyenne'], color='red', linestyle='--', label='Moyenne')

    for i, (bar, jour_count, moy_count) in enumerate(zip(bars, df_jour['count'], df_moyenne['count_moyenne'])):
        if jour_count < moy_count:
            bar.set_color('orange')

    plt.title(f"Nombre d'opérations par heure pour le service {service_id} le {date}")
    plt.xlabel("Heure")
    plt.ylabel("Nombre d'opérations")
    plt.xticks(range(24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    for i, v in df_jour.iterrows():
        plt.text(v['hour'], v['count'], str(int(v['count'])), ha='center', va='bottom')

    plt.legend()
    plt.tight_layout()
    plt.show()

date = "2024-08-16"  # Format: AAAA-MM-JJ 
service_id = 4  # ID du service
plot_operations_per_hour(date, service_id)

# Fermer la connexion
conn.close()
