import pyodbc
from datetime import datetime, timedelta
import pandas as pd


conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage4;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()


date_input = input("Entrez la date (format JJ/MM/AA): ")
service_id = int(input("Entrez l'ID du service: "))


date = datetime.strptime(date_input, "%d/%m/%y")


cursor.execute("SELECT TOP 1 profondeur_moyenne FROM transactionsmain2 WHERE id_service = ?", service_id)
result = cursor.fetchone()
if result:
    profondeur_moyenne = result[0]
else:
    print(f"Aucune profondeur moyenne trouvée pour le service {service_id}")
    conn.close()
    exit()


date_debut = date - timedelta(days=30*profondeur_moyenne)


nombre_jours = (date - date_debut).days + 1


query = """
SELECT DATEPART(HOUR, heure) as heure, COUNT(*) as nb_operations
FROM transactionsmain2
WHERE date BETWEEN ? AND ?
AND id_service = ?
GROUP BY DATEPART(HOUR, heure)
"""

df_jour = pd.read_sql(query, conn, params=(date, date, service_id))

df_periode = pd.read_sql(query, conn, params=(date_debut, date, service_id))

df_moyenne = df_periode.groupby('heure')['nb_operations'].sum().reset_index()
df_moyenne['nb_operations_moyenne'] = df_moyenne['nb_operations'] / nombre_jours

df_resultat = pd.merge(df_jour, df_moyenne[['heure', 'nb_operations_moyenne']], on='heure')

df_resultat['difference'] = df_resultat['nb_operations'] - df_resultat['nb_operations_moyenne']
df_resultat['inferieur_a_la_moyenne'] = df_resultat['difference'] < 0

df_resultat_filtre = df_resultat[df_resultat['inferieur_a_la_moyenne']]

cursor.execute("""
IF OBJECT_ID('ResultatsComparaison', 'U') IS NOT NULL 
    DROP TABLE ResultatsComparaison
CREATE TABLE ResultatsComparaison (
    date DATE,
    service_id INT,
    heure INT,
    nb_operations INT,
   nb_operations_moy FLOAT,
    difference FLOAT
)
""")

for _, row in df_resultat_filtre.iterrows():
    cursor.execute("""
    INSERT INTO ResultatsComparaison (date, service_id, heure, nb_operations,nb_operations_moy, difference)
    VALUES (?, ?, ?, ?, ?, ?)
    """, date, service_id, int(row['heure']), int(row['nb_operations']), float(row['nb_operations_moyenne']), float(row['difference']))

conn.commit()
conn.close()

print("Analyse terminée. Les résultats ont été insérés dans la table 'ResultatsComparaison'.")