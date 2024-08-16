import schedule
import time
import pyodbc
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def envoyer_email(sujet, corps, destinataire):
    expediteur = "mouadyaya3@gmail.com"
    mot_de_passe = "smiw iyof naoi ratx "
    message = MIMEMultipart()
    message['From'] = expediteur
    message['To'] = destinataire
    message['Subject'] = sujet
    message.attach(MIMEText(corps, 'plain'))
    with smtplib.SMTP('smtp.gmail.com', 587) as serveur:
        serveur.starttls()
        serveur.login(expediteur, mot_de_passe)
        texte = message.as_string()
        serveur.sendmail(expediteur, destinataire, texte)

def job():
   
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage4;UID=sa;PWD=May2015++'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

   
    heure_actuelle = datetime.now().hour
    date_actuelle = datetime.now().date()

    cursor.execute("SELECT DISTINCT id_service FROM transactionsmain3")
    services = [row.id_service for row in cursor.fetchall()]

    for service_id in services:
        
        cursor.execute("SELECT TOP 1 profondeur_moyenne FROM transactionsmain3 WHERE id_service = ?", service_id)
        result = cursor.fetchone()
        if not result:
            print(f"Aucune profondeur moyenne trouvée pour le service {service_id}")
            continue
        profondeur_moyenne = result[0]

        
        date_debut = date_actuelle - timedelta(days=30*profondeur_moyenne)
        nombre_jours = (date_actuelle - date_debut).days + 1

        
        query_actuelle = """
        SELECT COUNT(*) as nb_operations
        FROM transactionsmain3
        WHERE CAST(date AS DATE) = CAST(GETDATE() AS DATE)
        AND DATEPART(HOUR, heure) = ?
        AND id_service = ?
        """
        df_actuelle = pd.read_sql(query_actuelle, conn, params=(heure_actuelle, service_id))

      
        query_moyenne = """
        SELECT AVG(nb_operations) as moyenne
        FROM (
            SELECT CAST(date AS DATE) as date, DATEPART(HOUR, heure) as heure, COUNT(*) as nb_operations
            FROM transactionsmain3
            WHERE date BETWEEN ? AND ?
            AND id_service = ?
            AND DATEPART(HOUR, heure) = ?
            GROUP BY CAST(date AS DATE), DATEPART(HOUR, heure)
        ) subquery
        """
        df_moyenne = pd.read_sql(query_moyenne, conn, params=(date_debut, date_actuelle, service_id, heure_actuelle))

        nb_actuel = df_actuelle['nb_operations'].iloc[0]
        moyenne = df_moyenne['moyenne'].iloc[0]

        if nb_actuel < moyenne:
            sujet = f"Alerte : Baisse d'activité pour le service {service_id}"
            corps = f"""
            Le nombre de transactions pour le service {service_id} à {heure_actuelle}h est inférieur à la moyenne.
            Nombre actuel : {nb_actuel}
            Moyenne historique : {moyenne:.2f}
            """
            destinataire = "mouadmay10@gmail.com"
            envoyer_email(sujet, corps, destinataire)

    conn.close()
    print("Analyse terminée. Des e-mails ont été envoyés pour les services dont l'activité est inférieure à la moyenne.")

schedule.every().hour.do(job)

job()


while True:
    schedule.run_pending()
    time.sleep(1)