import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import pyodbc
from datetime import datetime, timedelta

# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage3;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def get_last_processed_date(id_service):
    cursor = conn.cursor()
    cursor.execute("""
    SELECT last_date FROM last_processed WHERE id_service = ?
    """, (id_service,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

def update_last_processed_date(id_service, new_date):
    cursor = conn.cursor()
    
    # Utiliser MERGE pour insérer ou mettre à jour la date du dernier traitement
    cursor.execute("""
    MERGE last_processed AS target
    USING (VALUES (?, ?)) AS source (id_service, last_date)
    ON target.id_service = source.id_service
    WHEN MATCHED THEN
        UPDATE SET last_date = source.last_date
    WHEN NOT MATCHED THEN
        INSERT (id_service, last_date) VALUES (source.id_service, source.last_date);
    """, (id_service, new_date))
    
    conn.commit()
    cursor.close()

def check_for_new_data():
    last_processed_dates = {}
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT id_service FROM NOPJ")
    services = cursor.fetchall()
    
    for service in services:
        id_service = service[0]
        last_processed_date = get_last_processed_date(id_service)
        
        if last_processed_date:
            cursor.execute("SELECT TOP 1 date FROM NOPJ WHERE id_service = ? AND date > ? ORDER BY date DESC", (id_service, last_processed_date))
        else:
            cursor.execute("SELECT TOP 1 date FROM NOPJ WHERE id_service = ? ORDER BY date DESC", (id_service,))
        
        most_recent_date = cursor.fetchone()
        
        if most_recent_date:
            most_recent_date = most_recent_date[0]
            if not last_processed_date or most_recent_date > last_processed_date:
                last_processed_dates[id_service] = most_recent_date
    
    cursor.close()
    
    return last_processed_dates

def load_data_from_sql(id_service):
    query = """
    SELECT date, nombre_operations
    FROM NOPJ
    WHERE id_service = ? AND date > (SELECT ISNULL(MAX(last_date), '1900-01-01') FROM last_processed WHERE id_service = ?)
    ORDER BY date DESC
    """
    data = pd.read_sql(query, conn, params=(id_service, id_service), parse_dates=['date'])
    return data

def fit_arima_model(data, order=(12,1,12)):
    model = ARIMA(data, order=order)
    fitted_model = model.fit()
    return fitted_model

def make_predictions(model, steps=125):
    forecast = model.forecast(steps=steps)
    return forecast

def save_predictions_to_sql(id_service, start_date, predictions):
    cursor = conn.cursor()
    
    for i, prediction in enumerate(predictions):
        date = start_date + timedelta(days=i)
        
        cursor.execute("SELECT COUNT(*) FROM predictions WHERE date = ? AND id_service = ?", (date.date(), id_service))
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute("""
            INSERT INTO predictions (date, id_service, predicted_operations)
            VALUES (?, ?, ?)
            """, (date.date(), id_service, float(prediction)))
            print(f"Nouvelle prédiction insérée pour {date.date()} pour le service {id_service}")
    
    conn.commit()
    cursor.close()

def check_and_predict():
    new_data_available = check_for_new_data()
    
    if new_data_available:
        print(f"Nouvelles données détectées. Mise à jour des prédictions...")
        
        for id_service, most_recent_date in new_data_available.items():
            print(f"Mise à jour des prévisions pour le service {id_service} jusqu'au {most_recent_date}.")
            
            # Charger les données pour ce service
            data = load_data_from_sql(id_service)
            
            # Obtenir les données les plus récentes
            last_date = data['date'].max()
            next_date = last_date + timedelta(days=1)
            
            # Ajuster le modèle ARIMA sur les données du service
            model = fit_arima_model(data.set_index('date')['nombre_operations'])
            
            # Faire des prédictions pour les 125 prochains jours
            predictions = make_predictions(model)
            
            # Sauvegarder les nouvelles prédictions pour ce service
            save_predictions_to_sql(id_service, next_date, predictions)
            
            # Mettre à jour la date du dernier traitement
            update_last_processed_date(id_service, most_recent_date)
        
        print("Mise à jour des prédictions terminée.")
    else:
        print("Pas de nouvelles données. Aucune mise à jour des prédictions n'est nécessaire.")

if __name__ == "__main__":
    check_and_predict()
    conn.close()
