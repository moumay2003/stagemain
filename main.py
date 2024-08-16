import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import pyodbc
from datetime import datetime, timedelta

# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage2;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def get_last_processed_date():
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='last_processed' AND xtype='U')
    CREATE TABLE last_processed (last_date DATE);
    
    SELECT TOP 1 last_date FROM last_processed;
    """)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

def update_last_processed_date(new_date):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM last_processed; INSERT INTO last_processed (last_date) VALUES (?)", (new_date,))
    conn.commit()
    cursor.close()

def check_for_new_data():
    last_processed_date = get_last_processed_date()
    cursor = conn.cursor()
    
    if last_processed_date:
        cursor.execute("SELECT TOP 1 date FROM NOPJ4 WHERE date > ? ORDER BY date DESC", (last_processed_date,))
    else:
        cursor.execute("SELECT TOP 1 date FROM NOPJ4 ORDER BY date DESC")
    
    most_recent_date = cursor.fetchone()
    cursor.close()
    
    if most_recent_date:
        most_recent_date = most_recent_date[0]
        if not last_processed_date or most_recent_date > last_processed_date:
            return True, most_recent_date
    
    return False, None

def load_data_from_sql():
    query = """
    SELECT TOP 240 date, nombre_operations
    FROM NOPJ4
    ORDER BY date DESC
    """
    data = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
    return data

def fit_arima_model(data, order=(12,1,12)):
    model = ARIMA(data, order=order)
    fitted_model = model.fit()
    return fitted_model

def make_predictions2(model, steps=125):
    forecast = model.forecast(steps=steps)
    return forecast

def save_predictions2_to_sql(start_date, predictions2):
    cursor = conn.cursor()
    
    for i, prediction in enumerate(predictions2):
        date = start_date + timedelta(days=i)
        
        cursor.execute("SELECT COUNT(*) FROM predictions2 WHERE date = ?", (date.date(),))
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute("""
            INSERT INTO predictions2 (date, predicted_operations)
            VALUES (?, ?)
            """, (date.date(), float(prediction)))
            print(f"Nouvelle prédiction insérée pour {date.date()}")
    
    conn.commit()
    cursor.close()

def check_and_predict():
    new_data_available, most_recent_date = check_for_new_data()
    
    if new_data_available:
        print(f"Nouvelles données détectées jusqu'au {most_recent_date}. Mise à jour des prédictions...")
        
        # Charger les 240 derniers jours de données, triées par date DESC
        data = load_data_from_sql()
        
        # Obtenir la dernière date dans les données
        last_date = data.index[0]
        next_date = last_date + timedelta(days=1)
        
        # Ajuster le modèle ARIMA sur les 240 derniers jours
        model = fit_arima_model(data['nombre_operations'][::-1])
        
        # Faire des prédictions pour les 125 prochains jours
        predictions2 = make_predictions2(model)
        
        # Sauvegarder les nouvelles prédictions
        save_predictions2_to_sql(next_date, predictions2)
        
        # Mettre à jour la date du dernier traitement
        update_last_processed_date(most_recent_date)
        
        print(f"Nouvelles prédictions faites pour les 125 jours à partir de {next_date.date()}")
    else:
        print("Pas de nouvelles données. Aucune mise à jour des prédictions n'est nécessaire.")

if __name__ == "__main__":
    check_and_predict()
    conn.close()