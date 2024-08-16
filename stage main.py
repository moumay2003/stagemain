import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import pyodbc
from datetime import datetime, timedelta
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
from datetime import date


# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage2;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def load_data_from_sql():
    query = "SELECT TOP 261 date, nombre_operations FROM NOPJ4 ORDER BY date DESC"
    data = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
    return data.sort_index()

def fit_arima_model(data, order=(12,1,12)):
    model = ARIMA(data, order=order)
    fitted_model = model.fit()
    return fitted_model

def make_predictions(model, steps=125):
    forecast = model.forecast(steps=steps)
    return forecast

def save_predictions_to_sql(start_date, predictions):
    cursor = conn.cursor()
    
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='predictions' AND xtype='U')
    CREATE TABLE predictions2 (
        date DATE PRIMARY KEY,
        predicted_operations FLOAT
    )
    """)
    
    for i, prediction in enumerate(predictions):
        date = start_date + timedelta(days=i)
        
        # Vérifier si une prédiction existe déjà pour cette date
        cursor.execute("SELECT COUNT(*) FROM predictions2 WHERE date = ?", (date.date(),))
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insérer seulement si la prédiction n'existe pas déjà
            cursor.execute("""
            INSERT INTO predictions2 (date, predicted_operations)
            VALUES (?, ?)
            """, (date.date(), float(prediction)))
            print(f"Nouvelle prédiction insérée pour {date.date()}")
        else:
            print(f"Prédiction existante pour {date.date()}, pas d'insertion")
    
    conn.commit()
    cursor.close()

def check_and_predict():
    # Charger les données
    data = load_data_from_sql()
    
    # Vérifier si une nouvelle ligne a été ajoutée
    last_date = data.index[-1]
    next_date = last_date + timedelta(days=1)
    
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 date FROM predictions2 ORDER BY date DESC")
    last_prediction_date = cursor.fetchone()
    
    if last_prediction_date is None or next_date.date() > last_prediction_date[0]:
        # Ajuster le modèle ARIMA
        model = fit_arima_model(data['nombre_operations'])
        
        # Faire des prédictions pour les 125 prochains jours
        predictions = make_predictions(model)
        
        # Sauvegarder les prédictions
        save_predictions_to_sql(next_date, predictions)
        
        print(f"Nouvelles prédictions faites pour les 125 jours à partir de {next_date.date()}")
    else:
        print("Pas de nouvelle donnée, pas de nouvelles prédictions.")
    
    cursor.close()
def compare_and_insert(conn, current_date):
    cursor = conn.cursor()
    
    cursor.execute("SELECT predicted_operations FROM predictions2 WHERE date = ?", (current_date,))
    prediction = cursor.fetchone()
    
    if prediction:
        predicted_value = prediction[0]
        
        cursor.execute("SELECT nombre_operations FROM NOPJ4 WHERE date = ?", (current_date,))
        actual = cursor.fetchone()
        
        if actual:
            actual_value = actual[0]
            
            if actual_value < predicted_value:
                cursor.execute("""
                INSERT INTO operations_alert (date, actual_operations, predicted_operations)
                VALUES (?, ?, ?)
                """, (current_date, actual_value, predicted_value))
                conn.commit()
    
    cursor.close()    

 
if __name__ == "__main__":
    check_and_predict()
    
    
    
    
    
 
    
    current_date = date(2024, 4, 5)
    compare_and_insert(conn, current_date)
    
    compare_and_insert(conn, current_date)
    conn.close()