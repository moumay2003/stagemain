import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pyodbc
from datetime import datetime, timedelta
import joblib
import os

conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage3;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def load_data_from_sql(id_service):
    query = """
    SELECT date, nombre_operations
    FROM NOPJ
    WHERE id_service = ?
    ORDER BY date DESC
    """
    data = pd.read_sql(query, conn, params=(id_service,), index_col='date', parse_dates=['date'])
    return data.sort_index() 

def fit_arima_model(data, order=(12, 1, 12)):
    model = ARIMA(data, order=order)
    fitted_model = model.fit()
    return fitted_model

def predict_future(fitted_model, steps=50):
    forecast = fitted_model.get_forecast(steps=steps)
    forecast_mean = forecast.predicted_mean
    confidence_intervals = forecast.conf_int()
    return forecast_mean, confidence_intervals

def calculate_errors(true_values, predicted_values):
    mae = mean_absolute_error(true_values, predicted_values)
    rmse = np.sqrt(mean_squared_error(true_values, predicted_values))
    return mae, rmse

def plot_results(data, forecast, confidence_intervals):
    plt.figure(figsize=(10, 5))
    plt.plot(data, label='Observations')
    plt.plot(forecast, label='Prévisions', color='red')
    plt.fill_between(forecast.index,
                     confidence_intervals.iloc[:, 0],
                     confidence_intervals.iloc[:, 1],
                     color='pink', alpha=0.3)
    plt.legend()
    plt.title('Prévisions des opérations')
    plt.xlabel('Date')
    plt.ylabel('Nombre d\'opérations')
    plt.show()

def save_model(model, id_service):
    model_dir = 'models'
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
    model_path = os.path.join(model_dir, f'arima_model_{id_service}.pkl')
    joblib.dump((model, datetime.now()), model_path)
    print(f"Modèle sauvegardé : {model_path}")

def load_model(id_service):
    model_path = os.path.join('models', f'arima_model_{id_service}.pkl')
    if os.path.exists(model_path):
        try:
            loaded_data = joblib.load(model_path)
            if isinstance(loaded_data, tuple) and len(loaded_data) == 2:
                model, timestamp = loaded_data
                if datetime.now() - timestamp < timedelta(hours=48):
                    return model
            else:
                print("Ancien format de modèle détecté, un nouveau modèle sera entraîné.")
        except Exception as e:
            print(f"Erreur lors du chargement du modèle : {e}")
    return None

if __name__ == "__main__":
    id_service = int(input("Veuillez entrer l'ID du service pour les prévisions : "))
    data = load_data_from_sql(id_service)
    data_series = data['nombre_operations']
    
    if data_series.empty:
        print("Aucune donnée trouvée pour ce service.")
    else:
        fitted_model = load_model(id_service)
        
        if fitted_model is None:
            print("Entraînement d'un nouveau modèle...")
            fitted_model = fit_arima_model(data_series)
            save_model(fitted_model, id_service)
        else:
            print("Modèle existant chargé.")
        
        forecast, confidence_intervals = predict_future(fitted_model, steps=50)
        observed_future_values = [data_series[-1]] * len(forecast)
        mae, rmse, mape = calculate_errors(observed_future_values, forecast)
        print(f"Erreur absolue moyenne (MAE): {mae}")
        print(f"Erreur quadratique moyenne (RMSE): {rmse}")
        print(f"Erreur (MAPE): {mape}")
        plot_results(data_series, forecast, confidence_intervals)

conn.close()