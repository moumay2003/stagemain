import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pyodbc
from datetime import date, timedelta

conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage3;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def load_data_from_sql():
    query = """
    SELECT reference, amount, date, heure, service, id_service
    FROM transactionsmain2
    ORDER BY date, heure
    """
    data = pd.read_sql(query, conn)
    data['date'] = pd.to_datetime(data['date'])
    data['heure'] = pd.to_datetime(data['heure'], format='%H:%M:%S').dt.time
    return data

def process_data(data):
    daily_ops = data.groupby([data['date'].dt.date, 'service', 'id_service']).size().reset_index(name='nombre_operations')
    daily_ops['date'] = pd.to_datetime(daily_ops['date'])
    return daily_ops

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
    
    # Calcul du sMAPE
    smape = 100 * np.mean(2 * np.abs(predicted_values - true_values) / (np.abs(predicted_values) + np.abs(true_values)))
    
    return mae, rmse, smape

def plot_results(data, forecast, confidence_intervals, service_name):
    plt.figure(figsize=(10, 5))
    plt.plot(data.index, data.values, label='Observations')
    plt.plot(forecast.index, forecast.values, label='Prévisions', color='red')
    plt.fill_between(forecast.index,
                     confidence_intervals.iloc[:, 0],
                     confidence_intervals.iloc[:, 1],
                     color='pink', alpha=0.3)
    plt.legend()
    plt.title(f'Prévisions des opérations pour {service_name}')
    plt.xlabel('Date')
    plt.ylabel('Nombre d\'opérations')
    plt.show()

if __name__ == "__main__":
    data = load_data_from_sql()
    processed_data = process_data(data)
    
    # Afficher les services disponibles
    available_services = processed_data['id_service'].unique()
    print("Services disponibles :")
    for id_service in available_services:
        service_name = processed_data[processed_data['id_service'] == id_service]['service'].iloc[0]
        print(f"ID: {id_service}, Nom: {service_name}")
    
    # Demander à l'utilisateur de choisir un service
    while True:
        try:
            selected_id = int(input("\nVeuillez entrer l'ID du service pour les prévisions : "))
            if selected_id in available_services:
                break
            else:
                print("ID de service non valide. Veuillez réessayer.")
        except ValueError:
            print("Veuillez entrer un nombre entier valide.")
    
    service_data = processed_data[processed_data['id_service'] == selected_id]
    service_name = service_data['service'].iloc[0]
    
    if not service_data.empty:
        service_data = service_data.set_index('date')['nombre_operations']
        
        print(f"\nAnalyse pour {service_name} (ID: {selected_id})")
        fitted_model = fit_arima_model(service_data)
        forecast, confidence_intervals = predict_future(fitted_model, steps=50)
        
        # Utiliser les dernières valeurs observées pour le calcul des erreurs
        last_observed = service_data.iloc[-50:] if len(service_data) > 50 else service_data
        mae, rmse, smape = calculate_errors(last_observed, forecast[:len(last_observed)])
        
        print(f"Erreur absolue moyenne (MAE): {mae}")
        print(f"Erreur quadratique moyenne (RMSE): {rmse}")
        print(f"Erreur (sMAPE): {smape}")
        
        plot_results(service_data, forecast, confidence_intervals, service_name)
    else:
        print(f"Aucune donnée trouvée pour le service {service_name} (ID: {selected_id})")

conn.close()