import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pyodbc
from datetime import date, timedelta

conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage2;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def load_data_from_sql():
    query = "SELECT date, nombre_operations FROM NOPJ3 ORDER BY date"
    data = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
    return data

def fit_arima_model(data, order=(11, 1, 10)):
    model = ARIMA(data, order=order)
    fitted_model = model.fit()
    return fitted_model

def predict_future(fitted_model, steps=125):
    forecast = fitted_model.get_forecast(steps=steps)
    forecast_mean = forecast.predicted_mean
    confidence_intervals = forecast.conf_int()
    return forecast_mean, confidence_intervals

def calculate_errors(true_values, predicted_values):
    mae = mean_absolute_error(true_values, predicted_values)
    rmse = np.sqrt(mean_squared_error(true_values, predicted_values))
    mape = np.mean(np.abs((true_values - predicted_values) / true_values)) * 100
    return mae, rmse ,mape

def plot_results(data, forecast, confidence_intervals):
    plt.figure(figsize=(10, 5))
    plt.plot(data, label='Observations')
    plt.plot(forecast, label='Prévisions', color='red')
    plt.fill_between(forecast.index,
                     confidence_intervals.iloc[:, 0],
                     confidence_intervals.iloc[:, 1],
                     color='pink', alpha=0.3)
    plt.legend()
    plt.show()

def save_predictions_to_sql(conn, forecast, confidence_intervals):
    cursor = conn.cursor()
    
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='predictions' AND xtype='U')
    CREATE TABLE predictionsm (
        date DATE PRIMARY KEY,
        forecast FLOAT,
        lower_ci FLOAT,
        upper_ci FLOAT
    )
    """)
    
    for date, pred, lower, upper in zip(forecast.index, forecast, 
                                        confidence_intervals.iloc[:, 0], 
                                        confidence_intervals.iloc[:, 1]):
        cursor.execute("""
        INSERT INTO predictionsm (date, forecast, lower_ci, upper_ci)
        VALUES (?, ?, ?, ?)
        """, (date.date(), float(pred), float(lower), float(upper)))
    
    conn.commit()
    cursor.close()

def create_comparison_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='operations_below_prediction' AND xtype='U')
    CREATE TABLE operations_below_prediction (
        date DATE PRIMARY KEY,
        actual_operations INT,
        predicted_operations FLOAT
    )
    """)
    conn.commit()
    cursor.close()

def compare_and_insert(conn, current_date):
    cursor = conn.cursor()
    
    cursor.execute("SELECT forecast FROM predictionsm WHERE date = ?", (current_date,))
    prediction = cursor.fetchone()
    
    if prediction:
        predicted_value = prediction[0]
        
        cursor.execute("SELECT nombre_operations FROM NombreOperationsParJour2 WHERE date = ?", (current_date,))
        actual = cursor.fetchone()
        
        if actual:
            actual_value = actual[0]
            
            if actual_value < predicted_value:
                cursor.execute("""
                INSERT INTO operations_below_prediction (date, actual_operations, predicted_operations)
                VALUES (?, ?, ?)
                """, (current_date, actual_value, predicted_value))
                conn.commit()
    
    cursor.close()

if __name__ == "__main__":
    data = load_data_from_sql()
    data_series = data['nombre_operations']
    
    fitted_model = fit_arima_model(data_series)
    forecast, confidence_intervals = predict_future(fitted_model, steps=125)
    
    observed_future_values = [data_series[-1]] * len(forecast)
    mae, rmse ,mape= calculate_errors(observed_future_values, forecast)
    print(f"Erreur absolue moyenne (MAE): {mae}")
    print(f"Erreur quadratique moyenne (RMSE): {rmse}")
    print(f"Erreur (RMSE): {mape}")
    
    plot_results(data_series, forecast, confidence_intervals)
    
    
    
    
    current_date = date(2025, 1, 2)
    compare_and_insert(conn, current_date)
    
    conn.close()