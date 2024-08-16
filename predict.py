import pandas as pd
from prophet import Prophet
import pyodbc
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error

try:
    # Connexion à SQL Server
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage2;UID=sa;PWD=May2015++'
    conn = pyodbc.connect(conn_str)

    sql_query = "SELECT date, nombre_operations FROM NOPJ3"
    data = pd.read_sql(sql_query, conn)

    data['date'] = pd.to_datetime(data['date'])
    data = data.rename(columns={'date':'ds', 'nombre_operations':'y'})

    model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)
    model.fit(data)

    future = model.make_future_dataframe(periods=35, freq='D')
    forecast = model.predict(future)

    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

    fig1 = model.plot(forecast)
    plt.title('Prédictions')
    plt.show()

    fig2 = model.plot_components(forecast)
    plt.show()

    forecast.to_csv('predictions.csv', index=False)

    y_true = data['y'].values
    y_pred = forecast['yhat'].values[:len(y_true)]

    
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    print(f"RMSE: {rmse}")

    mae = mean_absolute_error(y_true, y_pred)
    print(f"MAE: {mae}")

    def mape(y_true, y_pred):
        return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

    mape_value = mape(y_true, y_pred)
    print(f"MAPE: {mape_value}%")

    confidence_interval_width = np.mean(forecast['yhat_upper'] - forecast['yhat_lower'])
    print(f"Largeur moyenne de l'intervalle de confiance: {confidence_interval_width}")

except Exception as e:
    print(f"Une erreur s'est produite : {str(e)}")

finally:
    # Fermer la connexion
    if 'conn' in locals():
        conn.close()