import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
import numpy as np
from sklearn.metrics import mean_absolute_percentage_error

# Connexion à SQL Server
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=.;DATABASE=stage2;UID=sa;PWD=May2015++'
conn = pyodbc.connect(conn_str)

def load_data_from_sql():
    query = """
    SELECT TOP 254 date, nombre_operations
    FROM NOPJ3
    ORDER BY date DESC
    """
    data = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
    return data.sort_index()  # Trier par ordre croissant de date

def load_predictions_from_sql():
    query = """
    SELECT date, predicted_operations
    FROM predictions
    WHERE date >= (SELECT DATEADD(day, -254, MAX(date)) FROM NOPJ3)
    ORDER BY date
    """
    predictions = pd.read_sql(query, conn, index_col='date', parse_dates=['date'])
    return predictions

def calculate_mape(y_true, y_pred):
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def align_data_and_predictions(data, predictions, shift_range=range(-7, 8)):
    best_shift = 0
    best_mape = float('inf')
    
    for shift in shift_range:
        shifted_pred = predictions.shift(shift)
        common_dates = data.index.intersection(shifted_pred.index)
        if len(common_dates) > 0:
            mape = calculate_mape(data.loc[common_dates, 'nombre_operations'], 
                                  shifted_pred.loc[common_dates, 'predicted_operations'])
            if mape < best_mape:
                best_mape = mape
                best_shift = shift
    
    return predictions.shift(best_shift), best_shift, best_mape

def plot_results(data, predictions, shift, mape):
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data['nombre_operations'], label='Données réelles', color='blue')
    plt.plot(predictions.index, predictions['predicted_operations'], label='Prédictions', color='red')
    
    plt.title(f'Nombre d\'opérations réelles vs prédictions (MAPE: {mape:.2f}%, Décalage: {shift} jours)')
    plt.xlabel('Date')
    plt.ylabel('Nombre d\'opérations')
    plt.legend()
    plt.grid(True)
    
    plt.text(0.05, 0.95, f'MAPE: {mape:.2f}%\nDécalage: {shift} jours', 
             transform=plt.gca().transAxes, 
             verticalalignment='top', 
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Charger les données réelles
    data = load_data_from_sql()
    
    # Charger les prédictions
    predictions = load_predictions_from_sql()
    
    # Aligner les données et calculer le meilleur MAPE
    aligned_predictions, best_shift, best_mape = align_data_and_predictions(data, predictions)
    
    # Tracer les résultats
    plot_results(data, aligned_predictions, best_shift, best_mape)
    
    conn.close()