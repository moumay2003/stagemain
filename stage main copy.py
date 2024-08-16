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