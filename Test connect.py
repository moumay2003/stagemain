import pyodbc

# Paramètres de connexion
server = ''
database = ''
username = 'votre_nom_utilisateur'
password = 'votre_mot_de_passe'
driver = '{ODBC Driver 18 for SQL Server}'  # Assurez-vous que c'est le bon driver

# Chaîne de connexion
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try:
    # Tentative de connexion
    conn = pyodbc.connect(conn_str)
    print("Connexion réussie !")
    
    # Exécution d'une requête simple
    cursor = conn.cursor()
    cursor.execute("SELECT @@version;")
    row = cursor.fetchone()
    print("Version du serveur :", row[0])
    
    # Fermeture de la connexion
    conn.close()
except pyodbc.Error as e:
    print(f"Erreur de connexion : {e}")
