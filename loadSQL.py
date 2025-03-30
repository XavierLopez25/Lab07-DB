import pandas as pd
from sqlalchemy import create_engine

# Ruta al archivo CSV (asegúrate de que la ruta sea correcta)
csv_file = './Datos_para_SQL/Datos_para_SQL/pais_poblacion.csv'

# Leer el CSV en un DataFrame
df = pd.read_csv(csv_file)

# Mostrar las primeras filas para verificar la lectura
print(df.head())

# Configurar la conexión a PostgreSQL (reemplaza user, password, host, puerto y nombre de la base de datos)
engine = create_engine("postgresql://postgres:1234@localhost:5432/lab07db2")

# Cargar los datos en una tabla de PostgreSQL.
# if_exists='replace' crea la tabla o la reemplaza si ya existe; también puedes usar 'append' para agregar.
df.to_sql('pais_poblacion', engine, if_exists='replace', index=False)

print("Datos cargados exitosamente en PostgreSQL.")
