import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

# --- Extracción ---

# Conexión a MongoDB Atlas (NoSQL)
mongo_uri = "mongodb+srv://lop22716:xd@xd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri)
db = client["lab07"]
collection = db["paises_big_mac"]  # Nombre de la colección

# Extraer y aplanar documentos de MongoDB
docs = list(collection.find())
df_nosql = pd.json_normalize(docs)

# Renombrar la columna "país" a "pais" (si existe)
df_nosql.rename(columns={'país': 'pais'}, inplace=True)

# Convertir la columna _id a string para evitar problemas al insertar en SQL
if "_id" in df_nosql.columns:
    df_nosql["_id"] = df_nosql["_id"].astype(str)

# Conexión a PostgreSQL para datos raw (lab07db2)
engine_raw = create_engine("postgresql://postgres:1234@localhost:5432/lab07db2")

# Extraer datos SQL (ejemplo: tabla 'pais_poblacion')
df_sql = pd.read_sql("SELECT * FROM pais_poblacion", engine_raw)

# --- Transformación ---

# Integrar datos SQL y NoSQL usando la columna 'pais'
df_integrado = pd.merge(df_nosql, df_sql, on="pais", how="left")

# Convertir a string cualquier columna de ID en el DataFrame integrado
for col in df_integrado.columns:
    if col.startswith('_id'):
        df_integrado[col] = df_integrado[col].astype(str)

# Dropear columnas de ID innecesarias (por ejemplo, _id, _id_x, _id_y)
df_integrado.drop(columns=['_id'], inplace=True, errors='ignore')
df_integrado.drop(columns=['_id_x', '_id_y'], inplace=True, errors='ignore')

# Combinar las columnas de continente en una sola
# Se asume que tras el merge quedan 'continente_x' y 'continente_y'
if 'continente_x' in df_integrado.columns and 'continente_y' in df_integrado.columns:
    # Tomamos la que no sea nula, o ambas si son iguales
    df_integrado['continente'] = df_integrado['continente_x'].combine_first(df_integrado['continente_y'])
    df_integrado.drop(columns=['continente_x', 'continente_y'], inplace=True)

# (Opcional) Si deseas agregar una columna secuencial "id" en el DataFrame:
# df_integrado.insert(0, 'id', range(1, len(df_integrado) + 1))

# --- Carga ---

# Conexión a PostgreSQL para el data warehouse (lab07dw)
engine_dw = create_engine("postgresql://postgres:1234@localhost:5432/lab07dw")

# Cargar datos raw en la base de datos raw (lab07db2)
df_nosql.to_sql("paises_mundo_big_mac_raw", engine_raw, if_exists="replace", index=False)
df_sql.to_sql("pais_poblacion_raw", engine_raw, if_exists="replace", index=False)

# Cargar datos integrados en el data warehouse (lab07dw)
df_integrado.to_sql("datawarehouse", engine_dw, if_exists="replace", index=False)

print("Proceso ETL completado:")
print("- Datos raw cargados en la base de datos 'lab07db2'")
print("- Datos integrados cargados en la base de datos 'lab07dw'")
