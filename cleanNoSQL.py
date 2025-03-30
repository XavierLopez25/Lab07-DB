from pymongo import MongoClient
import pandas as pd

# Configurar la conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://lop22716:1234@cluster0.mqducqk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri)
db = client["lab07"]
collection = db["paises_big_mac"]  # Nombre de la colección

# Extraer documentos
docs = list(collection.find())

# Convertir a DataFrame y aplanar campos anidados
df_costos = pd.json_normalize(docs)

# Mostrar información del DataFrame
print("Información del DataFrame de costos turísticos:")
print(df_costos.info())

# Verificar valores nulos
print("Valores nulos por columna:")
print(df_costos.isnull().sum())

# Verificar duplicados
print("Número de registros duplicados:")
print(df_costos.duplicated().sum())

# Opcional: Guardar reporte de calidad
df_report_nosql = pd.DataFrame({
    "nulos": df_costos.isnull().sum(),
    "duplicados": [df_costos.duplicated().sum()] * len(df_costos.columns)
}, index=df_costos.columns)
df_report_nosql.to_csv("reporte_calidad_costos_turisticos.csv", index=True)
