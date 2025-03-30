import pandas as pd
from sqlalchemy import create_engine

# Configurar la conexión a la base de datos SQL (raw data)
engine = create_engine("postgresql://postgres:1234@localhost:5432/lab07db2")

# Extraer los datos (ejemplo: tabla 'pais_poblacion')
df_poblacion = pd.read_sql("SELECT * FROM pais_poblacion", engine)

# Mostrar información general
print("Información del DataFrame de población:")
print(df_poblacion.info())

# Verificar valores nulos
print("Valores nulos por columna:")
print(df_poblacion.isnull().sum())

# Verificar registros duplicados
print("Número de registros duplicados:")
print(df_poblacion.duplicated().sum())

# Opcional: Guardar un reporte de calidad de datos
df_report = pd.DataFrame({
    "nulos": df_poblacion.isnull().sum(),
    "duplicados": [df_poblacion.duplicated().sum()] * len(df_poblacion.columns)
}, index=df_poblacion.columns)
df_report.to_csv("reporte_calidad_poblacion.csv", index=True)
