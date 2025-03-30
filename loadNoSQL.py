import json
from pymongo import MongoClient

# Cargar el archivo JSON
with open('./Datos_para_MongoDB/Datos_para_MongoDB/paises_mundo_big_mac.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Configurar la conexión a MongoDB Atlas
# Reemplaza 'user', 'password', 'cluster.mongodb.net' y 'tu_basedatos' con tus credenciales y nombre de base de datos.
mongo_uri = "mongodb+srv://lop22716:1234@cluster0.mqducqk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri)

# Seleccionar la base de datos y la colección
db = client["lab07"]
collection = db["paises_big_mac"]

# Insertar los documentos en la colección
resultado = collection.insert_many(data)
print(f"Documentos insertados: {len(resultado.inserted_ids)}")
