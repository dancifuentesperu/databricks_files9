# Databricks notebook source
# MAGIC %run ./0.Variables
# MAGIC

# COMMAND ----------

# Copia los archivos del repositorio GitHub a local_tmp_path
import requests
import os

# ✅ Tus variables

# 🔗 Repositorio GitHub (cambia si quieres usar otro)
github_user = "dancifuentesperu"

# repo_name = "databricks_files5"
branch = "main"

# 🔧 Construir paths
volume_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}"
dbfs_path = f"dbfs:{volume_path}"

# 🌐 GitHub API para listar archivos del repo
api_url = f"https://api.github.com/repos/{github_user}/{repo_name}/git/trees/{branch}?recursive=1"

# 📥 Obtener lista de archivos
response = requests.get(api_url)
response.raise_for_status()
tree = response.json().get("tree", [])

# 🔁 Descargar y copiar cada archivo al Volume UC
for item in tree:
    if item["type"] == "blob":  # Asegura que es un archivo
        file_path = item["path"]
        print(f"📄 Descargando: {file_path}")
        
        # Construir URL RAW
        raw_url = f"https://raw.githubusercontent.com/{github_user}/{repo_name}/{branch}/{file_path}"
        
        # Descargar contenido del archivo
        r = requests.get(raw_url)
        if r.status_code != 200:
            print(f"❌ Error al descargar: {raw_url}")
            continue

        # Guardar temporalmente en /Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}
        tmp_filename = os.path.basename(file_path)
        local_tmp_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_tmp}/{tmp_filename}"
        with open(local_tmp_path, "wb") as f:
            f.write(r.content)

        # Subir al Volume UC (manteniendo la ruta del archivo)


# COMMAND ----------

# Variables
#catalog_name        = "integration_catalog"
#schema_name_raw     = "raw_files"
#volume_name_raw     = "tmp"       # Volumen origen
#volume_name_target  = "files"   # Volumen destino (ajusta si es otro)

# Rutas de origen y destino
source_volume_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_raw}/"
target_volume_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_target}/"

# Listar archivos y copiar solo los .parquet
files = dbutils.fs.ls(source_volume_path)

copied_files = []

for file in files:
    if file.name.endswith(".parquet"):
        src = f"{source_volume_path}{file.name}"
        dst = f"{target_volume_path}{file.name}"
        print(f"Copiando {file.name}...")
        dbutils.fs.cp(src, dst)
        copied_files.append(file.name)

# Mostrar resumen al final
print("\n✅ Archivos copiados exitosamente:")
for f in copied_files:
    print(f"- {f}")

print(f"\n📂 Archivos almacenados en: {target_volume_path}")