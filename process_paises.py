import os
import csv
from mongo_connection import get_collection

def process_paises(category_folder_path, db):
    collection = get_collection(db, "paises")
    
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([f for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    for csv_file in csv_files:
        csv_file_path = os.path.join(category_folder_path, csv_file)
        print(f"Lendo arquivo {csv_file_path}...")

        # Abrir o CSV sem cabeçalho e mapear campos por posição
        with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
            reader = csv.reader(file, delimiter=';')
            
            for row in reader:
                # Mapear os campos conforme as posições especificadas no CSV para "Paises"
                codigo = row[0].strip()  # Código do País (posição 0)
                nome = row[1].strip()  # Nome do País (posição 1)

                # Verificar se os campos principais não estão vazios ou nulos
                if codigo and nome:
                    # Verificar se o código do país já existe no MongoDB
                    if collection.find_one({"codigo": codigo}) is None:
                        document = {
                            "codigo": codigo,
                            "nome": nome,
                        }
                        collection.insert_one(document)
                        print(f"Documento inserido: {document}")
                    else:
                        print(f"Código do País {codigo} já existe. Registro ignorado.")
                else:
                    print(f"Registro ignorado por conter campos vazios: {row}")

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_paises("/caminho/para/a/pasta/categoria", db)
