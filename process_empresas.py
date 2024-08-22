import os
import csv
from concurrent.futures import ThreadPoolExecutor
from mongo_connection import get_collection

def process_single_csv(csv_file_path, db):
    collection = get_collection(db, "empresas")
    print(f"Lendo arquivo {csv_file_path}...")

    # Abrir o CSV sem cabeçalho e mapear campos por posição
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        reader = csv.reader(file, delimiter=';')

        for row in reader:
            # Mapear os campos conforme as posições especificadas no CSV
            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
            razao_social = row[1].strip()  # Razão Social (posição 1)
            natureza_juridica = row[2].strip()  # Natureza Jurídica (posição 2)
            qualificacao_responsavel = row[3].strip()  # Qualificação do Responsável (posição 3)
            capital_social = row[4].replace(",", ".").strip()  # Capital Social (posição 4), substitui vírgula por ponto
            porte = row[5].strip()  # Porte da Empresa (posição 5)
            ente_federativo_responsavel = row[6].strip() if row[6].strip() else None  # Ente Federativo (posição 6), pode ser vazio

            # Verificar se os campos principais não estão vazios ou nulos
            if cnpj_basico and razao_social:
                # Verificar se o CNPJ Básico já existe no MongoDB
                if collection.find_one({"cnpj_basico": cnpj_basico}) is None:
                    document = {
                        "cnpj_basico": cnpj_basico,
                        "razao_social": razao_social,
                        "natureza_juridica": natureza_juridica,
                        "qualificacao_responsavel": qualificacao_responsavel,
                        "capital_social": float(capital_social),  # Converter capital social para número
                        "porte": porte,
                        "ente_federativo_responsavel": ente_federativo_responsavel
                    }
                    collection.insert_one(document)
                    print(f"Documento inserido: {document}")
                else:
                    print(f"CNPJ Básico {cnpj_basico} já existe. Registro ignorado.")
            else:
                print(f"Registro ignorado por conter campos vazios: {row}")

def process_empresas(category_folder_path, db):
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo
    with ThreadPoolExecutor() as executor:
        executor.map(lambda csv_file: process_single_csv(csv_file, db), csv_files)

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_empresas("/caminho/para/a/pasta/categoria", db)
