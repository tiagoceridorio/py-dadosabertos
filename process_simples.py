import os
import csv
from tqdm import tqdm
from mongo_connection import get_collection

def process_simples(category_folder_path, db):
    collection = get_collection(db, "simples")
    
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    csv_files = sorted([f for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    for csv_file in csv_files:
        csv_file_path = os.path.join(category_folder_path, csv_file)
        print(f"Lendo arquivo {csv_file_path}...")

        # Contar o número de linhas no arquivo para a barra de progresso
        total_lines = sum(1 for line in open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore'))
        
        # Abrir o CSV sem cabeçalho e mapear campos por posição
        with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
            reader = csv.reader(file, delimiter=';')
            progress_bar = tqdm(total=total_lines, desc=f"Processando {os.path.basename(csv_file_path)}")

            for row in reader:
                # Mapear os campos conforme as posições especificadas no CSV para "Simples"
                cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
                opcao_pelo_simples = row[1].strip()  # Opção pelo Simples (posição 1)
                data_opcao_simples = row[2].strip()  # Data da Opção pelo Simples (posição 2)
                data_exclusao_simples = row[3].strip() if row[3].strip() else None  # Data da Exclusão do Simples (posição 3)
                opcao_mei = row[4].strip()  # Opção pelo MEI (posição 4)
                data_opcao_mei = row[5].strip()  # Data da Opção pelo MEI (posição 5)
                data_exclusao_mei = row[6].strip() if row[6].strip() else None  # Data da Exclusão do MEI (posição 6)

                # Verificar se os campos principais não estão vazios ou nulos
                if cnpj_basico:
                    # Verificar se o CNPJ Básico já existe no MongoDB
                    if collection.find_one({"cnpj_basico": cnpj_basico}) is None:
                        document = {
                            "cnpj_basico": cnpj_basico,
                            "opcao_pelo_simples": opcao_pelo_simples,
                            "data_opcao_simples": data_opcao_simples,
                            "data_exclusao_simples": data_exclusao_simples,
                            "opcao_mei": opcao_mei,
                            "data_opcao_mei": data_opcao_mei,
                            "data_exclusao_mei": data_exclusao_mei,
                        }
                        collection.insert_one(document)
                progress_bar.update(1)
            progress_bar.close()

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_simples("/caminho/para/a/pasta/categoria", db)
