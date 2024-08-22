import os
from mongo_connection import get_mongo_client, get_database
from process_naturezas import process_naturezas

def find_latest_data_folder(base_path):
    # Encontra a pasta de data mais recente
    folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    date_folders = [f for f in folders if f[0].isdigit()]  # Filtra apenas pastas que comecem com números (datas)
    latest_folder = sorted(date_folders)[-1] if date_folders else None
    return latest_folder

def process_all_categories(base_path, db):
    latest_folder = find_latest_data_folder(base_path)
    
    if latest_folder:
        # Caminho para a pasta plain_files dentro da data mais recente
        plain_files_path = os.path.join(base_path, latest_folder, "plain_files")
        
        # Caminho para a pasta da categoria "Naturezas"
        naturezas_folder_path = os.path.join(plain_files_path, "Naturezas")
        
        if os.path.exists(naturezas_folder_path):
            print(f"Processando arquivos na pasta {naturezas_folder_path}...")
            process_naturezas(naturezas_folder_path, db)
        else:
            print(f"Pasta {naturezas_folder_path} não encontrada!")
    else:
        print("Nenhuma pasta de data encontrada!")

if __name__ == "__main__":
    base_path = os.getcwd()  # Ou o caminho onde os CSVs estão armazenados
    client = get_mongo_client()
    db = get_database(client)

    process_all_categories(base_path, db)
