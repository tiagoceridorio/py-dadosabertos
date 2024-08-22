import os
from mongo_connection import get_mongo_client, get_database
from process_naturezas import process_naturezas
from process_cnaes import process_cnaes
from process_empresas import process_empresas
from process_estabelecimentos import process_estabelecimentos
from process_motivos import process_motivos
from process_municipios import process_municipios
from process_paises import process_paises  # Importando o novo script

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
        
        # Processar "Naturezas"
        naturezas_folder_path = os.path.join(plain_files_path, "Naturezas")
        if os.path.exists(naturezas_folder_path):
            print(f"Processando arquivos na pasta {naturezas_folder_path}...")
            process_naturezas(naturezas_folder_path, db)
        else:
            print(f"Pasta {naturezas_folder_path} não encontrada!")

        # Processar "Cnaes"
        cnaes_folder_path = os.path.join(plain_files_path, "Cnaes")
        if os.path.exists(cnaes_folder_path):
            print(f"Processando arquivos na pasta {cnaes_folder_path}...")
            process_cnaes(cnaes_folder_path, db)
        else:
            print(f"Pasta {cnaes_folder_path} não encontrada!")

        # Processar "Empresas"
        empresas_folder_path = os.path.join(plain_files_path, "Empresas")
        if os.path.exists(empresas_folder_path):
            print(f"Processando arquivos na pasta {empresas_folder_path}...")
            process_empresas(empresas_folder_path, db)
        else:
            print(f"Pasta {empresas_folder_path} não encontrada!")

        # Processar "Estabelecimentos"
        estabelecimentos_folder_path = os.path.join(plain_files_path, "Estabelecimentos")
        if os.path.exists(estabelecimentos_folder_path):
            print(f"Processando arquivos na pasta {estabelecimentos_folder_path}...")
            process_estabelecimentos(estabelecimentos_folder_path, db)
        else:
            print(f"Pasta {estabelecimentos_folder_path} não encontrada!")

        # Processar "Motivos"
        motivos_folder_path = os.path.join(plain_files_path, "Motivos")
        if os.path.exists(motivos_folder_path):
            print(f"Processando arquivos na pasta {motivos_folder_path}...")
            process_motivos(motivos_folder_path, db)
        else:
            print(f"Pasta {motivos_folder_path} não encontrada!")

        # Processar "Municipios"
        municipios_folder_path = os.path.join(plain_files_path, "Municipios")
        if os.path.exists(municipios_folder_path):
            print(f"Processando arquivos na pasta {municipios_folder_path}...")
            process_municipios(municipios_folder_path, db)
        else:
            print(f"Pasta {municipios_folder_path} não encontrada!")

        # Processar "Paises"
        paises_folder_path = os.path.join(plain_files_path, "Paises")
        if os.path.exists(paises_folder_path):
            print(f"Processando arquivos na pasta {paises_folder_path}...")
            process_paises(paises_folder_path, db)
        else:
            print(f"Pasta {paises_folder_path} não encontrada!")
    else:
        print("Nenhuma pasta de data encontrada!")

if __name__ == "__main__":
    base_path = os.getcwd()  # Ou o caminho onde os CSVs estão armazenados
    client = get_mongo_client()
    db = get_database(client)

    process_all_categories(base_path, db)
