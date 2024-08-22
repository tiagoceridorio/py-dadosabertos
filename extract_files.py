import os
import zipfile

# Função para verificar e adicionar a extensão .csv, se necessário
def ensure_csv_extension(file_path):
    if not file_path.lower().endswith('.csv'):
        new_file_path = file_path + '.csv'
        os.rename(file_path, new_file_path)
        return new_file_path
    return file_path

# Função para extrair os arquivos .zip
def extract_zip_files(data_folder):
    # Caminho para a pasta onde os arquivos .zip foram salvos
    zip_folder = os.path.join(os.getcwd(), data_folder)
    
    # Caminho para a pasta plain_files
    plain_files_folder = os.path.join(zip_folder, 'plain_files')
    
    # Verificar se a pasta plain_files já existe
    if not os.path.exists(plain_files_folder):
        os.makedirs(plain_files_folder)
    else:
        print(f"A pasta {plain_files_folder} já existe. Pulando extração para {data_folder}.")
        return
    
    # Percorrer todos os arquivos na pasta de data
    for item in os.listdir(zip_folder):
        if item.endswith('.zip'):
            # Identificar a categoria a partir do nome do arquivo .zip
            category_name = item.rsplit('.', 1)[0]  # Remove a extensão .zip
            category_name = ''.join([i for i in category_name if not i.isdigit()])  # Remove números do final
            
            # Criar a pasta da categoria dentro de plain_files
            category_folder = os.path.join(plain_files_folder, category_name)
            if not os.path.exists(category_folder):
                os.makedirs(category_folder)
            
            zip_path = os.path.join(zip_folder, item)
            print(f"Extraindo {zip_path} para {category_folder}...")
            
            # Extrair o conteúdo do arquivo .zip para a pasta da categoria
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(category_folder)
            
            # Verificar se os arquivos extraídos têm a extensão .csv
            for root, dirs, files in os.walk(category_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    ensure_csv_extension(file_path)
            
            print(f"Extração completa para {zip_path}. Arquivos salvos em {category_folder}")

# Função principal para varrer as pastas e executar a extração
def process_all_folders():
    # Caminho base do projeto
    base_path = os.getcwd()
    
    # Varrer todas as pastas no diretório atual
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        
        # Verificar se é uma pasta e se não contém a pasta plain_files
        if os.path.isdir(folder_path) and not os.path.exists(os.path.join(folder_path, 'plain_files')):
            print(f"Processando a pasta: {folder}")
            extract_zip_files(folder)
        else:
            print(f"A pasta {folder} já foi processada ou não é uma pasta de dados.")

# Executar a função principal
if __name__ == "__main__":
    process_all_folders()
