import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import shutil

# URL base do site
base_url = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"

# Função para criar uma pasta se ela não existir
def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

# Função para apagar uma pasta existente
def delete_folder(folder_name):
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)

# Função para baixar um arquivo
def download_file(url, folder):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(os.path.join(folder, local_filename), 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

# Função principal
def fetch_latest_data():
    # Obter o conteúdo da página principal
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encontrar todos os links para pastas (datas)
    links = soup.find_all('a')
    folders = [link.get('href') for link in links if link.get('href').endswith('/')]

    # Identificar a pasta de data mais recente
    latest_folder = sorted(folders)[-1]
    
    # Nome da pasta onde o conteúdo será salvo
    save_path = os.path.join(os.getcwd(), latest_folder.rstrip('/'))
    
    # Apagar a pasta existente se ela já estiver presente
    delete_folder(save_path)
    
    # Criar a pasta novamente
    create_folder(save_path)

    # Baixar apenas os arquivos .zip da pasta mais recente
    folder_url = urljoin(base_url, latest_folder)
    response = requests.get(folder_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Filtrar e baixar apenas arquivos .zip
    files = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.zip')]

    for file in files:
        file_url = urljoin(folder_url, file)
        print(f"Baixando {file} para {save_path}...")
        download_file(file_url, save_path)

    print(f"Download completo. Arquivos .zip salvos em: {save_path}")

# Executar a função principal
if __name__ == "__main__":
    fetch_latest_data()
