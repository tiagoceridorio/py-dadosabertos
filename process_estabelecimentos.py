import os
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from mongo_connection import get_collection

def process_single_csv(csv_file_path, db):
    collection = get_collection(db, "estabelecimentos")
    print(f"Lendo arquivo {csv_file_path}...")

    # Contar o número de linhas no arquivo para a barra de progresso
    total_lines = sum(1 for line in open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore'))
    
    # Abrir o CSV sem cabeçalho e mapear campos por posição
    with open(csv_file_path, mode='r', encoding='ISO-8859-1', errors='ignore') as file:
        reader = csv.reader(file, delimiter=';')
        progress_bar = tqdm(total=total_lines, desc=f"Processando {os.path.basename(csv_file_path)}", position=csv_files.index(csv_file_path))

        for row in reader:
            # Mapear os campos conforme as posições especificadas no CSV
            cnpj_basico = row[0].strip()  # CNPJ Básico (posição 0)
            cnpj_ordem = row[1].strip()  # CNPJ Ordem (posição 1)
            cnpj_dv = row[2].strip()  # CNPJ DV (posição 2)
            identificador_matriz_filial = row[3].strip()  # Identificador Matriz/Filial (posição 3)
            nome_fantasia = row[4].strip() if row[4].strip() else None  # Nome Fantasia (posição 4), pode ser vazio
            situacao_cadastral = row[5].strip()  # Situação Cadastral (posição 5)
            data_situacao_cadastral = row[6].strip()  # Data da Situação Cadastral (posição 6)
            motivo_situacao_cadastral = row[7].strip() if row[7].strip() else None  # Motivo da Situação Cadastral (posição 7), pode ser vazio
            nome_cidade_exterior = row[8].strip() if row[8].strip() else None  # Nome da Cidade no Exterior (posição 8), pode ser vazio
            pais = row[9].strip() if row[9].strip() else None  # País (posição 9), pode ser vazio
            data_inicio_atividade = row[10].strip()  # Data de Início da Atividade (posição 10)
            cnae_fiscal_principal = row[11].strip()  # CNAE Fiscal Principal (posição 11)
            cnae_fiscal_secundario = row[12].strip() if row[12].strip() else None  # CNAE Fiscal Secundário (posição 12), pode ser vazio
            tipo_logradouro = row[13].strip() if row[13].strip() else None  # Tipo de Logradouro (posição 13), pode ser vazio
            logradouro = row[14].strip() if row[14].strip() else None  # Logradouro (posição 14), pode ser vazio
            numero = row[15].strip() if row[15].strip() else None  # Número (posição 15), pode ser vazio
            complemento = row[16].strip() if row[16].strip() else None  # Complemento (posição 16), pode ser vazio
            bairro = row[17].strip() if row[17].strip() else None  # Bairro (posição 17), pode ser vazio
            cep = row[18].strip() if row[18].strip() else None  # CEP (posição 18), pode ser vazio
            uf = row[19].strip()  # UF (posição 19)
            municipio = row[20].strip()  # Município (posição 20)
            ddd_1 = row[21].strip() if row[21].strip() else None  # DDD 1 (posição 21), pode ser vazio
            telefone_1 = row[22].strip() if row[22].strip() else None  # Telefone 1 (posição 22), pode ser vazio
            ddd_2 = row[23].strip() if row[23].strip() else None  # DDD 2 (posição 23), pode ser vazio
            telefone_2 = row[24].strip() if row[24].strip() else None  # Telefone 2 (posição 24), pode ser vazio
            ddd_fax = row[25].strip() if row[25].strip() else None  # DDD Fax (posição 25), pode ser vazio
            fax = row[26].strip() if row[26].strip() else None  # Fax (posição 26), pode ser vazio
            email = row[27].strip() if row[27].strip() else None  # Email (posição 27), pode ser vazio
            situacao_especial = row[28].strip() if row[28].strip() else None  # Situação Especial (posição 28), pode ser vazio
            data_situacao_especial = row[29].strip() if row[29].strip() else None  # Data da Situação Especial (posição 29), pode ser vazio

            # Verificar se os campos principais não estão vazios ou nulos
            if cnpj_basico and cnpj_ordem and cnpj_dv:
                # Verificar se o estabelecimento já existe no MongoDB
                if collection.find_one({"cnpj_basico": cnpj_basico, "cnpj_ordem": cnpj_ordem, "cnpj_dv": cnpj_dv}) is None:
                    document = {
                        "cnpj_basico": cnpj_basico,
                        "cnpj_ordem": cnpj_ordem,
                        "cnpj_dv": cnpj_dv,
                        "identificador_matriz_filial": identificador_matriz_filial,
                        "nome_fantasia": nome_fantasia,
                        "situacao_cadastral": situacao_cadastral,
                        "data_situacao_cadastral": data_situacao_cadastral,
                        "motivo_situacao_cadastral": motivo_situacao_cadastral,
                        "nome_cidade_exterior": nome_cidade_exterior,
                        "pais": pais,
                        "data_inicio_atividade": data_inicio_atividade,
                        "cnae_fiscal_principal": cnae_fiscal_principal,
                        "cnae_fiscal_secundario": cnae_fiscal_secundario,
                        "tipo_logradouro": tipo_logradouro,
                        "logradouro": logradouro,
                        "numero": numero,
                        "complemento": complemento,
                        "bairro": bairro,
                        "cep": cep,
                        "uf": uf,
                        "municipio": municipio,
                        "ddd_1": ddd_1,
                        "telefone_1": telefone_1,
                        "ddd_2": ddd_2,
                        "telefone_2": telefone_2,
                        "ddd_fax": ddd_fax,
                        "fax": fax,
                        "email": email,
                        "situacao_especial": situacao_especial,
                        "data_situacao_especial": data_situacao_especial,
                    }
                    collection.insert_one(document)
            progress_bar.update(1)
        progress_bar.close()

def process_estabelecimentos(category_folder_path, db):
    # Obter todos os arquivos CSV na pasta da categoria, em ordem alfabética
    global csv_files
    csv_files = sorted([os.path.join(category_folder_path, f) for f in os.listdir(category_folder_path) if f.endswith('.csv')])

    # Processar arquivos CSV em paralelo
    with ThreadPoolExecutor() as executor:
        executor.map(lambda csv_file: process_single_csv(csv_file, db), csv_files)

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# process_estabelecimentos("/caminho/para/a/pasta/categoria", db)
