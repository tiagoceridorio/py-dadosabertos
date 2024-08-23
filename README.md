# Projeto de Download e Processamento de Dados de Empresas

Este projeto em Python realiza o download e o processamento dos dados de empresas disponibilizados pela Receita Federal do Brasil, através do portal de [Dados Abertos](https://dadosabertos.rfb.gov.br/CNPJ/). 

## Funcionalidades

- **Download Automático**: Realiza o download dos arquivos ZIP contendo os dados das empresas diretamente do site oficial da Receita Federal.
- **Descompactação e Organização**: Descompacta os arquivos ZIP, verificando e corrigindo as extensões dos arquivos para garantir que estejam em formato CSV.
- **Leitura e Mapeamento de Dados**: Lê os arquivos CSV e mapeia a estrutura dos dados, organizando-os em categorias específicas.
- **Armazenamento no MongoDB**: Insere os dados processados em um banco de dados MongoDB, garantindo que registros duplicados não sejam inseridos.

## Estrutura do Projeto

- **Scripts Específicos por Categoria**: Cada categoria de dados, como 'Naturezas', 'Cnaes', 'Empresas', 'Estabelecimentos', etc., possui um script dedicado para processar e armazenar seus respectivos dados.
- **Gestão Centralizada**: Um script principal coordena a execução de todos os scripts específicos, garantindo a correta sequência e organização do processamento.
- **Suporte a Grandes Volumes de Dados**: Utiliza barras de progresso para monitorar o processamento de categorias com muitos registros, como a categoria 'Sócios'.

## Como Utilizar

1. Clone este repositório:
    ```bash
    git clone https://github.com/tiagoceridorio/py-dadosaberto.git
    ```
2. Instale as dependências necessárias:
    ```bash
    pip install -r requirements.txt
    ```
3. Execute o script para iniciar o processo de download dos dados:
    ```bash
    python3 fetch_data.py
    ```
4. Execute o script para iniciar o processo de descompactação dos dados:
    ```bash
    python3 extract_files.py
    ```
5. Execute o script principal para iniciar o processo de armazenamento dos dados:
    ```bash
    python3 main.py
    ```

## Requisitos

- Python 3.x
- MongoDB

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

---

Este projeto foi desenvolvido para facilitar o acesso e o processamento de dados de empresas fornecidos pela Receita Federal, com o objetivo de automatizar o trabalho e melhorar a eficiência no tratamento de grandes volumes de dados.
