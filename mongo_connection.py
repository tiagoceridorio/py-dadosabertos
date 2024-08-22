from pymongo import MongoClient

def get_mongo_client(uri="mongodb://192.168.0.5:27017/"):
    client = MongoClient(uri)
    return client

def get_database(client, db_name="dadosabertos"):
    return client[db_name]

def get_collection(db, collection_name):
    return db[collection_name]

# Exemplo de uso:
# client = get_mongo_client()
# db = get_database(client)
# collection = get_collection(db, "nome_da_colecao")
