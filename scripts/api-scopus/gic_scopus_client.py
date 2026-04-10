import os
import requests
from dotenv import load_dotenv  # pyright: ignore[reportMissingImports]

load_dotenv()

API_KEY = os.getenv("ELSEVIER_API_KEY")

BASE_URL = "https://api.elsevier.com/content/search/scopus"
AUTHOR_URL = "https://api.elsevier.com/content/search/author" 

def search_scopus(query: str, count: int = 5):
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/json"
    }

    all_entries = []
    cursor = "*"

    while True:
        params = {
            "query": query,
            "count": count,
            "cursor": cursor,
            "view": "COMPLETE"
        }

        response = requests.get(BASE_URL, headers=headers, params=params)

        print("--- SCOPUS API Headers ---")
        print(response.headers)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code} - {response.text}")

        data = response.json()
        search_results = data.get("search-results", {})
        entries = search_results.get("entry", [])
        
        all_entries.extend(entries)
        
        next_cursor = search_results.get("cursor", {}).get("@next")
        
        if not next_cursor or next_cursor == cursor:
            break
            
        cursor = next_cursor

    return {"search-results": {"entry": all_entries}}
    
def search_author_publications(author_id: str, count: int = 5):
    """
    Busca todas as publicações de um autor específico no Scopus.
    
    :param author_id: Identificador único do autor no Scopus.
    :param count: Número de resultados por página (padrão: 5).
    :return: Lista de publicações do autor.
    """
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/json"
    }

    all_entries = []
    cursor = "*"

    while True:
        params = {
            "query": f"AU-ID({author_id})",
            "count": count,
            "cursor": cursor,
            "view": "COMPLETE"
        }

        response = requests.get(BASE_URL, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Erro na API: {response.status_code} - {response.text}")

        data = response.json()
        search_results = data.get("search-results", {})
        entries = search_results.get("entry", [])
        
        all_entries.extend(entries)
        
        next_cursor = search_results.get("cursor", {}).get("@next")
        
        if not next_cursor or next_cursor == cursor:
            break
            
        cursor = next_cursor

    return {"search-results": {"entry": all_entries}}