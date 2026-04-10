import json
from gic_scopus_client import search_scopus, search_author_publications

def parse_articles(raw_json):
    entries = raw_json.get("search-results", {}).get("entry", [])
    articles = []

    for item in entries:
        affiliations = item.get("affiliation", [])
        if isinstance(affiliations, dict): affiliations = [affiliations]
        affil_map = {af.get("afid"): af.get("affilname") for af in affiliations if af.get("afid")}

        authors_data = item.get("author", [])
        if isinstance(authors_data, dict): authors_data = [authors_data]
        
        lista_autores = []
        for auth in authors_data:
            afid_info = auth.get("afid", [])
            if isinstance(afid_info, dict): afid_info = [afid_info]
            
            nomes_afiliacoes = [affil_map.get(af.get("$")) for af in afid_info if af.get("$")]
            
            lista_autores.append({
                "nome": auth.get("authname"),
                "afiliacoes": list(filter(None, nomes_afiliacoes))
            })

        article = {
            "titulo": item.get("dc:title"),
            "data_publicacao": item.get("prism:coverDate"),
            "autores": lista_autores,
            "doi": item.get("prism:doi")
        }
        articles.append(article)
    return articles

def fetch_author_publications():
    author_id = input("Digite o ID do autor no Scopus: ")
    print(f"Buscando publicações para o autor com ID: {author_id}")
    
    try:
        raw_data = search_author_publications(author_id=author_id, count=25)
        structured_data = parse_articles(raw_data)
        
        nome_arquivo = f"publicacoes_autor_{author_id}.json"
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(structured_data, f, indent=4, ensure_ascii=False)
        
        print(f"Consulta finalizada. {len(structured_data)} publicações salvas em {nome_arquivo}.")
    except Exception as e:
        print(f"Erro ao buscar publicações do autor: {e}")

def main():
    print("Escolha uma opção:")
    print("1. Buscar artigos por instituição e área.")
    print("2. Buscar publicações de um autor específico.")
    opcao = input("Digite o número da opção desejada: ")

    if opcao == "1":
        instituicao = "Universidade Federal Rural de Pernambuco"
        area_codigo = "COMP"  # COMP = Computer Science
        ano = 2025
        
        query = f'AFFIL("{instituicao}") AND SUBJAREA({area_codigo}) AND PUBYEAR IS {ano}'
        
        print(f"Iniciando busca no Scopus: {query}")
        print("")
        raw_data = search_scopus(query=query, count=25)
        structured_data = parse_articles(raw_data)
        
        nome_arquivo = f"artigos_ufrpe_computacao_{ano}.json"
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(structured_data, f, indent=4, ensure_ascii=False)
        print("")
        print(f"Consulta finalizada. {len(structured_data)} artigos salvos em {nome_arquivo}.")
    elif opcao == "2":
        fetch_author_publications()
    else:
        print("Opção inválida.")

if __name__ == "__main__":
    main()