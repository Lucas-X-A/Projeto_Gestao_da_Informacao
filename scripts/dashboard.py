from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from SPARQLWrapper import JSON, SPARQLWrapper

# Configuração da página do Streamlit
st.set_page_config(page_title="Dashboard CT&I-PE", layout="wide", page_icon="📊")

# URL do Endpoint do Jena Fuseki 
FUSEKI_ENDPOINT = "http://localhost:3030/cti-pe/query"


# 
def load_sparql_file(filename: str) -> str:
    """Lê dinamicamente o conteúdo de um arquivo .sparql do disco."""
    filepath = Path(f"src/sparql/{filename}")
    if not filepath.exists():
        st.error(f"Arquivo de consulta não encontrado no caminho: {filepath}")
        return ""
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return file.read()
    except Exception as e:
        st.error(f"Erro ao ler o arquivo {filename}: {e}")
        return ""


@st.cache_data(ttl=3600)  # Cache por 1 hora para proteger a performance do Fuseki
def query_fuseki(sparql_query: str) -> pd.DataFrame:
    """Executa uma consulta SPARQL no Jena Fuseki e retorna um DataFrame do Pandas."""
    if not sparql_query:
        return pd.DataFrame()
        
    try:
        sparql = SPARQLWrapper(FUSEKI_ENDPOINT)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        
        # Executa e converte para dicionário Python
        resultados_json = sparql.query().convert()
        
        # Extrai os resultados do JSON retornado pelo Fuseki
        bindings = resultados_json.get("results", {}).get("bindings", []) # type: ignore
        
        dados_limpos = []
        for row in bindings:
            if isinstance(row, dict):
                linha = {chave: valor["value"] for chave, valor in row.items() if isinstance(valor, dict)}
                dados_limpos.append(linha)
            
        df = pd.DataFrame(dados_limpos)
        
        if not df.empty:
            df = df.apply(pd.to_numeric, errors='ignore')
            
        return df

    except Exception as e:
        st.error(f"Erro ao conectar ou consultar o Jena Fuseki: {e}")
        return pd.DataFrame()


# Menu lateral para navegação entre as análises
st.sidebar.title("📊 Navegação CT&I-PE")
st.sidebar.markdown("Selecione o indicador que deseja analisar:")

analises = {
    "Visão Geral": "home",
    "Produção por ICT e Nível": "Número total de produções dividido entre Mestrado e Doutorado, ordernado por Universidade.sparql",
    "Evolução de Notas dos PPGs": "Evolução Temporal das Notas dos Programas.sparql",
    "Matriz de Produtividade": "Matriz de Produtividade do Autor (Volume vs. Impacto).sparql",
    "Consistência do Autor": "Consistência do Autor.sparql",
    "Média de Citações por Conceito": "Media de Citacoes por Conceito CAPES.sparql",
    "Produções por Índice H": "Produções por Indice H.sparql",
    "Distribuição por Faixa de Citação": "Distribuição de Produção por Faixas de Citação.sparql" 
}

selecao = st.sidebar.radio("Ir para:", list(analises.keys()))

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 **Dica:** Passe o mouse sobre os gráficos para ver os dados exatos. Você pode dar zoom arrastando o mouse."
)

# Renderização condicional 
if selecao == "Visão Geral":
    st.title("Bem-vindo ao Painel Integrado CT&I-PE 🎓")
    st.markdown("""
    Esta é a área de visualização interativa da nossa Solução de Gestão do Conhecimento (KM).
    
    Diferente de relatórios estáticos, este painel executa consultas **SPARQL em tempo real** diretamente 
    no nosso servidor triplestore **Jena Fuseki**, garantindo dados sempre atualizados com a base OML.

    👈 **Utilize o menu lateral para navegar entre os diferentes indicadores extraídos.**
    """)

elif selecao == "Produção por ICT e Nível":
    st.title("🏛️ Produção Científica por ICT e Nível Acadêmico")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        fig = px.bar(
            df, x="siglaICT", y="totalProducoes", color="nivel",
            title="Total de Produções por Instituição e Nível (Mestrado vs Doutorado)",
            labels={"siglaICT": "Instituição", "totalProducoes": "Total de Produções", "nivel": "Nível"},
            barmode="group", text_auto=True
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver resposta bruta do Fuseki (Dados Tabulares)"):
            st.dataframe(df)

elif selecao == "Evolução de Notas dos PPGs":
    st.title("📈 Evolução Temporal dos Conceitos CAPES")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        programas = df["nomePPG"].unique().tolist()
        selecionados = st.multiselect(
            "Selecione os Programas (PPGs) para comparar:", programas, default=programas[:5]
        )

        if selecionados:
            df_filtrado = df[df["nomePPG"].isin(selecionados)].copy()
            df_filtrado = df_filtrado.sort_values("ano")  # type: ignore
            
            fig = px.line(
                df_filtrado, x="ano", y="conceito", color="nomePPG", markers=True,
                title="Evolução de Notas CAPES por Programa",
                labels={"ano": "Ano", "conceito": "Conceito CAPES", "nomePPG": "Programa"}
            )
            fig.update_xaxes(type="category")
            fig.update_yaxes(categoryorder="category ascending")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Selecione pelo menos um programa para visualizar o gráfico.")

elif selecao == "Matriz de Produtividade":
    st.title("🧑‍🔬 Matriz de Produtividade do Autor (Volume vs Impacto)")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        df = df.dropna(subset=["totalProducoes", "totalCitacoes"])
        fig = px.scatter(
            df, x="totalProducoes", y="totalCitacoes", size="indiceH", color="indiceI10",
            hover_name="nomeAutor",
            title="Produtividade Científica (Tamanho da bolha = Índice H)",
            labels={"totalProducoes": "Volume (Total de Produções)", "totalCitacoes": "Impacto (Total de Citações)"},
            log_y=True, log_x=True  # Escala logarítmica para lidar com os outliers de grandes pesquisadores
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver resposta bruta do Fuseki (Dados Tabulares)"):
            st.dataframe(df)

elif selecao == "Consistência do Autor":
    st.title("🎯 Consistência do Autor")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        fig = px.scatter(
            df, x="indiceH", y="totalPublicacoes", size="nrCitacoesAutor", hover_name="nomeAutor",
            color="indiceH",
            title="Índice H vs Total de Publicações (Tamanho da bolha = Citações Totais)",
            labels={"indiceH": "Índice H", "totalPublicacoes": "Total de Publicações"}
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver resposta bruta do Fuseki (Dados Tabulares)"):
            st.dataframe(df)

elif selecao == "Média de Citações por Conceito":
    st.title("⭐ Média de Citações por Conceito CAPES")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        df["conceito"] = df["conceito"].astype(str)
        df = df.sort_values("conceito")  # type: ignore
        
        fig = px.bar(
            df, x="conceito", y="mediaCitacoes", color="conceito",
            title="Média de Citações de Acordo com a Nota do Programa (CAPES)",
            labels={"conceito": "Conceito CAPES", "mediaCitacoes": "Média de Citações"},
            text_auto=True
        )
        fig.update_traces(texttemplate='%{y:.1f}')
        st.plotly_chart(fig, use_container_width=True)

elif selecao == "Produções por Índice H":
    st.title("📊 Produções por Índice H do Autor")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        df = df.sort_values("indiceH")  # type: ignore
        fig = px.bar(
            df, x="indiceH", y="totalProducoes",
            title="Volume de Produções Distribuído pelo Índice H do Autor",
            labels={"indiceH": "Índice H", "totalProducoes": "Total de Produções"},
        )
        fig.update_xaxes(type="category")
        st.plotly_chart(fig, use_container_width=True)

elif selecao == "Distribuição por Faixa de Citação":
    st.title("🥧 Distribuição de Produção por Faixas de Citação")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)
    
    if not df.empty:
        # Agrupa os dados vindo do SPARQL para garantir coerência no gráfico de pizza
        df_agrupado = df.groupby("faixaCitacao")["quantidade"].sum().reset_index()
        fig = px.pie(
            df_agrupado, names="faixaCitacao", values="quantidade", hole=0.4,
            title="Representatividade das Faixas de Citação nas Produções dos Discentes"
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)