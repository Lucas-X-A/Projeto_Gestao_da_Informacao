from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from SPARQLWrapper import JSON, SPARQLWrapper

# Configuração da página do Streamlit
st.set_page_config(page_title="Dashboard CT&I-PE", layout="wide", page_icon="📊")

# URL do Endpoint do Jena Fuseki
import os
FUSEKI_ENDPOINT = os.getenv(
    "FUSEKI_ENDPOINT",
    "https://fuseki-km.onrender.com/cti/sparql"
)


# Componente para carregar consultas SPARQL de arquivos .sparql
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

        resultados_json = sparql.query().convert()
        bindings = resultados_json.get("results", {}).get("bindings", [])  # type: ignore

        dados_limpos = []
        for row in bindings:
            if isinstance(row, dict):
                linha = {chave: valor["value"] for chave, valor in row.items() if isinstance(valor, dict)}
                dados_limpos.append(linha)

        df = pd.DataFrame(dados_limpos)

        if not df.empty:
            df = df.apply(pd.to_numeric, errors='ignore')

        return df   # pyright: ignore[reportReturnType]

    except Exception as e:
        st.error(f"Erro ao conectar ou consultar o Jena Fuseki: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Menu lateral
# ---------------------------------------------------------------------------
st.sidebar.title("📊 Navegação CT&I-PE")
st.sidebar.markdown("Selecione o indicador que deseja analisar:")

analises = {
    "Visão Geral": "home",
    "Produção Científica por Nível Acadêmico na UFRPE": "Número total de produções dividido entre Mestrado e Doutorado, ordernado por Universidade.sparql",
    "Evolução de Notas dos PPGs": "Evolução Temporal das Notas dos Programas.sparql",
    "Produções por Índice H": "Produções por Indice H.sparql",
    "Distribuição por Faixa de Citação": "Distribuição_de_Produção_por_Faixas_de_Citação.sparql",
    "Citações e Índice H por Nível Acadêmico": "Citações_Médias_por_Nível_Acadêmico.sparql",
    "Faixas de Impacto por Nível Acadêmico": "Faixas_de_Citação_por_Nível_Acadêmico.sparql",
    "Produtividade Média por Nível Acadêmico": "Média_de_Produções_por_Nível_Acadêmico.sparql",
}

selecao = st.sidebar.radio("Ir para:", list(analises.keys()))

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 **Dica:** Passe o mouse sobre os gráficos para ver os dados exatos. "
    "Você pode dar zoom arrastando o mouse."
)

# ---------------------------------------------------------------------------
# Renderização condicional
# ---------------------------------------------------------------------------

if selecao == "Visão Geral":
    st.title("Bem-vindo ao Painel Integrado CT&I-PE 🎓")
    st.markdown("""
    Esta é a área de visualização interativa da nossa Solução de Gestão do Conhecimento (KM).

    Diferente de relatórios estáticos, este painel executa consultas **SPARQL em tempo real** diretamente
    no nosso servidor triplestore **Jena Fuseki**, garantindo dados sempre atualizados com a base OML.

    ### 🎯 Pergunta central do KM
    > *O maior grau de instrução de um discente promove aumento na qualidade e quantidade
    > de suas publicações científicas?*

    Os indicadores abaixo foram construídos para responder essa pergunta comparando mestrandos e
    doutorandos da UFRPE em volume de produção, citações acumuladas e índice H.

    👈 **Utilize o menu lateral para navegar entre os indicadores.**
    """)

elif selecao == "Produção Científica por Nível Acadêmico na UFRPE":
    st.title("🏛️ Produção Científica por Nível Acadêmico na UFRPE")
    st.caption("Compara o volume total de produções entre discentes de Mestrado e Doutorado.")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        # Filtra apenas UFRPE se houver mais de uma ICT nos dados
        if "siglaICT" in df.columns and df["siglaICT"].nunique() > 1:
            df = df[df["siglaICT"].str.upper() == "UFRPE"]

        fig = px.bar(
            df, x="nivel", y="totalProducoes", color="nivel",
            title="Total de Produções por Nível Acadêmico (Mestrado vs Doutorado) — UFRPE",
            labels={"nivel": "Nível Acadêmico", "totalProducoes": "Total de Produções"},
            text_auto=True,
            color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"}
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df)

elif selecao == "Evolução de Notas dos PPGs":
    st.title("📈 Evolução Temporal dos Conceitos CAPES")
    st.caption(
        "Acompanhe a evolução da nota CAPES de cada PPG ao longo do tempo. "
        "PPGs com notas mais altas tendem a exigir e produzir maior volume de publicações qualificadas — "
        "o que se reflete no impacto científico dos seus discentes."
    )
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

elif selecao == "Produções por Índice H":
    st.title("📊 Produções por Índice H do Autor")
    st.caption("Distribui o volume total de produções pelo índice H dos autores, mostrando se autores mais impactantes produzem mais.")
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
    st.title("🥧 Distribuição de Autores por Faixa de Citação")
    st.caption("Agrupa todos os autores (discentes) em faixas de impacto pelo total de citações acumuladas.")
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        fig = px.pie(
            df, names="faixaCitacao", values="quantidade",
            title="Representatividade das Faixas de Citação nas Produções dos Discentes",
            category_orders={"faixaCitacao": [
                "0 - Sem impacto latente",
                "1 a 10 - Impacto Inicial",
                "11 a 50 - Impacto Consolidado",
                "50+ - Elite Científica"
            ]}
        )
        fig.update_traces(textposition="inside", textinfo="percent")
        fig.update_layout(
            legend=dict(font=dict(size=16)),
            legend_title=dict(font=dict(size=16))
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df[["faixaCitacao", "quantidade"]])

elif selecao == "Citações e Índice H por Nível Acadêmico":
    st.title("🎓 Citações Médias e Índice H por Nível Acadêmico")
    st.caption(
        "Responde diretamente à pergunta do KM: discentes de Doutorado apresentam maior "
        "média de citações e índice H do que os de Mestrado?"
    )
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.bar(
                df, x="nivel", y="mediaCitacoes", color="nivel",
                title="Média de Citações por Nível Acadêmico",
                labels={"nivel": "Nível", "mediaCitacoes": "Média de Citações"},
                text_auto=".1f",    # pyright: ignore[reportArgumentType]
                color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"}
            )
            fig1.update_layout(showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = px.bar(
                df, x="nivel", y="mediaIndiceH", color="nivel",
                title="Índice H Médio por Nível Acadêmico",
                labels={"nivel": "Nível", "mediaIndiceH": "Índice H Médio"},
                text_auto=".2f",    # pyright: ignore[reportArgumentType]
                color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"}
            )
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Ver dados tabulares"):
            st.dataframe(df)

elif selecao == "Faixas de Impacto por Nível Acadêmico":
    st.title("📊 Faixas de Impacto por Nível Acadêmico")
    st.caption(
        "Compara como mestrandos e doutorandos se distribuem nas faixas de citação, "
        "revelando se o doutorado concentra mais autores na faixa de alto impacto."
    )
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        ordem_faixas = [
            "0 - Sem impacto",
            "1 a 10 - Impacto Inicial",
            "11 a 50 - Impacto Consolidado",
            "50+ - Elite Científica"
        ]
        fig = px.bar(
            df, x="faixaCitacao", y="quantidade", color="nivel",
            barmode="group",
            title="Distribuição de Autores por Faixa de Citação e Nível Acadêmico",
            labels={"faixaCitacao": "Faixa de Citação", "quantidade": "Nº de Autores", "nivel": "Nível"},
            category_orders={"faixaCitacao": ordem_faixas},
            color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"},
            text_auto=True
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df)

elif selecao == "Produtividade Média por Nível Acadêmico":
    st.title("📝 Produtividade Média por Nível Acadêmico")
    st.caption(
        "Calcula a média de produções por autor em cada nível, normalizando pelo número de discentes "
        "para uma comparação justa entre mestrado e doutorado."
    )
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        fig = px.bar(
            df, x="nivel", y="mediaProducoesPorAutor", color="nivel",
            title="Média de Produções por Autor por Nível Acadêmico",
            labels={"nivel": "Nível Acadêmico", "mediaProducoesPorAutor": "Média de Produções/Autor"},
            text_auto=".2f",    # pyright: ignore[reportArgumentType]
            color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"}
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df)
