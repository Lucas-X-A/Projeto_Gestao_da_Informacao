import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from SPARQLWrapper import JSON, SPARQLWrapper

# Configuração da página do Streamlit
st.set_page_config(page_title="Dashboard CT&I-PE", layout="wide", page_icon="📊")

# URL do Endpoint do Jena Fuseki
FUSEKI_ENDPOINT = os.getenv(
    "FUSEKI_ENDPOINT", "https://fuseki-km.onrender.com/cti/sparql"
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
                linha = {
                    chave: valor["value"]
                    for chave, valor in row.items()
                    if isinstance(valor, dict)
                }
                dados_limpos.append(linha)

        df = pd.DataFrame(dados_limpos)

        if not df.empty:
            df = df.apply(pd.to_numeric, errors="ignore")

        return df  # pyright: ignore[reportReturnType]

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
    """)

    st.markdown(
        """
    <div style="
        background-color: #1a3a5c;
        border-left: 6px solid #4da6ff;
        border-radius: 8px;
        padding: 20px 24px;
        margin: 16px 0;
    ">
        <p style="
            color: #a8d4ff;
            font-size: 0.85rem;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin: 0 0 8px 0;
        ">🎯 Pergunta Central do KM</p>
        <p style="
            color: #e8f4ff;
            font-size: 1.2rem;
            font-weight: 600;
            font-style: italic;
            margin: 0;
            line-height: 1.5;
        ">O maior grau de instrução de um discente promove aumento na qualidade e quantidade de suas publicações científicas?</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.subheader("📊 Indicadores Gerais por Período")
    st.caption(
        "Os dados de Mestrado agrupam Mestrado Regular e Profissional. "
        "Os dados de Doutorado agrupam Doutorado Regular e Profissional."
    )

    # Constantes compartilhadas pelos 3 gráficos
    ANOS_DISPONIVEIS = list(range(2018, 2025))
    CORES_GRUPO = {
        "Mestrado (Regular + Profissional)":  "#636EFA",
        "Doutorado (Regular + Profissional)": "#EF553B",
    }
    ORDEM_GRUPOS = [
        "Mestrado (Regular + Profissional)",
        "Doutorado (Regular + Profissional)",
    ]
    MAPA_GRUPO = {
        "MESTRADO":               "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL":  "Mestrado (Regular + Profissional)",
        "DOUTORADO":              "Doutorado (Regular + Profissional)",
        "DOUTORADO PROFISSIONAL": "Doutorado (Regular + Profissional)",
    }

    # Filtro temporal compartilhado
    col_ini, col_fim = st.columns(2)
    with col_ini:
        ano_inicio_vg = st.selectbox(
            "Ano de início:", ANOS_DISPONIVEIS, index=0, key="vg_ini"
        )
    with col_fim:
        ano_fim_vg = st.selectbox(
            "Ano de fim:", ANOS_DISPONIVEIS, index=len(ANOS_DISPONIVEIS) - 1, key="vg_fim"
        )

    if ano_inicio_vg > ano_fim_vg:
        st.warning("O ano de início não pode ser maior que o ano de fim.")
        st.stop()

    titulo_periodo = (
        f"{ano_inicio_vg}"
        if ano_inicio_vg == ano_fim_vg
        else f"{ano_inicio_vg}–{ano_fim_vg}"
    )

    # Gráfico 1 da Visão Geral: Discentes titulados por ano e nível
    qt1 = load_sparql_file("Discentes_Titulados_por_Nível_e_Ano.sparql")
    qt1 = qt1.replace("{ano_inicio}", str(ano_inicio_vg)).replace("{ano_fim}", str(ano_fim_vg))
    df1 = query_fuseki(qt1)

    if not df1.empty:
        df1["grupo"] = df1["nivel"].replace(MAPA_GRUPO)
        df1["ano"] = df1["ano"].astype(str)
        df1_agr = df1.groupby(["ano", "grupo"], as_index=False).agg(totalDiscentes=("totalDiscentes", "sum"))
        df1_agr = df1_agr.sort_values("ano")  # type: ignore
        fig1 = px.bar(
            df1_agr, x="ano", y="totalDiscentes", color="grupo", barmode="group",
            title=f"Discentes Titulados por Nível Acadêmico — {titulo_periodo}",
            labels={"ano": "Ano", "totalDiscentes": "Nº de Discentes Titulados", "grupo": "Nível"},
            text_auto=True,
            color_discrete_map=CORES_GRUPO,
            category_orders={"grupo": ORDEM_GRUPOS},
        )
        fig1.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("Sem dados de discentes titulados para o período selecionado.")

    # Gráfico 2 da Visão Geral: Total de publicações por ano e nível
    qt2 = load_sparql_file("Produções_por_Nível_e_Ano.sparql")
    qt2 = qt2.replace("{ano_inicio}", str(ano_inicio_vg)).replace("{ano_fim}", str(ano_fim_vg))
    df2 = query_fuseki(qt2)

    if not df2.empty:
        df2["grupo"] = df2["nivel"].replace(MAPA_GRUPO)
        df2["ano"] = df2["ano"].astype(str)
        df2_agr = df2.groupby(["ano", "grupo"], as_index=False).agg(totalProducoes=("totalProducoes", "sum"))
        df2_agr = df2_agr.sort_values("ano")  # type: ignore
        fig2 = px.bar(
            df2_agr, x="ano", y="totalProducoes", color="grupo", barmode="group",
            title=f"Total de Publicações por Nível Acadêmico — {titulo_periodo}",
            labels={"ano": "Ano", "totalProducoes": "Total de Publicações", "grupo": "Nível"},
            text_auto=True,
            color_discrete_map=CORES_GRUPO,
            category_orders={"grupo": ORDEM_GRUPOS},
        )
        fig2.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Sem dados de publicações para o período selecionado.")

    # Gráfico 3 da Visão Geral: Publicações por discente titulado (produtividade)
    qt3 = load_sparql_file("Produtividade_por_Discente_Titulado.sparql")
    qt3 = qt3.replace("{ano_inicio}", str(ano_inicio_vg)).replace("{ano_fim}", str(ano_fim_vg))
    df3 = query_fuseki(qt3)

    if not df3.empty:
        df3["grupo"] = df3["nivel"].replace(MAPA_GRUPO)
        df3["ano"] = df3["ano"].astype(str)
        df3_agr = df3.groupby(["ano", "grupo"], as_index=False).agg(
            totalDiscentes=("totalDiscentes", "sum"),
            totalProducoes=("totalProducoes", "sum"),
        )
        df3_agr = df3_agr.sort_values("ano")  # type: ignore
        # Produtividade = publicações / discente titulado 
        df3_agr["producoesPorDiscente"] = (
            df3_agr["totalProducoes"]
            / df3_agr["totalDiscentes"].replace(0, float("nan"))
        )
        fig3 = px.bar(
            df3_agr, x="ano", y="producoesPorDiscente", color="grupo", barmode="group",
            title=f"Publicações por Discente Titulado — {titulo_periodo}",
            labels={
                "ano": "Ano",
                "producoesPorDiscente": "Publicações / Discente Titulado",
                "grupo": "Nível",
            },
            text_auto=".1f",  # pyright: ignore[reportArgumentType]
            color_discrete_map=CORES_GRUPO,
            category_orders={"grupo": ORDEM_GRUPOS},
        )
        fig3.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig3, use_container_width=True)
        st.caption(
            "💡 Este indicador normaliza o volume de publicações pelo número de discentes titulados "
            "no mesmo período, permitindo comparar a produtividade científica entre os níveis de forma justa — "
            "independentemente de quantos alunos cada nível tem."
        )
    else:
        st.info("Sem dados de produtividade para o período selecionado.")

    st.markdown("👈 **Utilize o menu lateral para explorar indicadores mais detalhados.**")

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
            "Selecione os Programas (PPGs) para comparar:",
            programas,
            default=programas[:5],
        )

        if selecionados:
            df_filtrado = df[df["nomePPG"].isin(selecionados)].copy()
            df_filtrado = df_filtrado.sort_values("ano")  # type: ignore

            fig = px.line(
                df_filtrado,
                x="ano",
                y="conceito",
                color="nomePPG",
                markers=True,
                title="Evolução de Notas CAPES por Programa",
                labels={
                    "ano": "Ano",
                    "conceito": "Conceito CAPES",
                    "nomePPG": "Programa",
                },
            )
            fig.update_xaxes(type="category")
            fig.update_yaxes(categoryorder="category ascending")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Selecione pelo menos um programa para visualizar o gráfico.")

elif selecao == "Produções por Índice H":
    st.title("📊 Produções por Índice H do Autor")
    st.caption(
        "Distribui o volume total de produções pelo índice H dos autores, mostrando se autores mais impactantes produzem mais."
    )
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        df = df.sort_values("indiceH")  # type: ignore
        fig = px.bar(
            df,
            x="indiceH",
            y="totalProducoes",
            title="Volume de Produções Distribuído pelo Índice H do Autor",
            labels={"indiceH": "Índice H", "totalProducoes": "Total de Produções"},
        )
        fig.update_xaxes(type="category")
        st.plotly_chart(fig, use_container_width=True)

elif selecao == "Distribuição por Faixa de Citação":
    st.title("🥧 Distribuição de Autores por Faixa de Citação")
    st.caption(
        "Agrupa todos os autores (discentes) em faixas de impacto pelo total de citações acumuladas."
    )
    query = load_sparql_file(analises[selecao])
    df = query_fuseki(query)

    if not df.empty:
        fig = px.pie(
            df,
            names="faixaCitacao",
            values="quantidade",
            title="Representatividade das Faixas de Citação nas Produções dos Discentes",
            category_orders={
                "faixaCitacao": [
                    "0 - Sem impacto latente",
                    "1 a 10 - Impacto Inicial",
                    "11 a 50 - Impacto Consolidado",
                    "50+ - Elite Científica",
                ]
            },
        )
        fig.update_traces(textposition="inside", textinfo="percent")
        fig.update_layout(
            legend=dict(font=dict(size=16)), legend_title=dict(font=dict(size=16))
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df[["faixaCitacao", "quantidade"]])

elif selecao == "Citações e Índice H por Nível Acadêmico":
    st.title("🎓 Citações Médias e Índice H por Nível Acadêmico")
    st.caption(
        "Responde diretamente à pergunta do KM: discentes de Doutorado apresentam maior "
        "média de citações e índice H do que os de Mestrado? "
        "Os dados de Mestrado incluem Mestrado Regular e Profissional. "
        "Os dados de Doutorado incluem Doutorado Regular e Profissional."
    )
 
    ANOS_DISPONIVEIS_CTI = list(range(2018, 2025))
    CORES_GRUPO_CTI = {
        "Mestrado (Regular + Profissional)":  "#636EFA",
        "Doutorado (Regular + Profissional)": "#EF553B",
    }
    ORDEM_GRUPOS_CTI = [
        "Mestrado (Regular + Profissional)",
        "Doutorado (Regular + Profissional)",
    ]
    MAPA_GRUPO_CTI = {
        "MESTRADO":               "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL":  "Mestrado (Regular + Profissional)",
        "DOUTORADO":              "Doutorado (Regular + Profissional)",
        "DOUTORADO PROFISSIONAL": "Doutorado (Regular + Profissional)",
    }
 
    # Dropdowns lado a lado para início e fim do período
    col_ini, col_fim = st.columns(2)
    with col_ini:
        ano_inicio = st.selectbox(
            "Ano de início:",
            options=ANOS_DISPONIVEIS_CTI,
            index=0,
            key="cti_ini",
        )
    with col_fim:
        ano_fim = st.selectbox(
            "Ano de fim:",
            options=ANOS_DISPONIVEIS_CTI,
            index=len(ANOS_DISPONIVEIS_CTI) - 1,
            key="cti_fim",
        )
 
    if ano_inicio > ano_fim:
        st.warning("O ano de início não pode ser maior que o ano de fim.")
        st.stop()
 
    titulo_periodo_cti = (
        f"{ano_inicio}" if ano_inicio == ano_fim else f"{ano_inicio}–{ano_fim}"
    )
 
    query_template = load_sparql_file(analises[selecao])
    query = (query_template
             .replace("{ano_inicio}", str(ano_inicio))
             .replace("{ano_fim}", str(ano_fim)))
    df = query_fuseki(query)
 
    if not df.empty:
        # Mescla subníveis em grupos (Regular + Profissional)
        df["grupo"] = df["nivel"].replace(MAPA_GRUPO_CTI)
        df["ano"] = df["ano"].astype(str)
 
        df_agrupado = df.groupby(["ano", "grupo"], as_index=False).agg(
            mediaCitacoes=("mediaCitacoes", "mean"),
            mediaIndiceH=("mediaIndiceH", "mean"),
        )
        df_agrupado = df_agrupado.sort_values("ano")  # type: ignore
 
        # Gráfico 1 — Média de Citações (largura total)
        fig1 = px.bar(
            df_agrupado,
            x="ano",
            y="mediaCitacoes",
            color="grupo",
            barmode="group",
            title=f"Média de Citações por Nível Acadêmico — {titulo_periodo_cti}",
            labels={
                "ano": "Ano de Avaliação",
                "mediaCitacoes": "Média de Citações",
                "grupo": "Nível",
            },
            text_auto=".1f",  # pyright: ignore[reportArgumentType]
            color_discrete_map=CORES_GRUPO_CTI,
            category_orders={"grupo": ORDEM_GRUPOS_CTI},
        )
        fig1.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig1, use_container_width=True)
 
        # Gráfico 2 — Índice H Médio
        fig2 = px.bar(
            df_agrupado,
            x="ano",
            y="mediaIndiceH",
            color="grupo",
            barmode="group",
            title=f"Índice H Médio por Nível Acadêmico — {titulo_periodo_cti}",
            labels={
                "ano": "Ano de Avaliação",
                "mediaIndiceH": "Índice H Médio",
                "grupo": "Nível",
            },
            text_auto=".2f",  # pyright: ignore[reportArgumentType]
            color_discrete_map=CORES_GRUPO_CTI,
            category_orders={"grupo": ORDEM_GRUPOS_CTI},
        )
        fig2.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig2, use_container_width=True)
 
        with st.expander("Ver dados tabulares"):
            st.dataframe(df_agrupado)
    else:
        st.warning("Nenhum dado encontrado para o intervalo selecionado.")

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
            "50+ - Elite Científica",
        ]
        fig = px.bar(
            df,
            x="faixaCitacao",
            y="quantidade",
            color="nivel",
            barmode="group",
            title="Distribuição de Autores por Faixa de Citação e Nível Acadêmico",
            labels={
                "faixaCitacao": "Faixa de Citação",
                "quantidade": "Nº de Autores",
                "nivel": "Nível",
            },
            category_orders={"faixaCitacao": ordem_faixas},
            color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"},
            text_auto=True,
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
            df,
            x="nivel",
            y="mediaProducoesPorAutor",
            color="nivel",
            title="Média de Produções por Autor por Nível Acadêmico",
            labels={
                "nivel": "Nível Acadêmico",
                "mediaProducoesPorAutor": "Média de Produções/Autor",
            },
            text_auto=".2f",  # pyright: ignore[reportArgumentType]
            color_discrete_map={"MESTRADO": "#636EFA", "DOUTORADO": "#EF553B"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver dados tabulares"):
            st.dataframe(df)