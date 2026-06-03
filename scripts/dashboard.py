import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as pgo
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
    "Citações e Índice H por Nível Acadêmico": "Citações_Médias_por_Nível_Acadêmico.sparql",
    "Distribuição de Autores por Faixa de Índice H": "Faixas_de_Citação_por_Nível_Acadêmico.sparql",
    "Impacto por Conceito CAPES e Nível": "Impacto_por_Conceito_CAPES_e_Nível.sparql",
}

selecao = st.sidebar.radio("Ir para:", list(analises.keys()))

st.sidebar.markdown("---")
st.sidebar.info(
    "💡 **Dica:** Passe o mouse sobre os gráficos para ver os dados exatos. "
    "Você pode dar zoom arrastando o mouse."
)

# Tela inicial
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
        "Mestrado (Regular + Profissional)": "#636EFA",
        "Doutorado (Regular + Profissional)": "#EF553B",
    }
    ORDEM_GRUPOS = [
        "Mestrado (Regular + Profissional)",
        "Doutorado (Regular + Profissional)",
    ]
    MAPA_GRUPO = {
        "MESTRADO": "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL": "Mestrado (Regular + Profissional)",
        "DOUTORADO": "Doutorado (Regular + Profissional)",
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
            "Ano de fim:",
            ANOS_DISPONIVEIS,
            index=len(ANOS_DISPONIVEIS) - 1,
            key="vg_fim",
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
    qt1 = qt1.replace("{ano_inicio}", str(ano_inicio_vg)).replace(
        "{ano_fim}", str(ano_fim_vg)
    )
    df1 = query_fuseki(qt1)

    if not df1.empty:
        df1["grupo"] = df1["nivel"].replace(MAPA_GRUPO)
        df1["ano"] = df1["ano"].astype(str)
        df1_agr = df1.groupby(["ano", "grupo"], as_index=False).agg(
            totalDiscentes=("totalDiscentes", "sum")
        )
        df1_agr = df1_agr.sort_values("ano")  # type: ignore
        fig1 = px.bar(
            df1_agr,
            x="ano",
            y="totalDiscentes",
            color="grupo",
            barmode="group",
            title=f"Discentes Titulados por Nível Acadêmico — {titulo_periodo}",
            labels={
                "ano": "Ano",
                "totalDiscentes": "Nº de Discentes Titulados",
                "grupo": "Nível",
            },
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
    qt2 = qt2.replace("{ano_inicio}", str(ano_inicio_vg)).replace(
        "{ano_fim}", str(ano_fim_vg)
    )
    df2 = query_fuseki(qt2)

    if not df2.empty:
        df2["grupo"] = df2["nivel"].replace(MAPA_GRUPO)
        df2["ano"] = df2["ano"].astype(str)
        df2_agr = df2.groupby(["ano", "grupo"], as_index=False).agg(
            totalProducoes=("totalProducoes", "sum")
        )
        df2_agr = df2_agr.sort_values("ano")  # type: ignore
        fig2 = px.bar(
            df2_agr,
            x="ano",
            y="totalProducoes",
            color="grupo",
            barmode="group",
            title=f"Total de Publicações por Nível Acadêmico — {titulo_periodo}",
            labels={
                "ano": "Ano",
                "totalProducoes": "Total de Publicações",
                "grupo": "Nível",
            },
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
    qt3 = qt3.replace("{ano_inicio}", str(ano_inicio_vg)).replace(
        "{ano_fim}", str(ano_fim_vg)
    )
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
        df3_agr["producoesPorDiscente"] = df3_agr["totalProducoes"] / df3_agr[
            "totalDiscentes"
        ].replace(0, float("nan"))
        fig3 = px.bar(
            df3_agr,
            x="ano",
            y="producoesPorDiscente",
            color="grupo",
            barmode="group",
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

    st.markdown(
        "👈 **Utilize o menu lateral para explorar indicadores mais detalhados.**"
    )

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
        "Mestrado (Regular + Profissional)": "#636EFA",
        "Doutorado (Regular + Profissional)": "#EF553B",
    }
    ORDEM_GRUPOS_CTI = [
        "Mestrado (Regular + Profissional)",
        "Doutorado (Regular + Profissional)",
    ]
    MAPA_GRUPO_CTI = {
        "MESTRADO": "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL": "Mestrado (Regular + Profissional)",
        "DOUTORADO": "Doutorado (Regular + Profissional)",
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
    query = query_template.replace("{ano_inicio}", str(ano_inicio)).replace(
        "{ano_fim}", str(ano_fim)
    )
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

elif selecao == "Distribuição de Autores por Faixa de Índice H":
    st.title("📊 Distribuição de Autores por Faixa de Índice H ao Longo dos Anos")
    st.caption(
        "Acompanhe a evolução da distribuição de autores nas diferentes faixas de Índice H (Hirsch) "
        "ao longo dos anos, comparando mestrandos e doutorandos. "
        "Os dados de Mestrado agrupam Mestrado Regular e Profissional. "
        "Os dados de Doutorado agrupam Doutorado Regular e Profissional."
    )

    ANOS_DISPONIVEIS_H = list(range(2018, 2025))
    MAPA_GRUPO_H = {
        "MESTRADO": "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL": "Mestrado (Regular + Profissional)",
        "DOUTORADO": "Doutorado (Regular + Profissional)",
        "DOUTORADO PROFISSIONAL": "Doutorado (Regular + Profissional)",
    }

    # Filtro temporal
    col_ini, col_fim = st.columns(2)
    with col_ini:
        ano_inicio_h = st.selectbox(
            "Ano de início:", ANOS_DISPONIVEIS_H, index=0, key="h_ini"
        )
    with col_fim:
        ano_fim_h = st.selectbox(
            "Ano de fim:",
            ANOS_DISPONIVEIS_H,
            index=len(ANOS_DISPONIVEIS_H) - 1,
            key="h_fim",
        )

    if ano_inicio_h > ano_fim_h:
        st.warning("O ano de início não pode ser maior que o ano de fim.")
        st.stop()

    titulo_periodo_h = (
        f"{ano_inicio_h}"
        if ano_inicio_h == ano_fim_h
        else f"{ano_inicio_h}–{ano_fim_h}"
    )

    # Carregar e executar consulta
    query_template = load_sparql_file("Faixas_de_Citação_por_Nível_Acadêmico.sparql")
    query = query_template.replace("{ano_inicio}", str(ano_inicio_h)).replace(
        "{ano_fim}", str(ano_fim_h)
    )

    df = query_fuseki(query)

    if not df.empty:
        # Mapear níveis para grupos
        df["nivel"] = df["nivel"].replace(MAPA_GRUPO_H)

        # Converter ano para inteiro para ordenação correta
        df["ano"] = df["ano"].astype(int)

        # Agrupar por ano, faixa de Índice H e nível
        df_agr = df.groupby(
            ["ano", "faixaIndiceH", "nivel", "ordemFaixa"], as_index=False
        ).agg(quantidade=("quantidade", "sum"))
        df_agr = df_agr.sort_values(["ano", "ordemFaixa"])  # type: ignore

        # Definir ordem das faixas
        ordem_faixas = [
            "0-5 - Iniciante",
            "6-15 - Consolidado",
            "16-30 - Líder",
            "30+ - Elite",
        ]

        # Cores para cada faixa
        cores_faixas = {
            "0-5 - Iniciante": "#00E5FF",  # Azul Ciano
            "6-15 - Consolidado": "#FFEA00",  # Amarelo Limão
            "16-30 - Líder": "#FF00FF",  # Magenta Neon
            "30+ - Elite": "#FF6D00",  # Laranja Vibrante
        }

        # Gráfico de linhas
        fig = px.line(
            df_agr,
            x="ano",
            y="quantidade",
            color="faixaIndiceH",
            line_dash="nivel",
            markers=True,
            title=f"Evolução da Distribuição de Autores por Faixa de Índice H — {titulo_periodo_h}",
            labels={
                "ano": "Ano",
                "quantidade": "Nº de Autores",
                "faixaIndiceH": "Faixa de Índice H",
                "nivel": "Nível Acadêmico",
            },
            color_discrete_map=cores_faixas,
        )

        # Customizar estilos de linha
        for trace in fig.data:
            if isinstance(trace, (pgo.Scatter, pgo.Scattergl)):
                name = getattr(trace, "name", "")

                if name and "Mestrado" in name:
                    trace.line = dict(dash="solid")
                    trace.marker = dict(symbol="circle")
                elif name and "Doutorado" in name:
                    trace.line = dict(dash="dash")
                    trace.marker = dict(symbol="square")

        fig.update_layout(
            legend=dict(
                title=dict(
                    text="<b>Faixa de Índice H</b>", font=dict(size=12, color="white")
                ),
                font=dict(family="Arial, sans-serif", size=12, color="white"),
            ),
            hovermode="x unified",
            height=500,
            margin=dict(t=80, b=80, l=80, r=250),
        )

        fig.update_xaxes(type="category")

        st.plotly_chart(fig, use_container_width=True)

        # Tabela de dados
        with st.expander("Ver dados tabulares"):
            st.dataframe(df_agr)

        # Legenda explicativa
        st.caption(
            "💡 **Índice H (Hirsch):** Métrica que mede o impacto acadêmico de um autor. "
            "Um autor com índice H = 10 tem pelo menos 10 publicações com 10 ou mais citações cada. "
            "**Faixas:** 0-5 (Iniciante), 6-15 (Consolidado), 16-30 (Líder), 30+ (Elite). "
            "**Linhas sólidas:** Mestrado | **Linhas tracejadas:** Doutorado"
        )
    else:
        st.info("Sem dados disponíveis para o período selecionado.")

elif selecao == "Impacto por Conceito CAPES e Nível":
    st.title("🏅 Impacto Científico por Nota CAPES e Nível Acadêmico")
    st.caption(
        "Cruza a nota CAPES do PPG com o impacto dos seus discentes, respondendo: "
        "discentes de programas mais bem avaliados publicam com mais qualidade? "
        "Os dados de Mestrado agrupam Regular e Profissional; idem para Doutorado."
    )

    CORES_GRUPO_CAPES = {
        "Mestrado (Regular + Profissional)": "#636EFA",
        "Doutorado (Regular + Profissional)": "#EF553B",
    }
    ORDEM_GRUPOS_CAPES = [
        "Mestrado (Regular + Profissional)",
        "Doutorado (Regular + Profissional)",
    ]
    MAPA_GRUPO_CAPES = {
        "MESTRADO": "Mestrado (Regular + Profissional)",
        "MESTRADO PROFISSIONAL": "Mestrado (Regular + Profissional)",
        "DOUTORADO": "Doutorado (Regular + Profissional)",
        "DOUTORADO PROFISSIONAL": "Doutorado (Regular + Profissional)",
    }

    ANOS_DISPONIVEIS_CAPES = list(range(2018, 2025))
    col_ini, col_fim = st.columns(2)
    with col_ini:
        ano_inicio_capes = st.selectbox(
            "Ano de início:", ANOS_DISPONIVEIS_CAPES, index=0, key="capes_ini"
        )
    with col_fim:
        ano_fim_capes = st.selectbox(
            "Ano de fim:",
            ANOS_DISPONIVEIS_CAPES,
            index=len(ANOS_DISPONIVEIS_CAPES) - 1,
            key="capes_fim",
        )

    if ano_inicio_capes > ano_fim_capes:
        st.warning("O ano de início não pode ser maior que o ano de fim.")
        st.stop()

    titulo_periodo_capes = (
        f"{ano_inicio_capes}"
        if ano_inicio_capes == ano_fim_capes
        else f"{ano_inicio_capes}–{ano_fim_capes}"
    )

    query_template = load_sparql_file(analises[selecao])
    query = query_template.replace("{ano_inicio}", str(ano_inicio_capes)).replace(
        "{ano_fim}", str(ano_fim_capes)
    )
    df = query_fuseki(query)

    if not df.empty:
        df["grupo"] = df["nivel"].replace(MAPA_GRUPO_CAPES)
        df["conceito"] = df["conceito"].astype(str)

        df_agr = df.groupby(["conceito", "grupo"], as_index=False).agg(
            mediaCitacoes=("mediaCitacoes", "mean"),
            mediaIndiceH=("mediaIndiceH", "mean"),
            totalAutores=("totalAutores", "sum"),
        )
        df_agr = df_agr.sort_values("conceito")  # type: ignore

        fig1 = px.bar(
            df_agr,
            x="conceito",
            y="mediaCitacoes",
            color="grupo",
            barmode="group",
            title=f"Média de Citações por Nota CAPES do PPG e Nível Acadêmico — {titulo_periodo_capes}",
            labels={
                "conceito": "Nota CAPES do PPG",
                "mediaCitacoes": "Média de Citações",
                "grupo": "Nível",
            },
            text_auto=".1f",  # pyright: ignore[reportArgumentType]
            color_discrete_map=CORES_GRUPO_CAPES,
            category_orders={"grupo": ORDEM_GRUPOS_CAPES},
        )
        fig1.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig1, use_container_width=True)

        fig2 = px.bar(
            df_agr,
            x="conceito",
            y="mediaIndiceH",
            color="grupo",
            barmode="group",
            title=f"Índice H Médio por Nota CAPES do PPG e Nível Acadêmico — {titulo_periodo_capes}",
            labels={
                "conceito": "Nota CAPES do PPG",
                "mediaIndiceH": "Índice H Médio",
                "grupo": "Nível",
            },
            text_auto=".2f",  # pyright: ignore[reportArgumentType]
            color_discrete_map=CORES_GRUPO_CAPES,
            category_orders={"grupo": ORDEM_GRUPOS_CAPES},
        )
        fig2.update_layout(legend_title_text="Nível Acadêmico")
        st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Ver dados tabulares"):
            st.dataframe(df_agr)
    else:
        st.info("Sem dados disponíveis para este indicador.")
