#!/usr/bin/env python3
"""Gera graficos a partir dos CSVs produzidos pelas consultas SPARQL."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import re
import pandas as pd


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_DIR = PROJECT_ROOT / "build" / "results" / "csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "build" / "results" / "graficos"


def read_csv_flexible(path: Path) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(path, encoding=encoding, sep=None, engine="python")
            if len(df.columns) == 1 and ";" in str(df.columns[0]):
                df = pd.read_csv(path, encoding=encoding, sep=";")
            return df
        except Exception as exc:  # pragma: no cover - fallback path
            last_error = exc
    raise RuntimeError(f"Nao foi possivel ler {path.name}: {last_error}")


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")


def save_figure(fig: plt.Figure, output_path: Path) -> None:
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def annotate_bars(ax: plt.Axes, bars: Iterable) -> None:
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"{height:.0f}",
            (bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
        )


def sort_concepts(values: Sequence[str]) -> List[str]:
    def key(value: str) -> Tuple[int, str]:
        if value == "A":
            return (10_000, value)
        try:
            return (int(value), value)
        except ValueError:
            return (10_001, value)

    return sorted(values, key=key)


def plot_media_citacoes_por_conceito(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["conceito"] = df["conceito"].astype(str).str.strip()
    df["mediaCitacoes"] = to_numeric(df["mediaCitacoes"])
    order = sort_concepts(df["conceito"].dropna().astype(str).unique())
    df["conceito"] = pd.Categorical(df["conceito"], categories=order, ordered=True)
    df = df.sort_values("conceito")

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(df["conceito"].astype(str), df["mediaCitacoes"], color="#1f77b4")
    annotate_bars(ax, bars)
    ax.set_title("Media de citacoes por conceito CAPES")
    ax.set_xlabel("Conceito")
    ax.set_ylabel("Media de citacoes")
    ax.grid(axis="y", alpha=0.25)

    output_path = output_dir / "media_citacoes_por_conceito.png"
    save_figure(fig, output_path)
    return output_path


def plot_evolucao_notas_programas(df: pd.DataFrame, output_dir: Path, top_n: int) -> Path:
    df = df.copy()
    df["ano"] = to_numeric(df["ano"])
    df["conceito_num"] = pd.to_numeric(df["conceito"].astype(str).str.replace("A", "0", regex=False), errors="coerce")
    ranked = (
        df.groupby("nomePPG", as_index=False)["conceito_num"]
        .mean()
        .sort_values(["conceito_num", "nomePPG"], ascending=[False, True])
    )
    selected = ranked.head(top_n)["nomePPG"].tolist()
    pivot = (
        df[df["nomePPG"].isin(selected)]
        .pivot_table(index="nomePPG", columns="ano", values="conceito_num", aggfunc="mean")
        .sort_index()
    )
    pivot = pivot.reindex(selected)

    fig, ax = plt.subplots(figsize=(max(10, 0.75 * len(pivot.columns)), max(6, 0.45 * len(pivot.index))))
    heatmap = ax.imshow(pivot.values, aspect="auto", cmap="YlGnBu", interpolation="nearest")
    ax.set_title(f"Evolucao temporal das notas dos programas - top {len(selected)}")
    ax.set_xlabel("Ano")
    ax.set_ylabel("PPG")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(int(col)) if pd.notna(col) else "" for col in pivot.columns], rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    fig.colorbar(heatmap, ax=ax, label="Conceito medio")

    output_path = output_dir / "evolucao_notas_programas.png"
    save_figure(fig, output_path)
    return output_path


def plot_indice_h(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["indiceH"] = to_numeric(df["indiceH"])
    df["totalProducoes"] = to_numeric(df["totalProducoes"])
    df = df.sort_values("indiceH")

    fig, ax = plt.subplots(figsize=(10, 5))
    x = list(range(len(df)))
    labels = df["indiceH"].fillna(0).astype(int).astype(str).tolist()
    bars = ax.bar(x, df["totalProducoes"], color="#2ca02c")
    annotate_bars(ax, bars)
    ax.set_title("Producoes por indice H")
    ax.set_xlabel("Indice H")
    ax.set_ylabel("Total de producoes")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.grid(axis="y", alpha=0.25)

    output_path = output_dir / "producao_por_indice_h.png"
    save_figure(fig, output_path)
    return output_path


def plot_consistencia_autor(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["indiceH"] = to_numeric(df["indiceH"])
    df["nrCitacoesAutor"] = to_numeric(df["nrCitacoesAutor"])
    df["totalPublicacoes"] = to_numeric(df["totalPublicacoes"])

    fig, ax = plt.subplots(figsize=(10, 6))
    sizes = (df["totalPublicacoes"].fillna(0) + 1) * 18
    ax.scatter(df["indiceH"], df["nrCitacoesAutor"], s=sizes, alpha=0.7, color="#d62728", edgecolors="white", linewidths=0.5)
    top = df.nlargest(8, "nrCitacoesAutor")
    for _, row in top.iterrows():
        ax.annotate(
            clean_text(row["nomeAutor"]),
            (row["indiceH"], row["nrCitacoesAutor"]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=8,
        )
    ax.set_title("Consistencia do autor: indice H vs citacoes")
    ax.set_xlabel("Indice H")
    ax.set_ylabel("Numero de citacoes")
    ax.grid(alpha=0.25)

    output_path = output_dir / "consistencia_do_autor.png"
    save_figure(fig, output_path)
    return output_path


def plot_matriz_produtividade(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["totalProducoes"] = to_numeric(df["totalProducoes"])
    df["totalCitacoes"] = to_numeric(df["totalCitacoes"])
    df["indiceH"] = to_numeric(df["indiceH"])

    fig, ax = plt.subplots(figsize=(10, 6))
    sizes = (df["indiceH"].fillna(0) + 1) * 30
    sc = ax.scatter(df["totalProducoes"], df["totalCitacoes"], s=sizes, c=df["indiceH"], cmap="viridis", alpha=0.75, edgecolors="white", linewidths=0.5)
    top = df.nlargest(8, "totalCitacoes")
    for _, row in top.iterrows():
        ax.annotate(
            clean_text(row["nomeAutor"]),
            (row["totalProducoes"], row["totalCitacoes"]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=8,
        )
    ax.set_title("Matriz de produtividade do autor: volume vs impacto")
    ax.set_xlabel("Total de producoes")
    ax.set_ylabel("Total de citacoes")
    ax.grid(alpha=0.25)
    fig.colorbar(sc, ax=ax, label="Indice H")

    output_path = output_dir / "matriz_produtividade_autor.png"
    save_figure(fig, output_path)
    return output_path


def plot_matriz_produtividade_bins(df: pd.DataFrame, output_dir: Path) -> Path:
    """Complementary bar chart showing counts of authors by production-volume bins."""
    df = df.copy()
    df["totalProducoes"] = to_numeric(df.get("totalProducoes", pd.Series(dtype=float)))
    # Define bins and labels focused on 0-10 clarity
    bins = [-0.1, 0, 1, 2, 5, 10, 20, 999999]
    labels = ["0", "1", "2", "3-5", "6-10", "11-20", ">20"]
    # Create categorical bins
    series = pd.cut(df["totalProducoes"].fillna(0), bins=bins, labels=labels, include_lowest=True)
    counts = series.value_counts().reindex(labels).fillna(0).astype(int)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bars = ax.bar(labels, counts.values, color="#17becf")
    annotate_bars(ax, bars)
    ax.set_title("Distribuicao de autores por volume de producoes")
    ax.set_xlabel("Faixa de producoes")
    ax.set_ylabel("Numero de autores")
    ax.grid(axis="y", alpha=0.2)

    output_path = output_dir / "matriz_produtividade_bins.png"
    save_figure(fig, output_path)
    return output_path


def plot_faixa_citacao(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["quantidade"] = to_numeric(df.get("quantidade", pd.Series(dtype=float)))
    df["faixaCitacao"] = df["faixaCitacao"].astype(str).str.strip()

    def classify_range(text: str):
        s = (text or "").strip()
        # extract numbers
        nums = re.findall(r"\d+", s)
        nums = [int(n) for n in nums]
        s_lower = s.lower()
        # sem impacto: explicit 0 or contains 'sem' or ranges that indicate 0
        if "sem" in s_lower or (len(nums) == 1 and nums[0] == 0):
            return ("Sem impacto latente (<10 citações)", "Sem impacto (<10)")

        # if explicit '>=50' or single number >=50 or plus sign directly after number -> Elite
        if re.search(r">=\s*50", s) or re.search(r"\b\d+\+", s) or (len(nums) == 1 and nums[0] >= 50):
            return ("Elite científica (>=50 citações)", "Elite (>=50)")

        # if we have a range like '11 a 50' or '11-50', classify by low/high
        if len(nums) >= 2:
            low, high = nums[0], nums[1]
            if low >= 50:
                return ("Elite científica (>=50 citações)", "Elite (>=50)")
            if low >= 11 and high <= 50:
                return ("Impacto Consolidado (11-50 citações)", "Consolidado (11-50)")
            if low >= 1 and high <= 10:
                return ("Impacto Inicial (1-10 citações)", "Inicial (1-10)")

        # textual fallback
        if "inicial" in s_lower:
            return ("Impacto Inicial (1-10 citações)", "Inicial (1-10)")
        if "consolid" in s_lower:
            return ("Impacto Consolidado (11-50 citações)", "Consolidado (11-50)")
        if "+" in s or ">=" in s:
            return ("Elite científica (>=50 citações)", "Elite (>=50)")

        # last fallback: return same text
        return (s, s)

    mapped = df["faixaCitacao"].apply(classify_range)
    df["categoria_long"] = mapped.apply(lambda t: t[0])
    df["categoria_short"] = mapped.apply(lambda t: t[1])

    grouped = df.groupby(["categoria_long", "categoria_short"], as_index=False)["quantidade"].sum()
    grouped = grouped.sort_values("quantidade", ascending=False)

    labels = grouped["categoria_short"].tolist()
    sizes = grouped["quantidade"].fillna(0).astype(float).tolist()
    long_labels = grouped["categoria_long"].tolist()

    fig, ax = plt.subplots(figsize=(9, 6))
    # make room on the right for the legend (place legend outside the axes)
    fig.subplots_adjust(right=0.78)
    # draw the pie slightly left-centered so the legend can sit outside to the right
    wedges, texts, autotexts = ax.pie(
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        colors=plt.get_cmap("tab20").colors,
        center=(0.30, 0.5),
    )
    ax.axis("equal")
    ax.set_title("Percentual de autores por faixa de citacao (categoria)")
    for t in autotexts:
        t.set_fontsize(9)

    # build legend with long labels and place it outside the axes on the right
    legend_entries = [f"{lab} — {long}" for lab, long in zip(labels, long_labels)]
    ax.legend(wedges, legend_entries, title="Categorias (limites)", loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=9)

    output_path = output_dir / "faixas_de_citacao_pie.png"
    save_figure(fig, output_path)
    return output_path


def plot_nivel_por_universidade(df: pd.DataFrame, output_dir: Path) -> Path:
    df = df.copy()
    df["totalProducoes"] = to_numeric(df.get("totalProducoes", pd.Series(dtype=float)))
    pivot = df.pivot_table(index="siglaICT", columns="nivel", values="totalProducoes", aggfunc="sum", fill_value=0)
    pivot = pivot.sort_index()

    levels = list(pivot.columns)
    n = len(pivot.index)
    m = len(levels)
    x = np.arange(n)
    total_width = 0.8
    if m > 0:
        width = total_width / m
    else:
        width = total_width

    fig, ax = plt.subplots(figsize=(max(10, 0.7 * n), 6))
    palette = ["#4c78a8", "#f58518", "#54a24b", "#e45756", "#72b7b2"]
    bars_list = []
    legend_labels = []
    for i, level in enumerate(levels):
        values = pivot[level].fillna(0).astype(float).values
        positions = x - total_width / 2 + i * width + width / 2
        bars = ax.bar(positions, values, width=width * 0.95, color=palette[i % len(palette)])
        bars_list.append(bars)
        # annotate bars
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.annotate(f"{int(h)}", (bar.get_x() + bar.get_width() / 2, h), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8)
        # total per level for legend
        total_level = int(values.sum())
        legend_labels.append(f"{level} ({total_level})")

    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=45, ha="right")
    ax.set_xlabel("Universidade (sigla)")
    ax.set_ylabel("Total de producoes")
    ax.set_title("Total de producoes por universidade e nivel")
    ax.grid(axis="y", alpha=0.2)
    ax.legend(legend_labels, title="Nivel (total)")

    fig.tight_layout()

    output_path = output_dir / "producoes_por_universidade_por_nivel.png"
    save_figure(fig, output_path)
    return output_path


ChartBuilder = Tuple[str, str, Callable[..., Path]]


CHARTS: List[ChartBuilder] = [
    ("Media de Citacoes por Conceito CAPES.csv", "Media de citacoes por conceito CAPES", plot_media_citacoes_por_conceito),
    ("Evolução Temporal das Notas dos Programas.csv", "Evolucao temporal das notas dos programas", plot_evolucao_notas_programas),
    ("Produções por Indice H.csv", "Producoes por indice H", plot_indice_h),
    ("Consistência do Autor.csv", "Consistencia do autor", plot_consistencia_autor),
    ("Matriz de Produtividade do Autor (Volume vs. Impacto).csv", "Matriz de produtividade do autor", plot_matriz_produtividade),
    ("Distribuição de Produção por Faixas de Citação.csv", "Distribuicao de producao por faixas de citacao", plot_faixa_citacao),
    (
        "Número total de produções dividido entre Mestrado e Doutorado, ordernado por Universidade.csv",
        "Producoes por universidade e nivel",
        plot_nivel_por_universidade,
    ),
]


def build_report(entries: List[Tuple[str, str, Path, pd.DataFrame]], report_path: Path) -> None:
    cards: List[str] = []
    for title, description, image_path, df in entries:
        preview = df.head(6).to_html(index=False, border=0, classes="preview")
        cards.append(
            f"""
            <section class="card">
              <h2>{title}</h2>
              <p>{description}</p>
              <img src="{image_path.name}" alt="{title}">
              <details>
                <summary>Primeiras linhas</summary>
                {preview}
              </details>
            </section>
            """.strip()
        )

    report_path.write_text(
        "\n".join(
            [
                "<!doctype html>",
                '<html lang="pt-BR">',
                "<head>",
                '<meta charset="utf-8">',
                '<meta name="viewport" content="width=device-width, initial-scale=1">',
                "<title>Resultados SPARQL visualizados</title>",
                "<style>",
                "body{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f5f7fb;color:#1f2937;}",
                ".wrap{max-width:1200px;margin:0 auto;padding:24px;}",
                "h1{margin:0 0 8px;}",
                ".lead{margin:0 0 24px;color:#4b5563;}",
                ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:18px;}",
                ".card{background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:16px;box-shadow:0 8px 24px rgba(15,23,42,0.08);}",
                ".card img{width:100%;height:auto;border-radius:12px;border:1px solid #e5e7eb;margin:12px 0;}",
                "details{margin-top:8px;}",
                "table{width:100%;border-collapse:collapse;font-size:12px;overflow:auto;}",
                "th,td{border:1px solid #e5e7eb;padding:6px;text-align:left;}",
                "th{background:#f8fafc;}",
                "</style>",
                "</head>",
                "<body>",
                '<div class="wrap">',
                "<h1>Visualizacao dos resultados SPARQL</h1>",
                '<p class="lead">Grafico gerado automaticamente a partir dos CSVs em build/results/csv.</p>',
                '<div class="grid">',
                *cards,
                "</div>",
                "</div>",
                "</body>",
                "</html>",
            ]
        ),
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera graficos e um relatorio HTML a partir dos CSVs de SPARQL.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Diretorio com os CSVs de saida das consultas.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Diretorio onde os graficos e o HTML serao salvos.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=15,
        help="Numero maximo de programas para o grafico temporal.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Exibe os graficos na tela depois de gerar os arquivos.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    plt.style.use("seaborn-v0_8-whitegrid")

    parser = build_parser()
    args = parser.parse_args(argv)

    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir

    if not input_dir.exists():
        parser.error(f"Diretorio de entrada inexistente: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    generated: List[Tuple[str, str, Path, pd.DataFrame]] = []
    for filename, description, builder in CHARTS:
        source_path = input_dir / filename
        if not source_path.exists():
            logger.warning("Pulando arquivo ausente: %s", source_path.name)
            continue
        logger.info("Lendo %s", source_path.name)
        df = read_csv_flexible(source_path)
        # Handle special cases that produce multiple complementary charts
        if source_path.name == "Matriz de Produtividade do Autor (Volume vs. Impacto).csv":
            p1 = plot_matriz_produtividade(df, output_dir)
            generated.append((source_path.stem + " (scatter)", description, p1, df))
            logger.info("  ✓ %s", p1.name)
            p2 = plot_matriz_produtividade_bins(df, output_dir)
            generated.append((source_path.stem + " (bins)", description + " (bins)", p2, df))
            logger.info("  ✓ %s", p2.name)
            continue
        if builder is plot_evolucao_notas_programas:
            image_path = builder(df, output_dir, args.top_n)
        else:
            image_path = builder(df, output_dir)
        generated.append((source_path.stem, description, image_path, df))
        logger.info("  ✓ %s", image_path.name)

    if not generated:
        parser.error("Nenhum CSV reconhecido foi encontrado para gerar graficos.")

    report_path = output_dir / "index.html"
    build_report(generated, report_path)
    logger.info("Relatorio salvo em %s", report_path)

    if args.show:
        plt.show()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())