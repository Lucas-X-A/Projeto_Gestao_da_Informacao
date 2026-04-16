import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import (
    COLS_AUTORES,
    COLS_DISCENTES,
    COLS_PRODUCAO,
    COLS_PROGRAMAS,
    CSV_CHUNKSIZE,
    DATA_AUTORES_DIR,
    DATA_DISCENTES_DIR,
    DATA_PRODUCAO_DIR,
    DATA_PROGRAMAS_DIR,
    SITUACAO_FILTER,
    STATE_FILTER,
)
from .utils import strip_str_cols, to_int64

logger = logging.getLogger(__name__)


def read_capes_csvs(
    directory: Path,
    dtype_overrides: dict = None,
    uf_col: str = None,
    uf_value: str = None,
    use_chunks: bool = False,
) -> Optional[pd.DataFrame]:
    if not directory.exists():
        logger.warning(f"  Diretório não encontrado: {directory}")
        return None

    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        logger.warning(f"  Nenhum CSV em: {directory}")
        return None

    base_dtype: dict = {
        "AN_BASE": str,
        "ID_PESSOA": str,
        "ID_PESSOA_DISCENTE": str,
        "ID_ADD_PRODUCAO_INTELECTUAL": str,
    }
    if dtype_overrides:
        base_dtype.update(dtype_overrides)

    dfs = []
    for csv_file in csv_files:
        logger.info(f"    Lendo: {csv_file.name}")
        try:
            if use_chunks and uf_col and uf_value:
                chunks = []
                reader = pd.read_csv(
                    csv_file,
                    delimiter=";",
                    encoding="iso-8859-1",
                    dtype=base_dtype,
                    low_memory=False,
                    chunksize=CSV_CHUNKSIZE,
                )
                for chunk in reader:
                    if uf_col in chunk.columns:
                        chunk = chunk[chunk[uf_col].str.strip() == uf_value]
                    if not chunk.empty:
                        chunks.append(chunk)
                if not chunks:
                    logger.info(f"      ✓ 0 linhas (nenhum registro de {uf_value})")
                    continue
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"      ✓ {len(df):,} linhas (filtro {uf_value})")
            else:
                df = pd.read_csv(
                    csv_file,
                    delimiter=";",
                    encoding="iso-8859-1",
                    dtype=base_dtype,
                    low_memory=False,
                )
                logger.info(f"      ✓ {len(df):,} linhas")
            dfs.append(df)
        except Exception as exc:
            logger.error(f"      ✗ Erro ao ler {csv_file.name}: {exc}")

    if not dfs:
        return None

    result = pd.concat(dfs, ignore_index=True)
    logger.info(f"    Total consolidado: {len(result):,} linhas")
    return result


class DataLoader:
    def __init__(self, state: str = STATE_FILTER):
        self.state = state
        self.df_programas: Optional[pd.DataFrame] = None
        self.df_discentes: Optional[pd.DataFrame] = None
        self.df_autores: Optional[pd.DataFrame] = None
        self.df_producao: Optional[pd.DataFrame] = None

    def load_all(self):
        logger.info("\n[LOAD] Carregando datasets CAPES...")

        logger.info("\n  → Programas")
        df = read_capes_csvs(DATA_PROGRAMAS_DIR)
        if df is not None:
            df = strip_str_cols(df)
            df = to_int64(df, [COLS_PROGRAMAS["ano"]])
            uf_col = COLS_PROGRAMAS["uf"]
            if uf_col in df.columns:
                before = len(df)
                df = df[df[uf_col] == self.state].copy()
                logger.info(f"    Filtro UF={self.state}: {before:,} → {len(df):,}")
        self.df_programas = df

        logger.info(
            f"\n  → Discentes (chunks de {CSV_CHUNKSIZE:,} linhas "
            f"com filtro UF={self.state} na leitura)"
        )
        df = read_capes_csvs(
            DATA_DISCENTES_DIR,
            uf_col=COLS_DISCENTES["uf"],
            uf_value=self.state,
            use_chunks=True,
        )
        if df is not None:
            df = strip_str_cols(df)
            df = to_int64(
                df,
                [
                    COLS_DISCENTES["ano"],
                    COLS_DISCENTES["nascimento"],
                    COLS_DISCENTES["mes_tit"],
                ],
            )
            if SITUACAO_FILTER:
                col_sit = COLS_DISCENTES["situacao"]
                if col_sit in df.columns:
                    before = len(df)
                    df = df[df[col_sit].isin(SITUACAO_FILTER)].copy()
                    logger.info(
                        f"    Filtro situação {SITUACAO_FILTER}: "
                        f"{before:,} → {len(df):,}"
                    )
        self.df_discentes = df

        logger.info("\n  → Autores da Produção Intelectual")
        df = read_capes_csvs(DATA_AUTORES_DIR)
        if df is not None:
            df = strip_str_cols(df)
            df = to_int64(df, [COLS_AUTORES["ano"]])
        self.df_autores = df

        logger.info("\n  → Produção Intelectual")
        df = read_capes_csvs(
            DATA_PRODUCAO_DIR,
            dtype_overrides={"AN_BASE_PRODUCAO": str},
        )
        if df is not None:
            df = strip_str_cols(df)
            df = to_int64(df, [COLS_PRODUCAO["ano"], COLS_PRODUCAO["base_ano"]])
        self.df_producao = df
