#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  SCRIPT: Gerador de OML — CT&I Pernambuco (Pipeline Unificado)
  GIC-UFRPE 2025

  FONTES DE DADOS (subpastas de data/raw/capes/):
    programas/  → ICT, PPG, Conceito_PPG
    discentes/  → Discente, Docente (orientadores)
    autores/    → ligação ID_PESSOA_DISCENTE → ID_ADD_PRODUCAO_INTELECTUAL
    producao/   → Producao_Cientifica, Veiculo_Publicacao

  CRUZAMENTO DE CHAVES:
    Discente.ID_PESSOA
        ↕  autores.ID_PESSOA_DISCENTE == Discente.ID_PESSOA
    AutorProducao.ID_ADD_PRODUCAO_INTELECTUAL
        ↕  producao.ID_ADD_PRODUCAO_INTELECTUAL
    ProducaoIntelectual → título, DOI, veículo
        ↕  Scopus API
    Citacao → nr_indice_h, nr_indice_i10, nr_citacoes_autor

  USO:
    python generate_oml_cti_full.py                   # todas as etapas
    python generate_oml_cti_full.py --steps capes     # só CAPES (1-3)
    python generate_oml_cti_full.py --steps scopus    # só enriquecimento (4)
    python generate_oml_cti_full.py --steps oml       # só gerar OML (5-6)
    python generate_oml_cti_full.py --steps capes,oml # CAPES + OML sem Scopus

  .env (na pasta scripts/ ou raiz do projeto):
    SCOPUS_API_KEY=sua_chave_aqui
    STATE_FILTER=PE
    SITUACAO_FILTER=TITULADO

  ESTADO INTERMEDIÁRIO:
    data/processed/pipeline_state.pkl  (salvo após extração CAPES)
    Permite retomar apenas a etapa Scopus ou OML sem re-processar tudo.
================================================================================
"""

# ── Imports ──────────────────────────────────────────────────────────────────
import os
import sys
import time
import hashlib
import logging
import argparse
import pickle
import re
import requests
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict

# ── python-dotenv (opcional, mas recomendado) ─────────────────────────────────
try:
    from dotenv import load_dotenv
    _script_dir   = Path(__file__).parent
    _project_root = _script_dir.parent
    _env_found = False
    for _env_path in [_script_dir / ".env", _project_root / ".env"]:
        if _env_path.exists():
            load_dotenv(_env_path)
            _env_found = True
            break
    if not _env_found:
        load_dotenv()
except ImportError:
    pass

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT       = Path(__file__).parent.parent
DATA_PROGRAMAS_DIR = PROJECT_ROOT / "data" / "raw" / "capes" / "programas"
DATA_DISCENTES_DIR = PROJECT_ROOT / "data" / "raw" / "capes" / "discentes"
DATA_AUTORES_DIR   = PROJECT_ROOT / "data" / "raw" / "capes" / "autores"
DATA_PRODUCAO_DIR  = PROJECT_ROOT / "data" / "raw" / "capes" / "producao"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OML_OUTPUT_DIR     = (
    PROJECT_ROOT / "src" / "oml" / "gic.ufrpe.br" / "cti" / "description"
)
STATE_PICKLE_PATH  = DATA_PROCESSED_DIR / "pipeline_state.pkl"

VOCABULARY_URI         = "http://gic.ufrpe.br/cti/vocabulary/cti"
VOCABULARY_NAMESPACE   = "cti"
CTI_PE_DESCRIPTION_URI = "http://gic.ufrpe.br/cti/description/cti-pe"
DC_URI                 = "http://purl.org/dc/elements/1.1/"
DC_NAMESPACE           = "dc"

SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY", "")
STATE_FILTER   = os.getenv("STATE_FILTER", "PE")

_sit_raw = os.getenv("SITUACAO_FILTER", "TITULADO")
SITUACAO_FILTER: Optional[List[str]] = (
    [s.strip() for s in _sit_raw.split(",") if s.strip()]
    if _sit_raw.strip() else None
)

CSV_CHUNKSIZE = 200_000
SCOPUS_DELAY  = 0.15


# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO DE COLUNAS CAPES
# ─────────────────────────────────────────────────────────────────────────────

COLS_PROGRAMAS = {
    "uf":         "SG_UF_PROGRAMA",
    "cd_ent":     "CD_ENTIDADE_CAPES",
    "sg_ent":     "SG_ENTIDADE_ENSINO",
    "nm_ent":     "NM_ENTIDADE_ENSINO",
    "cd_prog":    "CD_PROGRAMA_IES",
    "nm_prog":    "NM_PROGRAMA_IES",
    "modalidade": "NM_MODALIDADE_PROGRAMA",
    "area":       "NM_AREA_CONHECIMENTO",
    "conceito":   "CD_CONCEITO_PROGRAMA",
    "ano":        "AN_BASE",
}

COLS_DISCENTES = {
    "uf":          "SG_UF_PROGRAMA",
    "cd_ent":      "CD_ENTIDADE_CAPES",
    "sg_ent":      "SG_ENTIDADE_ENSINO",
    "nm_ent":      "NM_ENTIDADE_ENSINO",
    "cd_prog":     "CD_PROGRAMA_IES",
    "nm_prog":     "NM_PROGRAMA_IES",
    "modalidade":  "NM_MODALIDADE_PROGRAMA",
    "grande_area": "NM_GRANDE_AREA_CONHECIMENTO",
    "id_pessoa":   "ID_PESSOA",
    "nm_disc":     "NM_DISCENTE",
    "nascimento":  "AN_NASCIMENTO_DISCENTE",
    "pais":        "NM_PAIS_NACIONALIDADE_DISCENTE",
    "grau":        "DS_GRAU_ACADEMICO_DISCENTE",
    "situacao":    "NM_SITUACAO_DISCENTE",
    "mes_tit":     "QT_MES_TITULACAO",
    "orientador":  "NM_ORIENTADOR_PRINCIPAL",
    "ano":         "AN_BASE",
}

COLS_AUTORES = {
    # CORRIGIDO: a coluna de ID do discente no CSV de autores é ID_PESSOA_DISCENTE
    "id_add":       "ID_ADD_PRODUCAO_INTELECTUAL",
    "id_pessoa":    "ID_PESSOA_DISCENTE",
    "nm_autor":     "NM_AUTOR",
    "tipo_vinculo": "DS_TIPO_VINCULO_PRODUCAO",
    "cd_prog":      "CD_PROGRAMA_IES",
    "ano":          "AN_BASE",
}

COLS_PRODUCAO = {
    "id_add":   "ID_ADD_PRODUCAO_INTELECTUAL",
    "titulo":   "NM_PRODUCAO",
    "natureza": "DS_NATUREZA",
    "ano":      "AN_BASE_PRODUCAO",
    "url":      "DS_URL_ACESSO_PRODUCAO",
    "palavras": "DS_PALAVRA_CHAVE",
    "doi":      "DS_DOI",
    "veiculo":  "NM_PERIODICO",
    "issn":     "DS_ISSN",
    "cd_prog":  "CD_PROGRAMA_IES",
    "base_ano": "AN_BASE",
}


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ICTInstance:
    id: str
    cd_entidade_capes: str
    sg_entidade_ensino: str
    nm_entidade_ensino: str
    sg_uf: str

@dataclass
class PPGInstance:
    id: str
    cd_programa_ies: str
    nm_programa_ies: str
    nm_modalidade_programa: str
    nm_area_conhecimento: str
    ict_id: str

@dataclass
class ConceituPPGInstance:
    id: str
    cd_programa_ies: str
    cd_conceito_programa: str
    an_base_conceito: int           # xsd:integer

@dataclass
class DocenteInstance:
    id: str
    nm_pessoa: str

@dataclass
class DiscenteInstance:
    id: str
    id_pessoa: int                  # xsd:integer
    nm_pessoa: str
    an_nascimento: int              # xsd:integer  (0 = não informado)
    nm_pais_nacionalidade: str
    ds_grau_academico_discente: str
    nm_situacao_discente: str
    qt_mes_titulacao: int           # xsd:int  (0 = não informado)
    vinculado_id: str
    orientador_id: str

@dataclass
class AutorInstance:
    id: str
    id_pessoa: int                  # xsd:integer
    nm_pessoa: str
    ds_orc_id: str = ""
    ds_scopus_id: str = ""
    ds_url_google_scholar: str = ""

@dataclass
class ProducaoCientificaInstance:
    id: str
    id_add: str
    nm_titulo: str
    ds_natureza: str
    an_base_producao: int           # xsd:integer
    ds_url_acesso: str
    ds_palavras_chave: str
    ds_doi: str
    nr_citacoes_publicacao: str     # xsd:string (conforme ontologia)
    veiculo_id: str
    autor_ids: List[str] = field(default_factory=list)

@dataclass
class VeiculoPublicacaoInstance:
    id: str
    nm_veiculo_publicacao: str
    ds_isbn_issn: str
    nr_citescore_scopus: int        # xsd:int
    nr_quartil_scopus: int          # xsd:int

@dataclass
class CitacaoInstance:
    id: str
    autor_id: str
    an_base_citacao: int            # xsd:integer
    nr_citacoes_autor: int          # xsd:integer
    nr_indice_h: int                # xsd:int
    nr_indice_i10: int              # xsd:int


# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────

def stable_hash(text: str, length: int = 12) -> str:
    """Hash MD5 determinístico — resolve o bug do hash() com PYTHONHASHSEED."""
    return hashlib.md5(text.strip().upper().encode("utf-8")).hexdigest()[:length]


def safe_int(value, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_str(value, default: str = "") -> str:
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def escape_oml(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def oml_safe_id(raw: str) -> str:
    """Garante que um ID OML seja válido (letras, dígitos e _)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", str(raw).strip())


def _strip_str_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip whitespace em colunas string.
    Compatível com pandas 2 (object) e pandas 3 (string).
    """
    try:
        str_cols = df.select_dtypes(include=["object", "string"]).columns
    except Exception:
        str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        try:
            df[col] = df[col].str.strip()
        except AttributeError:
            pass
    return df


def _to_int64(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# LEITURA DE CSV
# ─────────────────────────────────────────────────────────────────────────────

def read_capes_csvs(
    directory: Path,
    dtype_overrides: dict = None,
    uf_col: str = None,
    uf_value: str = None,
    use_chunks: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Lê e concatena todos os CSVs de um diretório no formato CAPES.
    Se use_chunks=True com uf_col/uf_value, filtra por UF durante a leitura
    (economiza memória para arquivos grandes como discentes ~8.5M linhas).
    """
    if not directory.exists():
        logger.warning(f"  Diretório não encontrado: {directory}")
        return None
    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        logger.warning(f"  Nenhum CSV em: {directory}")
        return None

    base_dtype: dict = {
        "AN_BASE":                      str,
        "ID_PESSOA":                    str,
        "ID_PESSOA_DISCENTE":           str,
        "ID_ADD_PRODUCAO_INTELECTUAL":  str,
    }
    if dtype_overrides:
        base_dtype.update(dtype_overrides)

    dfs = []
    for f in csv_files:
        logger.info(f"    Lendo: {f.name}")
        try:
            if use_chunks and uf_col and uf_value:
                chunks = []
                reader = pd.read_csv(
                    f, delimiter=";", encoding="iso-8859-1",
                    dtype=base_dtype, low_memory=False,
                    chunksize=CSV_CHUNKSIZE,
                )
                for chunk in reader:
                    if uf_col in chunk.columns:
                        chunk = chunk[
                            chunk[uf_col].str.strip() == uf_value
                        ]
                    if not chunk.empty:
                        chunks.append(chunk)
                if chunks:
                    df = pd.concat(chunks, ignore_index=True)
                    logger.info(f"      ✓ {len(df):,} linhas (filtro {uf_value})")
                else:
                    logger.info(f"      ✓ 0 linhas (nenhum registro de {uf_value})")
                    continue
            else:
                df = pd.read_csv(
                    f, delimiter=";", encoding="iso-8859-1",
                    dtype=base_dtype, low_memory=False,
                )
                logger.info(f"      ✓ {len(df):,} linhas")
            dfs.append(df)
        except Exception as e:
            logger.error(f"      ✗ Erro ao ler {f.name}: {e}")

    if not dfs:
        return None
    result = pd.concat(dfs, ignore_index=True)
    logger.info(f"    Total consolidado: {len(result):,} linhas")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# CLASSE 1: DataLoader
# ─────────────────────────────────────────────────────────────────────────────

class DataLoader:
    def __init__(self, state: str = STATE_FILTER):
        self.state = state
        self.df_programas: Optional[pd.DataFrame] = None
        self.df_discentes: Optional[pd.DataFrame] = None
        self.df_autores:   Optional[pd.DataFrame] = None
        self.df_producao:  Optional[pd.DataFrame] = None

    def load_all(self):
        logger.info("\n[LOAD] Carregando datasets CAPES...")

        # ── Programas ────────────────────────────────────────────────────
        logger.info(f"\n  → Programas")
        df = read_capes_csvs(DATA_PROGRAMAS_DIR)
        if df is not None:
            df = _strip_str_cols(df)
            df = _to_int64(df, [COLS_PROGRAMAS["ano"]])
            uf_col = COLS_PROGRAMAS["uf"]
            if uf_col in df.columns:
                before = len(df)
                df = df[df[uf_col] == self.state].copy()
                logger.info(f"    Filtro UF={self.state}: {before:,} → {len(df):,}")
        self.df_programas = df

        # ── Discentes (CSV grande → leitura em chunks com filtro embutido) ──
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
            df = _strip_str_cols(df)
            df = _to_int64(df, [
                COLS_DISCENTES["ano"],
                COLS_DISCENTES["nascimento"],
                COLS_DISCENTES["mes_tit"],
            ])
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

        # ── Autores ──────────────────────────────────────────────────────
        logger.info(f"\n  → Autores da Produção Intelectual")
        df = read_capes_csvs(DATA_AUTORES_DIR)
        if df is not None:
            df = _strip_str_cols(df)
            df = _to_int64(df, [COLS_AUTORES["ano"]])
        self.df_autores = df

        # ── Produção ─────────────────────────────────────────────────────
        logger.info(f"\n  → Produção Intelectual")
        df = read_capes_csvs(
            DATA_PRODUCAO_DIR,
            dtype_overrides={"AN_BASE_PRODUCAO": str},
        )
        if df is not None:
            df = _strip_str_cols(df)
            df = _to_int64(df, [COLS_PRODUCAO["ano"], COLS_PRODUCAO["base_ano"]])
        self.df_producao = df


# ─────────────────────────────────────────────────────────────────────────────
# CLASSE 2: InstanceExtractor
# ─────────────────────────────────────────────────────────────────────────────

class InstanceExtractor:
    def __init__(self, loader: DataLoader):
        self.loader = loader
        self.ict_instances:      Dict[str, ICTInstance]                = {}
        self.ppg_instances:      Dict[str, PPGInstance]                = {}
        self.conceito_instances: List[ConceituPPGInstance]             = []
        self.docente_instances:  Dict[str, DocenteInstance]            = {}
        self.discente_instances: Dict[str, DiscenteInstance]           = {}
        self.autor_instances:    Dict[str, AutorInstance]              = {}
        self.producao_instances: Dict[str, ProducaoCientificaInstance] = {}
        self.veiculo_instances:  Dict[str, VeiculoPublicacaoInstance]  = {}
        self.citacao_instances:  List[CitacaoInstance]                 = []
        self._pessoa_to_autor:   Dict[str, str]                        = {}

    # ── A: Programas ──────────────────────────────────────────────────────

    def extract_from_programas(self):
        df = self.loader.df_programas
        if df is None:
            logger.warning("  Dataset de Programas não disponível — pulando.")
            return
        c = COLS_PROGRAMAS
        logger.info("\n[EXTRACT-A] Programas → ICT / PPG / Conceito_PPG")

        for _, row in df.drop_duplicates(subset=[c["cd_ent"]]).iterrows():
            raw_id = safe_str(row[c["cd_ent"]])
            ict_id = f"ict_{oml_safe_id(raw_id)}"
            self.ict_instances[ict_id] = ICTInstance(
                id=ict_id,
                cd_entidade_capes=raw_id,
                sg_entidade_ensino=safe_str(row[c["sg_ent"]]),
                nm_entidade_ensino=safe_str(row[c["nm_ent"]]),
                sg_uf=safe_str(row[c["uf"]]),
            )

        for _, row in df.drop_duplicates(subset=[c["cd_prog"]]).iterrows():
            ppg_id = f"ppg_{oml_safe_id(safe_str(row[c['cd_prog']]))}"
            ict_id = f"ict_{oml_safe_id(safe_str(row[c['cd_ent']]))}"
            self.ppg_instances[ppg_id] = PPGInstance(
                id=ppg_id,
                cd_programa_ies=safe_str(row[c["cd_prog"]]),
                nm_programa_ies=safe_str(row[c["nm_prog"]]),
                nm_modalidade_programa=safe_str(row[c["modalidade"]]),
                nm_area_conhecimento=safe_str(row[c["area"]]),
                ict_id=ict_id,
            )

        seen: Set[tuple] = set()
        for _, row in df.iterrows():
            cd_prog  = safe_str(row[c["cd_prog"]])
            an_base  = safe_int(row[c["ano"]])
            raw_conc = row.get(c["conceito"])
            try:
                if pd.isna(raw_conc):
                    continue
            except (TypeError, ValueError):
                pass
            key = (cd_prog, an_base)
            if key in seen:
                continue
            seen.add(key)
            self.conceito_instances.append(ConceituPPGInstance(
                id=f"conceito_{oml_safe_id(cd_prog)}_{an_base}",
                cd_programa_ies=cd_prog,
                cd_conceito_programa=safe_str(raw_conc),
                an_base_conceito=an_base,
            ))

        logger.info(f"  ICTs:      {len(self.ict_instances):,}")
        logger.info(f"  PPGs:      {len(self.ppg_instances):,}")
        logger.info(f"  Conceitos: {len(self.conceito_instances):,}")

    # ── B: Discentes ──────────────────────────────────────────────────────

    def extract_from_discentes(self):
        df = self.loader.df_discentes
        if df is None:
            logger.warning("  Dataset de Discentes não disponível — pulando.")
            return
        c = COLS_DISCENTES
        logger.info("\n[EXTRACT-B] Discentes → Discente / Docente")

        # Docentes (orientadores) — hash MD5 estável do nome
        col_orient = c["orientador"]
        if col_orient in df.columns:
            for nome in df[col_orient].dropna().unique():
                nome_str = str(nome).strip()
                if not nome_str:
                    continue
                doc_id = f"docente_{stable_hash(nome_str)}"
                self.docente_instances[doc_id] = DocenteInstance(
                    id=doc_id, nm_pessoa=nome_str
                )

        col_id  = c["id_pessoa"]
        col_ano = c["ano"]
        if col_id not in df.columns:
            logger.error(
                f"  Coluna '{col_id}' não encontrada no CSV de discentes.\n"
                f"  Colunas disponíveis: {', '.join(df.columns.tolist()[:20])}"
            )
            return

        # Manter registro mais recente por ID_PESSOA
        df_sorted = df.sort_values(col_ano, ascending=False, na_position="last")
        df_unique = df_sorted.drop_duplicates(subset=[col_id], keep="first")

        for _, row in df_unique.iterrows():
            id_str = safe_str(row[col_id])
            if not id_str:
                continue
            disc_id = f"discente_{id_str}"
            ppg_id  = f"ppg_{oml_safe_id(safe_str(row[c['cd_prog']]))}"

            orient_id = ""
            if col_orient in row.index:
                nome_orient = row[col_orient]
                try:
                    is_na = pd.isna(nome_orient)
                except (TypeError, ValueError):
                    is_na = False
                if not is_na:
                    nome_orient_str = str(nome_orient).strip()
                    if nome_orient_str:
                        orient_id = f"docente_{stable_hash(nome_orient_str)}"

            self.discente_instances[disc_id] = DiscenteInstance(
                id=disc_id,
                id_pessoa=safe_int(id_str),
                nm_pessoa=safe_str(row[c["nm_disc"]]),
                an_nascimento=safe_int(row.get(c["nascimento"]), 0),
                nm_pais_nacionalidade=safe_str(row.get(c["pais"]), "Brasil"),
                ds_grau_academico_discente=safe_str(row[c["grau"]]),
                nm_situacao_discente=safe_str(row[c["situacao"]]),
                qt_mes_titulacao=safe_int(row.get(c["mes_tit"]), 0),
                vinculado_id=ppg_id,
                orientador_id=orient_id,
            )

            # Garantir ICT e PPG mesmo sem dataset de Programas
            ict_id = f"ict_{oml_safe_id(safe_str(row[c['cd_ent']]))}"
            if ict_id not in self.ict_instances:
                self.ict_instances[ict_id] = ICTInstance(
                    id=ict_id,
                    cd_entidade_capes=safe_str(row[c["cd_ent"]]),
                    sg_entidade_ensino=safe_str(row[c["sg_ent"]]),
                    nm_entidade_ensino=safe_str(row[c["nm_ent"]]),
                    sg_uf=STATE_FILTER,
                )
            if ppg_id not in self.ppg_instances:
                self.ppg_instances[ppg_id] = PPGInstance(
                    id=ppg_id,
                    cd_programa_ies=safe_str(row[c["cd_prog"]]),
                    nm_programa_ies=safe_str(row[c["nm_prog"]]),
                    nm_modalidade_programa=safe_str(row[c["modalidade"]]),
                    nm_area_conhecimento=safe_str(row[c["grande_area"]]),
                    ict_id=ict_id,
                )

        logger.info(f"  Docentes:  {len(self.docente_instances):,}")
        logger.info(f"  Discentes: {len(self.discente_instances):,}")

    # ── C: Autores × Produção ─────────────────────────────────────────────

    def extract_from_producao(self):
        df_aut = self.loader.df_autores
        df_pro = self.loader.df_producao
        ca, cp = COLS_AUTORES, COLS_PRODUCAO

        if df_aut is None:
            logger.warning(
                "\n[EXTRACT-C] Dataset de Autores não disponível — pulando."
            )
            return
        if df_pro is None:
            logger.warning(
                "\n[EXTRACT-C] Dataset de Produção não disponível — pulando."
            )
            return

        logger.info("\n[EXTRACT-C] Autores × Produção → Autor / Producao / Veiculo")

        col_id_aut = ca["id_pessoa"]   # "ID_PESSOA_DISCENTE"
        if col_id_aut not in df_aut.columns:
            available = ", ".join(df_aut.columns.tolist()[:25])
            logger.error(
                f"  Coluna '{col_id_aut}' não encontrada no CSV de autores.\n"
                f"  Colunas disponíveis (primeiras 25): {available}\n"
                f"  Ajuste COLS_AUTORES['id_pessoa'] no topo do script."
            )
            return

        ids_disc_pe: Set[str] = {
            str(d.id_pessoa) for d in self.discente_instances.values()
        }
        if not ids_disc_pe:
            logger.warning(
                "  Nenhum discente PE encontrado — abortando extração de Produção."
            )
            return

        logger.info(f"  IDs de discentes PE: {len(ids_disc_pe):,}")

        df_aut_pe = df_aut[
            df_aut[col_id_aut].astype(str).str.strip().isin(ids_disc_pe)
        ].copy()
        logger.info(
            f"  Linhas autores PE: {len(df_aut_pe):,} (de {len(df_aut):,} total)"
        )
        if df_aut_pe.empty:
            logger.warning(
                "  Nenhum autor encontrado para os discentes PE.\n"
                "  Verifique se ID_PESSOA_DISCENTE nos autores "
                "corresponde ao ID_PESSOA nos discentes."
            )
            return

        col_id_add = ca["id_add"]
        ids_add_pe: Set[str] = set(df_aut_pe[col_id_add].astype(str).unique())
        logger.info(f"  IDs de produção PE: {len(ids_add_pe):,}")

        col_id_add_pro = cp["id_add"]
        if col_id_add_pro not in df_pro.columns:
            available = ", ".join(df_pro.columns.tolist()[:25])
            logger.error(
                f"  Coluna '{col_id_add_pro}' não encontrada no CSV de produção.\n"
                f"  Colunas disponíveis (primeiras 25): {available}"
            )
            return

        df_pro_pe = df_pro[
            df_pro[col_id_add_pro].astype(str).isin(ids_add_pe)
        ].copy()
        logger.info(f"  Produções vinculadas: {len(df_pro_pe):,}")

        # ── Veículos ──
        vei_key_cols = [
            col for col in [cp["issn"], cp["veiculo"]] if col in df_pro_pe.columns
        ]
        if vei_key_cols:
            for _, row in df_pro_pe.drop_duplicates(
                subset=vei_key_cols, keep="first"
            ).iterrows():
                issn   = safe_str(row.get(cp["issn"]))
                nm_vei = safe_str(row.get(cp["veiculo"]))
                chave  = issn or nm_vei
                if not chave:
                    continue
                veiculo_id = f"veiculo_{stable_hash(chave)}"
                if veiculo_id not in self.veiculo_instances:
                    self.veiculo_instances[veiculo_id] = VeiculoPublicacaoInstance(
                        id=veiculo_id,
                        nm_veiculo_publicacao=nm_vei,
                        ds_isbn_issn=issn,
                        nr_citescore_scopus=0,
                        nr_quartil_scopus=0,
                    )

        # ── Produções ──
        for _, row in df_pro_pe.iterrows():
            id_add_str = safe_str(row[col_id_add_pro])
            prod_id    = f"prod_{oml_safe_id(id_add_str)}"
            if prod_id in self.producao_instances:
                continue
            issn      = safe_str(row.get(cp["issn"]))
            nm_vei    = safe_str(row.get(cp["veiculo"]))
            chave     = issn or nm_vei
            veiculo_id = f"veiculo_{stable_hash(chave)}" if chave else ""

            self.producao_instances[prod_id] = ProducaoCientificaInstance(
                id=prod_id,
                id_add=id_add_str,
                nm_titulo=safe_str(row.get(cp["titulo"])),
                ds_natureza=safe_str(row.get(cp["natureza"])),
                an_base_producao=safe_int(row.get(cp["ano"]), 0),
                ds_url_acesso=safe_str(row.get(cp["url"])),
                ds_palavras_chave=safe_str(row.get(cp["palavras"])),
                ds_doi=safe_str(row.get(cp["doi"])),
                nr_citacoes_publicacao="0",
                veiculo_id=veiculo_id,
            )

        # ── Autores + relação autoria ──
        for _, row in df_aut_pe.iterrows():
            id_pes_str = safe_str(row[col_id_aut])
            id_add_str = safe_str(row[col_id_add])
            autor_id   = f"autor_{oml_safe_id(id_pes_str)}"

            if autor_id not in self.autor_instances:
                nm_autor = safe_str(row.get(ca["nm_autor"]))
                disc = self.discente_instances.get(f"discente_{id_pes_str}")
                if disc and not nm_autor:
                    nm_autor = disc.nm_pessoa
                self.autor_instances[autor_id] = AutorInstance(
                    id=autor_id,
                    id_pessoa=safe_int(id_pes_str),
                    nm_pessoa=nm_autor,
                )
                self._pessoa_to_autor[id_pes_str] = autor_id

            prod_id = f"prod_{oml_safe_id(id_add_str)}"
            if prod_id in self.producao_instances:
                prod = self.producao_instances[prod_id]
                if autor_id not in prod.autor_ids:
                    prod.autor_ids.append(autor_id)

        logger.info(f"  Autores:   {len(self.autor_instances):,}")
        logger.info(f"  Produções: {len(self.producao_instances):,}")
        logger.info(f"  Veículos:  {len(self.veiculo_instances):,}")

    def get_summary(self) -> Dict:
        return {
            "ICT":                 len(self.ict_instances),
            "PPG":                 len(self.ppg_instances),
            "Conceito_PPG":        len(self.conceito_instances),
            "Docente":             len(self.docente_instances),
            "Discente":            len(self.discente_instances),
            "Autor":               len(self.autor_instances),
            "Producao_Cientifica": len(self.producao_instances),
            "Veiculo_Publicacao":  len(self.veiculo_instances),
            "Citacao":             len(self.citacao_instances),
        }


# ─────────────────────────────────────────────────────────────────────────────
# CLASSE 3: ScopusEnricher
# ─────────────────────────────────────────────────────────────────────────────

class ScopusEnricher:
    """
    Enriquece instâncias com dados da API Scopus (Elsevier).

    APIs usadas:
      • Abstract Retrieval API  → nr_citacoes_publicacao (por DOI)
      • Author Search API       → ds_scopus_id (por nome)
      • Author Retrieval API    → nr_indice_h, nr_citacoes_autor
      • Scopus Search API       → i10-index aproximado (AU-ID + REFCOUNT(10))

    SOBRE GOOGLE SCHOLAR:
      Não há API oficial do Google Scholar. A biblioteca `scholarly` (PyPI)
      é um scraper que o Google bloqueia com captchas regularmente — inviável
      para volumes grandes de autores. Para h-index e citações use SEMPRE
      a Scopus API (oficial, estável, gratuita para uso institucional).
      O campo ds_url_google_scholar na ontologia serve apenas para armazenar
      o link do perfil GS do autor (inserção manual ou via Lattes).

    REQUISITO DE REDE:
      Conecte-se à rede da UFRPE (cabo, Wi-Fi institucional ou VPN)
      para que o IP institucional seja reconhecido pela Elsevier.
    """

    BASE = "https://api.elsevier.com/content"
    HDR  = {"Accept": "application/json"}

    def __init__(self, api_key: str = SCOPUS_API_KEY):
        self.api_key  = api_key
        self._enabled = bool(api_key)
        if not self._enabled:
            logger.warning(
                "[SCOPUS] API Key não configurada. Enriquecimento desabilitado.\n"
                "  Configure no .env: SCOPUS_API_KEY=sua_chave_aqui"
            )

    def _get(self, url: str, params: dict) -> Optional[dict]:
        if not self._enabled:
            return None
        params["apiKey"] = self.api_key
        try:
            time.sleep(SCOPUS_DELAY)
            r = requests.get(url, headers=self.HDR, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 401:
                logger.error("[SCOPUS] 401 — verifique a API Key.")
                self._enabled = False
            elif r.status_code == 403:
                logger.error(
                    "[SCOPUS] 403 — acesso negado.\n"
                    "  Certifique-se de estar na rede da UFRPE (IP institucional)."
                )
            elif r.status_code == 429:
                logger.warning("[SCOPUS] 429 rate-limit — aguardando 60s...")
                time.sleep(60)
            else:
                logger.debug(f"[SCOPUS] HTTP {r.status_code}: {url}")
        except requests.RequestException as e:
            logger.error(f"[SCOPUS] Erro de rede: {e}")
        return None

    def enrich_producao_by_doi(
        self,
        producao_instances: Dict[str, ProducaoCientificaInstance],
    ) -> int:
        if not self._enabled:
            return 0
        with_doi = [p for p in producao_instances.values() if p.ds_doi]
        logger.info(f"\n[SCOPUS] Citações por DOI: {len(with_doi)} produções...")
        ok = 0
        for prod in with_doi:
            doi = prod.ds_doi.strip().lstrip("https://doi.org/")
            data = self._get(
                f"{self.BASE}/abstract/doi/{doi}",
                {"field": "citedby-count"},
            )
            if data:
                try:
                    count = (
                        data["abstracts-retrieval-response"]
                        ["coredata"]["citedby-count"]
                    )
                    prod.nr_citacoes_publicacao = str(count)
                    ok += 1
                except (KeyError, TypeError):
                    pass
        logger.info(f"  ✓ {ok}/{len(with_doi)} enriquecidas")
        return ok

    def enrich_autores(
        self,
        autor_instances: Dict[str, AutorInstance],
        citacao_instances: List[CitacaoInstance],
        ano_base: int = 2024,
    ) -> int:
        if not self._enabled:
            return 0
        logger.info(
            f"\n[SCOPUS] Índices de autor: {len(autor_instances)} autores..."
        )
        ok = 0
        for autor in autor_instances.values():
            # 1. Author Search por nome
            data = self._get(
                f"{self.BASE}/search/author",
                {
                    "query": f"AUTHNAME({autor.nm_pessoa})",
                    "field": "dc:identifier,h-index",
                    "count": "1",
                },
            )
            print(f"Buscando autor: {autor.nm_pessoa}, indice do loop atual: {ok}")
            if not data:
                continue
            try:
                entries = data["search-results"]["entry"]
                if not entries or "error" in entries[0]:
                    continue
                raw_id    = entries[0].get("dc:identifier", "")
                scopus_id = raw_id.split(":")[-1] if ":" in raw_id else raw_id
                autor.ds_scopus_id = scopus_id
            except (KeyError, IndexError, TypeError):
                continue

            # 2. Author Retrieval
            data2 = self._get(
                f"{self.BASE}/author/author_id/{autor.ds_scopus_id}",
                {"field": "h-index,cited-by-count,document-count"},
            )
            if not data2:
                continue
            try:
                core  = data2["author-retrieval-response"][0]["coredata"]
                h_idx = safe_int(core.get("h-index", 0))
                total = safe_int(core.get("cited-by-count", 0))
                i10   = self._i10(autor.ds_scopus_id)

                citacao_id = f"citacao_{autor.id_pessoa}_{ano_base}"
                citacao_instances.append(CitacaoInstance(
                    id=citacao_id,
                    autor_id=autor.id,
                    an_base_citacao=ano_base,
                    nr_citacoes_autor=total,
                    nr_indice_h=h_idx,
                    nr_indice_i10=i10,
                ))
                ok += 1
            except (KeyError, IndexError, TypeError):
                continue

        logger.info(f"  ✓ {ok}/{len(autor_instances)} enriquecidos")
        return ok

    def _i10(self, scopus_author_id: str) -> int:
        """
        Aproxima o i10-index contando publicações com >= 10 citações.
        O Scopus não expõe i10 diretamente (é métrica do Google Scholar),
        então usamos a Search API com AU-ID + REFCOUNT(10).
        """
        data = self._get(
            f"{self.BASE}/search/scopus",
            {
                "query": f"AU-ID({scopus_author_id}) AND REFCOUNT(10)",
                "field": "dc:identifier",
                "count": "1",
            },
        )
        if not data:
            return 0
        try:
            return safe_int(
                data["search-results"]["opensearch:totalResults"], 0
            )
        except (KeyError, TypeError):
            return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLASSE 4: OMLGenerator
# ─────────────────────────────────────────────────────────────────────────────

class OMLGenerator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _section(*titles: str) -> List[str]:
        lines = ["", "\t// " + "=" * 63]
        for t in titles:
            lines.append(f"\t// {t}")
        lines.append("\t// " + "=" * 63)
        return lines

    def generate(self, ext: InstanceExtractor) -> str:
        logger.info("\n[GENERATE] Construindo cti-pe.oml...")
        ns = VOCABULARY_NAMESPACE

        conceitos_by_ppg: Dict[str, List[ConceituPPGInstance]] = defaultdict(list)
        for c in ext.conceito_instances:
            conceitos_by_ppg[c.cd_programa_ies].append(c)

        citacoes_by_autor: Dict[str, List[CitacaoInstance]] = defaultdict(list)
        for cit in ext.citacao_instances:
            citacoes_by_autor[cit.autor_id].append(cit)

        lines = [
            "@dc:description "
            '"Descrição CT&I Pernambuco — gerado pelo pipeline GIC-UFRPE"',
            f"description <{CTI_PE_DESCRIPTION_URI}#> as cti-pe {{",
            "",
            f"\tuses <{DC_URI}> as {DC_NAMESPACE}",
            f"\tuses <{VOCABULARY_URI}#> as {VOCABULARY_NAMESPACE}",
        ]

        # ── ICT ──
        lines += self._section("INSTITUIÇÕES DE CIÊNCIA E TECNOLOGIA (ICT)")
        for ict in ext.ict_instances.values():
            lines += [
                "",
                f"\tinstance {ict.id} : {ns}:ICT [",
                f'\t\t{ns}:cd_entidade_capes "{escape_oml(ict.cd_entidade_capes)}"',
                f'\t\t{ns}:sg_entidade_ensino "{escape_oml(ict.sg_entidade_ensino)}"',
                f'\t\t{ns}:nm_entidade_ensino "{escape_oml(ict.nm_entidade_ensino)}"',
                f'\t\t{ns}:sg_uf "{ict.sg_uf}"',
                "\t]",
            ]

        # ── PPG ──
        lines += self._section(
            "PROGRAMAS DE PÓS-GRADUAÇÃO (PPG)",
            "sediado → ICT   |   avaliado → Conceito_PPG",
        )
        for ppg in ext.ppg_instances.values():
            lines += ["", f"\tinstance {ppg.id} : {ns}:PPG ["]
            lines.append(f'\t\t{ns}:cd_programa_ies "{escape_oml(ppg.cd_programa_ies)}"')
            lines.append(f'\t\t{ns}:nm_programa_ies "{escape_oml(ppg.nm_programa_ies)}"')
            lines.append(f'\t\t{ns}:nm_modalidade_programa "{escape_oml(ppg.nm_modalidade_programa)}"')
            lines.append(f'\t\t{ns}:nm_area_conhecimento "{escape_oml(ppg.nm_area_conhecimento)}"')
            lines.append(f"\t\t{ns}:sediado {ppg.ict_id}")
            for conceito in conceitos_by_ppg.get(ppg.cd_programa_ies, []):
                lines.append(f"\t\t{ns}:avaliado {conceito.id}")
            lines.append("\t]")

        # ── Conceito_PPG ──
        lines += self._section("CONCEITOS DE AVALIAÇÃO CAPES (Conceito_PPG)")
        for c in ext.conceito_instances:
            lines += [
                "",
                f"\tinstance {c.id} : {ns}:Conceito_PPG [",
                f'\t\t{ns}:cd_conceito_programa "{escape_oml(c.cd_conceito_programa)}"',
                f"\t\t{ns}:an_base_conceito {c.an_base_conceito}",
                "\t]",
            ]

        # ── Docente ──
        lines += self._section(
            "DOCENTES (ORIENTADORES)",
            "ID gerado via MD5(nome) — estável entre execuções",
        )
        for doc in ext.docente_instances.values():
            lines += [
                "",
                f"\tinstance {doc.id} : {ns}:Docente [",
                f'\t\t{ns}:nm_pessoa "{escape_oml(doc.nm_pessoa)}"',
                "\t]",
            ]

        # ── Discente ──
        lines += self._section(
            "DISCENTES",
            "vinculado → PPG   |   orientador → Docente",
        )
        for disc in ext.discente_instances.values():
            lines += ["", f"\tinstance {disc.id} : {ns}:Discente ["]
            lines.append(f"\t\t{ns}:id_pessoa {disc.id_pessoa}")
            lines.append(f'\t\t{ns}:nm_pessoa "{escape_oml(disc.nm_pessoa)}"')
            lines.append(f'\t\t{ns}:ds_grau_academico_discente "{escape_oml(disc.ds_grau_academico_discente)}"')
            lines.append(f'\t\t{ns}:nm_situacao_discente "{escape_oml(disc.nm_situacao_discente)}"')
            if disc.an_nascimento > 0:
                lines.append(f"\t\t{ns}:an_nascimento {disc.an_nascimento}")
            if disc.nm_pais_nacionalidade:
                lines.append(f'\t\t{ns}:nm_pais_nacionalidade "{escape_oml(disc.nm_pais_nacionalidade)}"')
            if disc.qt_mes_titulacao > 0:
                lines.append(f"\t\t{ns}:qt_mes_titulacao {disc.qt_mes_titulacao}")
            lines.append(f"\t\t{ns}:vinculado {disc.vinculado_id}")
            if disc.orientador_id:
                lines.append(f"\t\t{ns}:orientador {disc.orientador_id}")
            lines.append("\t]")

        # ── Autor ──
        lines += self._section(
            "AUTORES (subconceito de Pessoa)",
            "mensurado → Citacao   |   IDs Scopus/ORCID via enriquecimento",
        )
        for autor in ext.autor_instances.values():
            cits = citacoes_by_autor.get(autor.id, [])
            lines += ["", f"\tinstance {autor.id} : {ns}:Autor ["]
            lines.append(f"\t\t{ns}:id_pessoa {autor.id_pessoa}")
            lines.append(f'\t\t{ns}:nm_pessoa "{escape_oml(autor.nm_pessoa)}"')
            if autor.ds_scopus_id:
                lines.append(f'\t\t{ns}:ds_scopus_id "{escape_oml(autor.ds_scopus_id)}"')
            if autor.ds_orc_id:
                lines.append(f'\t\t{ns}:ds_orc_id "{escape_oml(autor.ds_orc_id)}"')
            if autor.ds_url_google_scholar:
                lines.append(f'\t\t{ns}:ds_url_google_scholar "{escape_oml(autor.ds_url_google_scholar)}"')
            for cit in cits:
                lines.append(f"\t\t{ns}:mensurado {cit.id}")
            lines.append("\t]")

        # ── Veiculo_Publicacao ──
        lines += self._section("VEÍCULOS DE PUBLICAÇÃO")
        for vei in ext.veiculo_instances.values():
            lines += ["", f"\tinstance {vei.id} : {ns}:Veiculo_Publicacao ["]
            lines.append(f'\t\t{ns}:nm_veiculo_publicacao "{escape_oml(vei.nm_veiculo_publicacao)}"')
            if vei.ds_isbn_issn:
                lines.append(f'\t\t{ns}:ds_isbn_issn "{escape_oml(vei.ds_isbn_issn)}"')
            if vei.nr_citescore_scopus > 0:
                lines.append(f"\t\t{ns}:nr_citescore_scopus {vei.nr_citescore_scopus}")
            if vei.nr_quartil_scopus > 0:
                lines.append(f"\t\t{ns}:nr_quartil_scopus {vei.nr_quartil_scopus}")
            lines.append("\t]")

        # ── Producao_Cientifica ──
        lines += self._section(
            "PRODUÇÕES CIENTÍFICAS",
            "autoria → Autor (N:N)   |   publicada → Veiculo_Publicacao",
        )
        for prod in ext.producao_instances.values():
            lines += ["", f"\tinstance {prod.id} : {ns}:Producao_Cientifica ["]
            lines.append(f'\t\t{ns}:nm_titulo "{escape_oml(prod.nm_titulo)}"')
            lines.append(f'\t\t{ns}:ds_natureza "{escape_oml(prod.ds_natureza)}"')
            if prod.an_base_producao > 0:
                lines.append(f"\t\t{ns}:an_base_producao {prod.an_base_producao}")
            if prod.ds_doi:
                lines.append(f'\t\t{ns}:ds_doi "{escape_oml(prod.ds_doi)}"')
            if prod.ds_url_acesso:
                lines.append(f'\t\t{ns}:ds_url_acesso "{escape_oml(prod.ds_url_acesso)}"')
            if prod.ds_palavras_chave:
                lines.append(f'\t\t{ns}:ds_palavras_chave "{escape_oml(prod.ds_palavras_chave)}"')
            # xsd:string na ontologia
            lines.append(f'\t\t{ns}:nr_citacoes_publicacao "{prod.nr_citacoes_publicacao}"')
            if prod.veiculo_id:
                lines.append(f"\t\t{ns}:publicada {prod.veiculo_id}")
            for autor_id in prod.autor_ids:
                lines.append(f"\t\t{ns}:autoria {autor_id}")
            lines.append("\t]")

        # ── Citacao ──
        lines += self._section(
            "CITAÇÕES (métricas por autor)",
            "Vinculadas ao Autor via relação 'mensurado'",
        )
        for cit in ext.citacao_instances:
            lines += [
                "",
                f"\tinstance {cit.id} : {ns}:Citacao [",
                f"\t\t{ns}:an_base_citacao {cit.an_base_citacao}",
                f"\t\t{ns}:nr_citacoes_autor {cit.nr_citacoes_autor}",
                f"\t\t{ns}:nr_indice_h {cit.nr_indice_h}",
                f"\t\t{ns}:nr_indice_i10 {cit.nr_indice_i10}",
                "\t]",
            ]

        lines += ["", "}"]
        return "\n".join(lines)

    def save(self, filename: str, content: str) -> Path:
        filepath = self.output_dir / filename
        with open(filepath, "w", encoding="utf-8") as fh:
            fh.write(content)
        logger.info(f"  ✓ OML salvo: {filepath}")
        return filepath


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTÊNCIA DE ESTADO
# ─────────────────────────────────────────────────────────────────────────────

def save_state(ext: InstanceExtractor):
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_PICKLE_PATH, "wb") as fh:
        pickle.dump(ext, fh)
    logger.info(f"  Estado salvo: {STATE_PICKLE_PATH}")


def load_state() -> Optional[InstanceExtractor]:
    if not STATE_PICKLE_PATH.exists():
        logger.error(
            f"  Estado intermediário não encontrado: {STATE_PICKLE_PATH}\n"
            "  Execute primeiro: python generate_oml_cti_full.py --steps capes"
        )
        return None
    with open(STATE_PICKLE_PATH, "rb") as fh:
        ext = pickle.load(fh)
    logger.info(f"  Estado carregado: {STATE_PICKLE_PATH}")
    return ext


# ─────────────────────────────────────────────────────────────────────────────
# AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(summary: Dict):
    print("\n" + "=" * 58)
    print("  EXTRACTION SUMMARY")
    print("=" * 58)
    for name, count in summary.items():
        print(f"  {name:<30} {count:>10,}")
    print("=" * 58 + "\n")


def check_integrity(ext: InstanceExtractor) -> List[str]:
    warns = []
    for d in ext.discente_instances.values():
        if d.vinculado_id not in ext.ppg_instances:
            warns.append(f"Discente {d.id}: PPG '{d.vinculado_id}' não existe.")
        if d.orientador_id and d.orientador_id not in ext.docente_instances:
            warns.append(f"Discente {d.id}: Docente '{d.orientador_id}' não existe.")
    for p in ext.producao_instances.values():
        for aid in p.autor_ids:
            if aid not in ext.autor_instances:
                warns.append(f"Producao {p.id}: Autor '{aid}' não existe.")
        if p.veiculo_id and p.veiculo_id not in ext.veiculo_instances:
            warns.append(f"Producao {p.id}: Veiculo '{p.veiculo_id}' não existe.")
    return warns


def export_audit(ext: InstanceExtractor):
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for disc in ext.discente_instances.values():
        autor_id = ext._pessoa_to_autor.get(str(disc.id_pessoa), "")
        autor    = ext.autor_instances.get(autor_id)
        n_prods  = (
            sum(1 for p in ext.producao_instances.values() if autor_id in p.autor_ids)
            if autor_id else 0
        )
        rows.append({
            "id_pessoa":        disc.id_pessoa,
            "nm_discente":      disc.nm_pessoa,
            "grau":             disc.ds_grau_academico_discente,
            "situacao":         disc.nm_situacao_discente,
            "ppg_id":           disc.vinculado_id,
            "scopus_author_id": autor.ds_scopus_id if autor else "",
            "n_producoes":      n_prods,
        })
    path = DATA_PROCESSED_DIR / "cti_pe_audit.csv"
    pd.DataFrame(rows).to_csv(path, sep=";", encoding="utf-8-sig", index=False)
    logger.info(f"  ✓ Auditoria salva: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI — seleção de etapas
# ─────────────────────────────────────────────────────────────────────────────

STEP_GROUPS: Dict[str, Set[str]] = {
    "capes":  {"load", "extract", "integrity", "save_state"},
    "scopus": {"load_state", "scopus"},
    "oml":    {"load_state", "generate", "audit"},
    "all":    {"load", "extract", "integrity", "save_state",
               "scopus", "generate", "audit"},
}


def parse_args() -> Set[str]:
    parser = argparse.ArgumentParser(
        description="Pipeline CT&I-PE: CAPES + Scopus → OML",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Grupos de etapas:
  all     → tudo (padrão)
  capes   → carrega/extrai CAPES e salva estado em pipeline_state.pkl
  scopus  → enriquecimento Scopus (requer estado salvo pelo grupo 'capes')
  oml     → gera OML + CSV de auditoria (requer estado salvo pelo grupo 'capes')

Exemplos:
  python generate_oml_cti_full.py
  python generate_oml_cti_full.py --steps capes
  python generate_oml_cti_full.py --steps scopus
  python generate_oml_cti_full.py --steps oml
  python generate_oml_cti_full.py --steps capes,oml
  python generate_oml_cti_full.py --steps capes,scopus,oml
        """,
    )
    parser.add_argument(
        "--steps",
        default="all",
        metavar="GRUPOS",
        help="Grupos separados por vírgula: all | capes | scopus | oml",
    )
    args   = parser.parse_args()
    groups = {s.strip().lower() for s in args.steps.split(",")}
    steps: Set[str] = set()
    for g in groups:
        if g not in STEP_GROUPS:
            parser.error(
                f"Grupo desconhecido: '{g}'. "
                f"Opções válidas: {', '.join(STEP_GROUPS)}"
            )
        steps |= STEP_GROUPS[g]
    return steps


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    steps = parse_args()

    logger.info("=" * 58)
    logger.info("  CT&I-PE → OML Pipeline  (GIC-UFRPE 2025)")
    logger.info(f"  Etapas ativas: {', '.join(sorted(steps))}")
    logger.info("=" * 58)

    ext: Optional[InstanceExtractor] = None
    loader: Optional[DataLoader]     = None

    try:
        # ── Carregamento CAPES ────────────────────────────────────────────
        if "load" in steps:
            logger.info("\n[STEP 1] Carregando datasets CAPES...")
            loader = DataLoader(state=STATE_FILTER)
            loader.load_all()

        # ── Extração de instâncias ────────────────────────────────────────
        if "extract" in steps:
            if loader is None:
                logger.error("  DataLoader não disponível. Execute --steps capes.")
                return 1
            logger.info("\n[STEP 2] Extraindo instâncias OML...")
            ext = InstanceExtractor(loader)
            ext.extract_from_programas()
            ext.extract_from_discentes()
            ext.extract_from_producao()
            print_summary(ext.get_summary())

        # ── Integridade referencial ───────────────────────────────────────
        if "integrity" in steps:
            if ext is None:
                logger.warning("  Extrator não disponível — etapa pulada.")
            else:
                logger.info("\n[STEP 3] Verificando integridade referencial...")
                warns = check_integrity(ext)
                if warns:
                    logger.warning(f"  {len(warns)} aviso(s):")
                    for w in warns[:20]:
                        logger.warning(f"    ⚠ {w}")
                    if len(warns) > 20:
                        logger.warning(f"    ... e mais {len(warns) - 20}.")
                else:
                    logger.info("  ✓ Sem problemas de integridade.")

        # ── Salvar estado intermediário ───────────────────────────────────
        if "save_state" in steps:
            if ext is not None:
                logger.info("\n[STEP 3b] Salvando estado intermediário...")
                save_state(ext)

        # ── Carregar estado (etapas scopus/oml sem capes) ─────────────────
        if "load_state" in steps and ext is None:
            logger.info("\n[STEP] Carregando estado intermediário...")
            ext = load_state()
            if ext is None:
                return 1

        # ── Enriquecimento Scopus ─────────────────────────────────────────
        if "scopus" in steps:
            if ext is None:
                logger.error("  Estado não disponível.")
                return 1
            logger.info("\n[STEP 4] Enriquecimento Scopus...")
            enricher = ScopusEnricher()
            if enricher._enabled:
                enricher.enrich_producao_by_doi(ext.producao_instances)
                enricher.enrich_autores(
                    ext.autor_instances,
                    ext.citacao_instances,
                    ano_base=2024,
                )
                save_state(ext)   # persistir com métricas Scopus
            else:
                logger.info(
                    "  Scopus pulado (API Key ausente).\n"
                    "  Configure SCOPUS_API_KEY no .env e execute:\n"
                    "    python generate_oml_cti_full.py --steps scopus"
                )

        # ── Geração do OML ────────────────────────────────────────────────
        if "generate" in steps:
            if ext is None:
                logger.error("  Estado não disponível.")
                return 1
            logger.info("\n[STEP 5] Gerando arquivo OML...")
            gen     = OMLGenerator(OML_OUTPUT_DIR)
            content = gen.generate(ext)
            gen.save("cti-pe.oml", content)

        # ── CSV de auditoria ──────────────────────────────────────────────
        if "audit" in steps:
            if ext is not None:
                logger.info("\n[STEP 6] Exportando CSV de auditoria...")
                export_audit(ext)

        # ── Conclusão ─────────────────────────────────────────────────────
        logger.info("\n" + "=" * 58)
        logger.info("  PIPELINE CONCLUÍDO!")
        logger.info("=" * 58)
        if "generate" in steps:
            logger.info(f"  OML:       {OML_OUTPUT_DIR / 'cti-pe.oml'}")
        if "audit" in steps:
            logger.info(f"  Auditoria: {DATA_PROCESSED_DIR / 'cti_pe_audit.csv'}")
        if "save_state" in steps or "scopus" in steps:
            logger.info(f"  Estado:    {STATE_PICKLE_PATH}")
        return 0

    except Exception as e:
        logger.error(f"\n✗ Erro: {e}", exc_info=True)
        return 1


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.exit(main())