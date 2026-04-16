import os
from pathlib import Path
from typing import List, Optional

try:
    from dotenv import load_dotenv

    _script_dir = Path(__file__).resolve().parent.parent
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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_CAPES_DIR = PROJECT_ROOT / "data" / "raw" / "capes"
DATA_PROGRAMAS_DIR = DATA_CAPES_DIR / "programas"
DATA_DISCENTES_DIR = DATA_CAPES_DIR / "discentes"
DATA_AUTORES_DIR = DATA_CAPES_DIR / "autores"
DATA_PRODUCAO_DIR = DATA_CAPES_DIR / "producao"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OML_OUTPUT_DIR = PROJECT_ROOT / "src" / "oml" / "gic.ufrpe.br" / "cti" / "description"
STATE_PICKLE_PATH = DATA_PROCESSED_DIR / "pipeline_state.pkl"

VOCABULARY_URI = "http://gic.ufrpe.br/cti/vocabulary/cti"
VOCABULARY_NAMESPACE = "cti"
CTI_PE_DESCRIPTION_URI = "http://gic.ufrpe.br/cti/description/cti-pe"
DC_URI = "http://purl.org/dc/elements/1.1/"
DC_NAMESPACE = "dc"

SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY", os.getenv("ELSEVIER_API_KEY", ""))
STATE_FILTER = os.getenv("STATE_FILTER", "PE")
SCOPUS_DELAY = float(os.getenv("SCOPUS_DELAY", "0.15"))
SCOPUS_TIMEOUT_SECONDS = int(os.getenv("SCOPUS_TIMEOUT_SECONDS", "15"))
SCOPUS_MAX_ITEMS_DEFAULT = int(os.getenv("SCOPUS_MAX_ITEMS", "100"))
SCOPUS_MODE_DEFAULT = os.getenv("SCOPUS_MODE", "incremental").strip().lower()
SCOPUS_MAX_RETRIES_DEFAULT = int(os.getenv("SCOPUS_MAX_RETRIES", "3"))
SCOPUS_BACKOFF_BASE_SECONDS_DEFAULT = float(os.getenv("SCOPUS_BACKOFF_BASE_SECONDS", "2"))
SCOPUS_BACKOFF_MAX_SECONDS_DEFAULT = float(os.getenv("SCOPUS_BACKOFF_MAX_SECONDS", "120"))
SCOPUS_BACKOFF_JITTER_SECONDS_DEFAULT = float(os.getenv("SCOPUS_BACKOFF_JITTER_SECONDS", "0.5"))
CSV_CHUNKSIZE = int(os.getenv("CSV_CHUNKSIZE", "200000"))

SCOPUS_CACHE_PATH = DATA_PROCESSED_DIR / "scopus_cache.json"
SCOPUS_CHECKPOINT_PATH = DATA_PROCESSED_DIR / "scopus_checkpoint.json"

_sit_raw = os.getenv("SITUACAO_FILTER", "TITULADO")
SITUACAO_FILTER: Optional[List[str]] = (
    [s.strip() for s in _sit_raw.split(",") if s.strip()]
    if _sit_raw.strip()
    else None
)

COLS_PROGRAMAS = {
    "uf": "SG_UF_PROGRAMA",
    "cd_ent": "CD_ENTIDADE_CAPES",
    "sg_ent": "SG_ENTIDADE_ENSINO",
    "nm_ent": "NM_ENTIDADE_ENSINO",
    "cd_prog": "CD_PROGRAMA_IES",
    "nm_prog": "NM_PROGRAMA_IES",
    "modalidade": "NM_MODALIDADE_PROGRAMA",
    "area": "NM_AREA_CONHECIMENTO",
    "conceito": "CD_CONCEITO_PROGRAMA",
    "ano": "AN_BASE",
}

COLS_DISCENTES = {
    "uf": "SG_UF_PROGRAMA",
    "cd_ent": "CD_ENTIDADE_CAPES",
    "sg_ent": "SG_ENTIDADE_ENSINO",
    "nm_ent": "NM_ENTIDADE_ENSINO",
    "cd_prog": "CD_PROGRAMA_IES",
    "nm_prog": "NM_PROGRAMA_IES",
    "modalidade": "NM_MODALIDADE_PROGRAMA",
    "grande_area": "NM_GRANDE_AREA_CONHECIMENTO",
    "id_pessoa": "ID_PESSOA",
    "nm_disc": "NM_DISCENTE",
    "nascimento": "AN_NASCIMENTO_DISCENTE",
    "pais": "NM_PAIS_NACIONALIDADE_DISCENTE",
    "grau": "DS_GRAU_ACADEMICO_DISCENTE",
    "situacao": "NM_SITUACAO_DISCENTE",
    "mes_tit": "QT_MES_TITULACAO",
    "ano": "AN_BASE",
}

COLS_AUTORES = {
    "id_add": "ID_ADD_PRODUCAO_INTELECTUAL",
    "id_pessoa": "ID_PESSOA_DISCENTE",
    "nm_autor": "NM_AUTOR",
    "tipo_vinculo": "DS_TIPO_VINCULO_PRODUCAO",
    "cd_prog": "CD_PROGRAMA_IES",
    "ano": "AN_BASE",
}

COLS_PRODUCAO = {
    "id_add": "ID_ADD_PRODUCAO_INTELECTUAL",
    "titulo": "NM_PRODUCAO",
    "natureza": "DS_NATUREZA",
    "ano": "AN_BASE_PRODUCAO",
    "url": "DS_URL_ACESSO_PRODUCAO",
    "palavras": "DS_PALAVRA_CHAVE",
    "doi": "DS_DOI",
    "veiculo": "NM_PERIODICO",
    "issn": "DS_ISSN",
    "cd_prog": "CD_PROGRAMA_IES",
    "base_ano": "AN_BASE",
}
