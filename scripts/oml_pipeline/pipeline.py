import logging
from typing import Dict, List, Optional, Set

import pandas as pd

from .capes_io import DataLoader
from .config import DATA_PROCESSED_DIR, OML_OUTPUT_DIR
from .extractor import InstanceExtractor
from .oml_generator import OMLGenerator
from .scopus import ScopusEnricher
from .state import load_state, save_state

logger = logging.getLogger(__name__)

STEP_GROUPS: Dict[str, Set[str]] = {
    "capes": {"load", "extract", "integrity", "save_state"},
    "scopus": {"load_state", "scopus"},
    "oml": {"load_state", "generate", "audit"},
    "all": {"load", "extract", "integrity", "save_state", "scopus", "generate", "audit"},
}


def print_summary(summary: Dict):
    print("\n" + "=" * 58)
    print("  EXTRACTION SUMMARY")
    print("=" * 58)
    for name, count in summary.items():
        print(f"  {name:<30} {count:>10,}")
    print("=" * 58 + "\n")


def check_integrity(ext: InstanceExtractor) -> List[str]:
    warns = []
    for disc in ext.discente_instances.values():
        if disc.vinculado_id not in ext.ppg_instances:
            warns.append(f"Discente {disc.id}: PPG '{disc.vinculado_id}' não existe.")

    for prod in ext.producao_instances.values():
        for aid in prod.autor_ids:
            if aid not in ext.autor_instances:
                warns.append(f"Producao {prod.id}: Autor '{aid}' não existe.")
        if prod.veiculo_id and prod.veiculo_id not in ext.veiculo_instances:
            warns.append(f"Producao {prod.id}: Veiculo '{prod.veiculo_id}' não existe.")

    return warns


def export_audit(ext: InstanceExtractor):
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for disc in ext.discente_instances.values():
        autor_id = ext.pessoa_to_autor.get(str(disc.id_pessoa), "")
        autor = ext.autor_instances.get(autor_id)
        n_prods = (
            sum(1 for p in ext.producao_instances.values() if autor_id in p.autor_ids)
            if autor_id
            else 0
        )
        rows.append(
            {
                "id_pessoa": disc.id_pessoa,
                "nm_discente": disc.nm_pessoa,
                "grau": disc.ds_grau_academico_discente,
                "situacao": disc.nm_situacao_discente,
                "ppg_id": disc.vinculado_id,
                "scopus_author_id": autor.ds_scopus_id if autor else "",
                "n_producoes": n_prods,
            }
        )

    path = DATA_PROCESSED_DIR / "cti_pe_audit.csv"
    pd.DataFrame(rows).to_csv(path, sep=";", encoding="utf-8-sig", index=False)
    logger.info("  ✓ Auditoria salva: %s", path)


def run_pipeline(
    steps: Set[str],
    scopus_limit: int,
    scopus_mode: str = "incremental",
    scopus_reset_progress: bool = False,
    scopus_max_retries: int = 3,
    scopus_backoff_base_seconds: float = 2.0,
    scopus_backoff_max_seconds: float = 120.0,
) -> int:
    ext: Optional[InstanceExtractor] = None
    loader: Optional[DataLoader] = None

    if "load" in steps:
        logger.info("\n[STEP 1] Carregando datasets CAPES...")
        loader = DataLoader()
        loader.load_all()

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

    if "integrity" in steps:
        if ext is None:
            logger.warning("  Extrator não disponível — etapa pulada.")
        else:
            logger.info("\n[STEP 3] Verificando integridade referencial...")
            warns = check_integrity(ext)
            if warns:
                logger.warning("  %s aviso(s):", len(warns))
                for warn in warns[:20]:
                    logger.warning("    ⚠ %s", warn)
                if len(warns) > 20:
                    logger.warning("    ... e mais %s.", len(warns) - 20)
            else:
                logger.info("  ✓ Sem problemas de integridade.")

    if "save_state" in steps and ext is not None:
        logger.info("\n[STEP 3b] Salvando estado intermediário...")
        save_state(ext)

    if "load_state" in steps and ext is None:
        logger.info("\n[STEP] Carregando estado intermediário...")
        ext = load_state()
        if ext is None:
            return 1

    if "scopus" in steps:
        if ext is None:
            logger.error("  Estado não disponível.")
            return 1

        logger.info("\n[STEP 4] Enriquecimento Scopus...")
        enricher = ScopusEnricher(
            mode=scopus_mode,
            reset_progress=scopus_reset_progress,
            max_retries=scopus_max_retries,
            backoff_base_seconds=scopus_backoff_base_seconds,
            backoff_max_seconds=scopus_backoff_max_seconds,
        )
        if enricher.enabled:
            enricher.enrich_producao_by_doi(ext.producao_instances, max_items=scopus_limit)
            enricher.enrich_autores(
                ext.autor_instances,
                ext.citacao_instances,
                ano_base=2024,
                max_items=scopus_limit,
            )
            save_state(ext)
        else:
            logger.info(
                "  Scopus pulado (API Key ausente).\n"
                "  Configure SCOPUS_API_KEY no .env e execute:\n"
                "    python scripts/generate_oml_cti_full.py --steps scopus"
            )

    if "generate" in steps:
        if ext is None:
            logger.error("  Estado não disponível.")
            return 1

        logger.info("\n[STEP 5] Gerando arquivo OML...")
        gen = OMLGenerator(OML_OUTPUT_DIR)
        content = gen.generate(ext)
        gen.save("cti-pe.oml", content)

    if "audit" in steps and ext is not None:
        logger.info("\n[STEP 6] Exportando CSV de auditoria...")
        export_audit(ext)

    logger.info("\n" + "=" * 58)
    logger.info("  PIPELINE CONCLUÍDO!")
    logger.info("=" * 58)
    return 0
