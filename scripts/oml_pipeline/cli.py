import argparse
import logging
import sys
from typing import Set

from .config import (
    SCOPUS_BACKOFF_BASE_SECONDS_DEFAULT,
    SCOPUS_BACKOFF_MAX_SECONDS_DEFAULT,
    SCOPUS_MAX_ITEMS_DEFAULT,
    SCOPUS_MAX_RETRIES_DEFAULT,
    SCOPUS_MODE_DEFAULT,
)
from .pipeline import STEP_GROUPS, run_pipeline


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_steps(raw_steps: str) -> Set[str]:
    groups = {item.strip().lower() for item in raw_steps.split(",")}
    steps: Set[str] = set()
    for group in groups:
        if group not in STEP_GROUPS:
            valid = ", ".join(STEP_GROUPS.keys())
            raise ValueError(f"Grupo desconhecido: '{group}'. Opções válidas: {valid}")
        steps |= STEP_GROUPS[group]
    return steps


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline CT&I-PE: CAPES + Scopus → OML",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Grupos de etapas:\n"
            "  all     → tudo (padrão)\n"
            "  capes   → carrega/extrai CAPES e salva estado em pipeline_state.pkl\n"
            "  scopus  → enriquecimento Scopus (requer estado salvo pelo grupo 'capes')\n"
            "  oml     → gera OML + CSV de auditoria (requer estado salvo pelo grupo 'capes')\n\n"
            "Exemplos:\n"
            "  python scripts/generate_oml_cti_full.py\n"
            "  python scripts/generate_oml_cti_full.py --steps capes\n"
            "  python scripts/generate_oml_cti_full.py --steps scopus\n"
            "  python scripts/generate_oml_cti_full.py --steps oml\n"
            "  python scripts/generate_oml_cti_full.py --steps capes,oml\n"
            "  python scripts/generate_oml_cti_full.py --steps capes,scopus,oml\n"
            "  python scripts/generate_oml_cti_full.py --steps scopus --scopus-limit 100\n"
        ),
    )
    parser.add_argument(
        "--steps",
        default="all",
        metavar="GRUPOS",
        help="Grupos separados por vírgula: all | capes | scopus | oml",
    )
    parser.add_argument(
        "--scopus-limit",
        type=int,
        default=SCOPUS_MAX_ITEMS_DEFAULT,
        help="Limite máximo de itens no enriquecimento Scopus (autores e produções).",
    )
    parser.add_argument(
        "--scopus-mode",
        choices=["incremental", "full"],
        default=SCOPUS_MODE_DEFAULT if SCOPUS_MODE_DEFAULT in {"incremental", "full"} else "incremental",
        help="Modo de execução do Scopus: incremental (com checkpoint) ou full.",
    )
    parser.add_argument(
        "--scopus-reset-progress",
        action="store_true",
        help="Reseta cache e checkpoint do Scopus antes de executar a etapa.",
    )
    parser.add_argument(
        "--scopus-max-retries",
        type=int,
        default=SCOPUS_MAX_RETRIES_DEFAULT,
        help="Número máximo de retries por requisição Scopus.",
    )
    parser.add_argument(
        "--scopus-backoff-base",
        type=float,
        default=SCOPUS_BACKOFF_BASE_SECONDS_DEFAULT,
        help="Backoff base em segundos para retries Scopus.",
    )
    parser.add_argument(
        "--scopus-backoff-max",
        type=float,
        default=SCOPUS_BACKOFF_MAX_SECONDS_DEFAULT,
        help="Backoff máximo em segundos para retries Scopus.",
    )
    parser.add_argument(
        "--scopus-batch-size",
        type=int,
        default=SCOPUS_MAX_ITEMS_DEFAULT,
        help="Tamanho do lote para enriquecimento Scopus (itens por execução).",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        steps = parse_steps(args.steps)
    except ValueError as exc:
        parser.error(str(exc))

    logger.info("=" * 58)
    logger.info("  CT&I-PE → OML Pipeline  (GIC-UFRPE)")
    logger.info("  Etapas ativas: %s", ", ".join(sorted(steps)))
    logger.info("  Limite Scopus: %s", args.scopus_limit)
    logger.info("  Modo Scopus: %s", args.scopus_mode)
    logger.info("=" * 58)

    try:
        return run_pipeline(
            steps=steps,
            scopus_limit=args.scopus_batch_size,
            scopus_mode=args.scopus_mode,
            scopus_reset_progress=bool(args.scopus_reset_progress),
            scopus_max_retries=max(args.scopus_max_retries, 0),
            scopus_backoff_base_seconds=max(args.scopus_backoff_base, 0.1),
            scopus_backoff_max_seconds=max(args.scopus_backoff_max, 0.1),
        )
    except Exception as exc:
        logger.error("\n✗ Erro: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
