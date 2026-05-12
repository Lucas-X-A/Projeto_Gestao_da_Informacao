import logging
import pickle
from typing import Optional

from .config import DATA_PROCESSED_DIR, STATE_PICKLE_PATH
from .extractor import InstanceExtractor

logger = logging.getLogger(__name__)


def save_state(ext: InstanceExtractor):
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_PICKLE_PATH, "wb") as file_obj:
        pickle.dump(ext, file_obj)
    logger.info("  Estado salvo: %s", STATE_PICKLE_PATH)


def load_state() -> Optional[InstanceExtractor]:
    if not STATE_PICKLE_PATH.exists():
        logger.error(
            "  Estado intermediário não encontrado: %s\n"
            "  Execute primeiro: python scripts/generate_oml_cti_full.py --steps capes",
            STATE_PICKLE_PATH,
        )
        return None

    with open(STATE_PICKLE_PATH, "rb") as file_obj:
        ext = pickle.load(file_obj)
    logger.info("  Estado carregado: %s", STATE_PICKLE_PATH)
    return ext
