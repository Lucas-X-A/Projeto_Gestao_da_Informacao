import logging
import json
import random
import time
from typing import Dict, List, Optional, Tuple

import requests

from .config import (
    DATA_PROCESSED_DIR,
    SCOPUS_API_KEY,
    SCOPUS_BACKOFF_BASE_SECONDS_DEFAULT,
    SCOPUS_BACKOFF_JITTER_SECONDS_DEFAULT,
    SCOPUS_BACKOFF_MAX_SECONDS_DEFAULT,
    SCOPUS_CACHE_PATH,
    SCOPUS_CHECKPOINT_PATH,
    SCOPUS_AUTHORS_PROCESSED_PATH,  
    SCOPUS_DOIS_PROCESSED_PATH,
    SCOPUS_DELAY,
    SCOPUS_MAX_RETRIES_DEFAULT,
    SCOPUS_MODE_DEFAULT,
    SCOPUS_TIMEOUT_SECONDS,
)
from .models import AutorInstance, CitacaoInstance, ProducaoCientificaInstance
from .utils import safe_int

logger = logging.getLogger(__name__)


def _read_json(path, default_value):
    if not path.exists():
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except Exception:
        return default_value


def _write_json(path, payload):
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, indent=2, ensure_ascii=False)


def _normalize_doi(raw: str) -> str:
    value = raw.strip()
    prefixes = [
        "https://doi.org/",
        "http://doi.org/",
        "doi.org/",
        "doi:",
    ]
    lowered = value.lower()
    for prefix in prefixes:
        if lowered.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().lower()


class ScopusEnricher:
    BASE = "https://api.elsevier.com/content"
    HDR = {"Accept": "application/json"}
    MAX_FAILURES_PER_ITEM = 3

    def __init__(
        self,
        api_key: str = SCOPUS_API_KEY,
        mode: str = SCOPUS_MODE_DEFAULT,
        max_retries: int = SCOPUS_MAX_RETRIES_DEFAULT,
        backoff_base_seconds: float = SCOPUS_BACKOFF_BASE_SECONDS_DEFAULT,
        backoff_max_seconds: float = SCOPUS_BACKOFF_MAX_SECONDS_DEFAULT,
        backoff_jitter_seconds: float = SCOPUS_BACKOFF_JITTER_SECONDS_DEFAULT,
        reset_progress: bool = False,
    ):
        self.api_key = api_key
        self.enabled = bool(api_key)
        self.mode = "full" if mode == "full" else "incremental"
        self.max_retries = max(max_retries, 0)
        self.backoff_base_seconds = max(backoff_base_seconds, 0.1)
        self.backoff_max_seconds = max(backoff_max_seconds, self.backoff_base_seconds)
        self.backoff_jitter_seconds = max(backoff_jitter_seconds, 0.0)

        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.cache = _read_json(SCOPUS_CACHE_PATH, {"doi": {}, "authors": {}})
        self.checkpoint = _read_json(
            SCOPUS_CHECKPOINT_PATH,
            {
                "version": 1,
                "doi_done": [],
                "doi_pending": [],
                "doi_failures": {},
                "author_done": [],
                "author_pending": [],
                "author_failures": {},
            },
        )

        if reset_progress:
            self.cache = {"doi": {}, "authors": {}}
            self.checkpoint = {
                "version": 1,
                "doi_done": [],
                "doi_pending": [],
                "doi_failures": {},
                "author_done": [],
                "author_pending": [],
                "author_failures": {},
            }
            SCOPUS_AUTHORS_PROCESSED_PATH.write_text("")
            SCOPUS_DOIS_PROCESSED_PATH.write_text("")
            self._save_state()

        if not self.enabled:
            logger.warning(
                "[SCOPUS] API Key não configurada. Enriquecimento desabilitado.\n"
                "  Configure no .env: SCOPUS_API_KEY=sua_chave_aqui"
            )
        else:
            logger.info(
                "[SCOPUS] modo=%s | retries=%s | backoff_base=%.2fs | backoff_max=%.2fs",
                self.mode,
                self.max_retries,
                self.backoff_base_seconds,
                self.backoff_max_seconds,
            )

    def _save_processed_list(self, kind: str, processed_list: List[str]):
        """Salva lista de processados em arquivo txt legível."""
        if kind == "author":
            path = SCOPUS_AUTHORS_PROCESSED_PATH
        elif kind == "doi":
            path = SCOPUS_DOIS_PROCESSED_PATH
        else:
            return
        
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# {kind.upper()} - Processados em {len(processed_list)}\n")
                f.write("# Atualizado automaticamente pelo pipeline\n\n")
                for item in processed_list:
                    f.write(f"{item}\n")
            logger.debug(f"[SCOPUS] Lista de {kind}s salvos em {path}")
        except Exception as e:
            logger.warning(f"[SCOPUS] Erro ao salvar lista de {kind}s: {e}")
    
    def _save_state(self):
        _write_json(SCOPUS_CACHE_PATH, self.cache)
        _write_json(SCOPUS_CHECKPOINT_PATH, self.checkpoint)
        self._save_processed_list("author", self.checkpoint.get("author_done", []))
        self._save_processed_list("doi", self.checkpoint.get("doi_done", []))
        
    def _load_processed_list(self, kind: str) -> set:
        """Carrega lista de processados do arquivo txt."""
        if kind == "author":
            path = SCOPUS_AUTHORS_PROCESSED_PATH
        elif kind == "doi":
            path = SCOPUS_DOIS_PROCESSED_PATH
        else:
            return set()
        
        if not path.exists():
            return set()
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                items = {line.strip() for line in f if line.strip() and not line.startswith("#")}
            return items
        except Exception as e:
            logger.warning(f"[SCOPUS] Erro ao carregar lista de {kind}s: {e}")
            return set()


    def _get_lists_and_sets(self, kind: str) -> Tuple[List[str], set, List[str], Dict[str, int]]:
        done_list = self.checkpoint.get(f"{kind}_done", [])
        pending_list = self.checkpoint.get(f"{kind}_pending", [])
        failures = self.checkpoint.get(f"{kind}_failures", {})
        return done_list, set(done_list), pending_list, failures

    def _prepare_queue(self, keys: List[str], kind: str) -> List[str]:
        done_list, done_set, pending_list, _ = self._get_lists_and_sets(kind)
        del done_list
        keys_set = set(keys)
        pending_valid = [k for k in pending_list if k in keys_set]

        if self.mode == "full":
            queue = keys
        else:
            new_keys = [k for k in keys if k not in done_set and k not in pending_valid]
            queue = pending_valid + new_keys

        # Persist pending queue trimmed to known candidates
        self.checkpoint[f"{kind}_pending"] = queue
        return queue

    def _mark_success(self, kind: str, key: str):
        done_list, done_set, pending_list, failures = self._get_lists_and_sets(kind)
        if key not in done_set:
            done_list.append(key)
            self.checkpoint[f"{kind}_done"] = done_list
        if key in pending_list:
            pending_list.remove(key)
            self.checkpoint[f"{kind}_pending"] = pending_list
        failures.pop(key, None)
        self.checkpoint[f"{kind}_failures"] = failures

    def _mark_failure(self, kind: str, key: str):
        _, _, pending_list, failures = self._get_lists_and_sets(kind)
        current = safe_int(failures.get(key, 0), 0) + 1
        failures[key] = current

        if current >= self.MAX_FAILURES_PER_ITEM:
            if key in pending_list:
                pending_list.remove(key)
            logger.info(
                "[SCOPUS] %s '%s' atingiu %s falhas e será retirado da fila pendente.",
                kind,
                key,
                self.MAX_FAILURES_PER_ITEM,
            )

        self.checkpoint[f"{kind}_pending"] = pending_list
        self.checkpoint[f"{kind}_failures"] = failures

    def _get(self, url: str, params: dict) -> Optional[dict]:
        if not self.enabled:
            return None

        params_with_key = dict(params)
        params_with_key["apiKey"] = self.api_key

        for attempt in range(self.max_retries + 1):
            try:
                time.sleep(SCOPUS_DELAY)
                response = requests.get(
                    url,
                    headers=self.HDR,
                    params=params_with_key,
                    timeout=SCOPUS_TIMEOUT_SECONDS,
                )

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 401:
                    logger.error("[SCOPUS] 401 — verifique a API Key.")
                    self.enabled = False
                    return None

                if response.status_code == 403:
                    logger.error(
                        "[SCOPUS] 403 — acesso negado.\n"
                        "  Certifique-se de estar na rede da instituição (IP permitido)."
                    )
                    return None

                retryable = response.status_code in (429, 500, 502, 503, 504)
                if not retryable:
                    logger.debug("[SCOPUS] HTTP %s: %s", response.status_code, url)
                    return None

                if attempt >= self.max_retries:
                    logger.warning(
                        "[SCOPUS] HTTP %s após %s tentativas: %s",
                        response.status_code,
                        self.max_retries + 1,
                        url,
                    )
                    return None

                sleep_seconds = min(
                    self.backoff_base_seconds * (2 ** attempt),
                    self.backoff_max_seconds,
                ) + random.uniform(0.0, self.backoff_jitter_seconds)
                logger.warning(
                    "[SCOPUS] HTTP %s (tentativa %s/%s). Aguardando %.2fs para retry...",
                    response.status_code,
                    attempt + 1,
                    self.max_retries + 1,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    logger.error("[SCOPUS] Erro de rede: %s", exc)
                    return None
                sleep_seconds = min(
                    self.backoff_base_seconds * (2 ** attempt),
                    self.backoff_max_seconds,
                ) + random.uniform(0.0, self.backoff_jitter_seconds)
                logger.warning(
                    "[SCOPUS] Erro de rede (%s). Retry em %.2fs (%s/%s).",
                    exc,
                    sleep_seconds,
                    attempt + 1,
                    self.max_retries + 1,
                )
                time.sleep(sleep_seconds)

        return None

    def enrich_producao_by_doi(
        self,
        producao_instances: Dict[str, ProducaoCientificaInstance],
        max_items: int = 100,
    ) -> int:
        if not self.enabled:
            return 0

        by_doi: Dict[str, ProducaoCientificaInstance] = {}
        for prod in producao_instances.values():
            if not prod.ds_doi:
                continue
            doi = _normalize_doi(prod.ds_doi)
            if doi and doi not in by_doi:
                by_doi[doi] = prod

        all_dois = sorted(by_doi.keys())
        queue = self._prepare_queue(all_dois, kind="doi")
        selected = queue[:max_items] if max_items > 0 else []

        logger.info(
            "\n[SCOPUS] Citações por DOI: %s candidatas, %s na fila, %s processadas (limite=%s)",
            len(all_dois),
            len(queue),
            len(selected),
            max_items,
        )

        enriched = 0
        for doi in selected:
            prod = by_doi[doi]
            cached = self.cache.get("doi", {}).get(doi)
            if cached and cached.get("status") == "ok":
                prod.nr_citacoes_publicacao = str(cached.get("nr_citacoes_publicacao", "0"))
                self._mark_success("doi", doi)
                enriched += 1
                continue

            data = self._get(f"{self.BASE}/abstract/doi/{doi}", {"field": "citedby-count"})
            if not data:
                self._mark_failure("doi", doi)
                continue

            try:
                cited_by_count = data["abstracts-retrieval-response"]["coredata"]["citedby-count"]
                prod.nr_citacoes_publicacao = str(cited_by_count)
                self.cache.setdefault("doi", {})[doi] = {
                    "status": "ok",
                    "nr_citacoes_publicacao": str(cited_by_count),
                }
                self._mark_success("doi", doi)
                enriched += 1
            except (KeyError, TypeError):
                self.cache.setdefault("doi", {})[doi] = {"status": "parse_error"}
                self._mark_failure("doi", doi)
                continue

        self._save_state()
        logger.info("  ✓ %s/%s produções enriquecidas", enriched, len(selected))
        return enriched

    def _upsert_citacao(self, citacao_instances: List[CitacaoInstance], item: CitacaoInstance):
        for idx, current in enumerate(citacao_instances):
            if current.id == item.id:
                citacao_instances[idx] = item
                return
        citacao_instances.append(item)

    def enrich_autores(
        self,
        autor_instances: Dict[str, AutorInstance],
        citacao_instances: List[CitacaoInstance],
        ano_base: int = 2024,
        max_items: int = 100,
    ) -> int:
        if not self.enabled:
            return 0

        by_author_id = {autor.id: autor for autor in autor_instances.values()}
        all_author_keys = sorted(by_author_id.keys())
        queue = self._prepare_queue(all_author_keys, kind="author")
        selected = queue[:max_items] if max_items > 0 else []

        logger.info(
            "\n[SCOPUS] Índices de autor: %s candidatos, %s na fila, %s processados (limite=%s)",
            len(all_author_keys),
            len(queue),
            len(selected),
            max_items,
        )

        enriched = 0
        for author_key in selected:
            autor = by_author_id[author_key]
            cached = self.cache.get("authors", {}).get(author_key)
            if cached and cached.get("status") == "ok":
                autor.ds_scopus_id = cached.get("ds_scopus_id", "")
                self._upsert_citacao(
                    citacao_instances,
                    CitacaoInstance(
                        id=f"citacao_{autor.id_pessoa}_{ano_base}",
                        autor_id=autor.id,
                        an_base_citacao=ano_base,
                        nr_citacoes_autor=safe_int(cached.get("nr_citacoes_autor", 0), 0),
                        nr_indice_h=safe_int(cached.get("nr_indice_h", 0), 0),
                        nr_indice_i10=safe_int(cached.get("nr_indice_i10", 0), 0),
                    ),
                )
                self._mark_success("author", author_key)
                enriched += 1
                continue

            scopus_id = autor.ds_scopus_id
            if not scopus_id:
                data = self._get(
                    f"{self.BASE}/search/author",
                    {
                        "query": f"AUTHNAME({autor.nm_pessoa})",
                        "field": "dc:identifier,h-index",
                        "count": "1",
                    },
                )
                if not data:
                    self._mark_failure("author", author_key)
                    continue

                try:
                    entries = data["search-results"]["entry"]
                    if not entries or "error" in entries[0]:
                        self._mark_failure("author", author_key)
                        continue
                    raw_id = entries[0].get("dc:identifier", "")
                    scopus_id = raw_id.split(":")[-1] if ":" in raw_id else raw_id
                    autor.ds_scopus_id = scopus_id
                except (KeyError, IndexError, TypeError):
                    self._mark_failure("author", author_key)
                    continue

            data2 = self._get(
                f"{self.BASE}/author/author_id/{scopus_id}",
                {"field": "h-index,cited-by-count,document-count"},
            )
            if not data2:
                self._mark_failure("author", author_key)
                continue

            try:
                core = data2["author-retrieval-response"][0]["coredata"]
                h_idx = safe_int(core.get("h-index", 0))
                total = safe_int(core.get("cited-by-count", 0))
                i10 = self._i10(scopus_id)
                self._upsert_citacao(
                    citacao_instances,
                    CitacaoInstance(
                        id=f"citacao_{autor.id_pessoa}_{ano_base}",
                        autor_id=autor.id,
                        an_base_citacao=ano_base,
                        nr_citacoes_autor=total,
                        nr_indice_h=h_idx,
                        nr_indice_i10=i10,
                    )
                )

                self.cache.setdefault("authors", {})[author_key] = {
                    "status": "ok",
                    "ds_scopus_id": scopus_id,
                    "nr_citacoes_autor": total,
                    "nr_indice_h": h_idx,
                    "nr_indice_i10": i10,
                }
                self._mark_success("author", author_key)
                enriched += 1
            except (KeyError, IndexError, TypeError):
                self.cache.setdefault("authors", {})[author_key] = {"status": "parse_error"}
                self._mark_failure("author", author_key)
                continue

        self._save_state()
        logger.info("  ✓ %s/%s autores enriquecidos", enriched, len(selected))
        return enriched

    def _i10(self, scopus_author_id: str) -> int:
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
            return safe_int(data["search-results"]["opensearch:totalResults"], 0)
        except (KeyError, TypeError):
            return 0
