import logging
from collections import defaultdict
from typing import Dict, List, Set

import pandas as pd

from .capes_io import DataLoader
from .config import COLS_AUTORES, COLS_DISCENTES, COLS_PRODUCAO, COLS_PROGRAMAS, STATE_FILTER
from .models import (
    AutorInstance,
    CitacaoInstance,
    ConceituPPGInstance,
    DiscenteInstance,
    ICTInstance,
    PPGInstance,
    ProducaoCientificaInstance,
    VeiculoPublicacaoInstance,
)
from .utils import oml_safe_id, safe_int, safe_str, stable_hash

logger = logging.getLogger(__name__)


class InstanceExtractor:
    def __init__(self, loader: DataLoader):
        self.loader = loader
        self.ict_instances: Dict[str, ICTInstance] = {}
        self.ppg_instances: Dict[str, PPGInstance] = {}
        self.conceito_instances: List[ConceituPPGInstance] = []
        self.discente_instances: Dict[str, DiscenteInstance] = {}
        self.autor_instances: Dict[str, AutorInstance] = {}
        self.producao_instances: Dict[str, ProducaoCientificaInstance] = {}
        self.veiculo_instances: Dict[str, VeiculoPublicacaoInstance] = {}
        self.citacao_instances: List[CitacaoInstance] = []
        self.pessoa_to_autor: Dict[str, str] = {}

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
            cd_prog = safe_str(row[c["cd_prog"]])
            an_base = safe_int(row[c["ano"]])
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
            self.conceito_instances.append(
                ConceituPPGInstance(
                    id=f"conceito_{oml_safe_id(cd_prog)}_{an_base}",
                    cd_programa_ies=cd_prog,
                    cd_conceito_programa=safe_str(raw_conc),
                    an_base_conceito=an_base,
                )
            )

        logger.info(f"  ICTs:      {len(self.ict_instances):,}")
        logger.info(f"  PPGs:      {len(self.ppg_instances):,}")
        logger.info(f"  Conceitos: {len(self.conceito_instances):,}")

    def extract_from_discentes(self):
        df = self.loader.df_discentes
        if df is None:
            logger.warning("  Dataset de Discentes não disponível — pulando.")
            return

        c = COLS_DISCENTES
        logger.info("\n[EXTRACT-B] Discentes → Discente")

        col_id = c["id_pessoa"]
        col_ano = c["ano"]
        if col_id not in df.columns:
            logger.error(
                f"  Coluna '{col_id}' não encontrada no CSV de discentes.\n"
                f"  Colunas disponíveis: {', '.join(df.columns.tolist()[:20])}"
            )
            return

        df_sorted = df.sort_values(col_ano, ascending=False, na_position="last")
        df_unique = df_sorted.drop_duplicates(subset=[col_id], keep="first")

        for _, row in df_unique.iterrows():
            id_str = safe_str(row[col_id])
            if not id_str:
                continue
            disc_id = f"discente_{id_str}"
            ppg_id = f"ppg_{oml_safe_id(safe_str(row[c['cd_prog']]))}"

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
            )

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

        logger.info(f"  Discentes: {len(self.discente_instances):,}")

    def extract_from_producao(self):
        df_aut = self.loader.df_autores
        df_pro = self.loader.df_producao
        ca, cp = COLS_AUTORES, COLS_PRODUCAO

        if df_aut is None:
            logger.warning("\n[EXTRACT-C] Dataset de Autores não disponível — pulando.")
            return
        if df_pro is None:
            logger.warning("\n[EXTRACT-C] Dataset de Produção não disponível — pulando.")
            return

        logger.info("\n[EXTRACT-C] Autores × Produção → Autor / Producao / Veiculo")

        col_id_aut = ca["id_pessoa"]
        if col_id_aut not in df_aut.columns:
            available = ", ".join(df_aut.columns.tolist()[:25])
            logger.error(
                f"  Coluna '{col_id_aut}' não encontrada no CSV de autores.\n"
                f"  Colunas disponíveis (primeiras 25): {available}"
            )
            return

        ids_disc_pe: Set[str] = {str(d.id_pessoa) for d in self.discente_instances.values()}
        if not ids_disc_pe:
            logger.warning("  Nenhum discente PE encontrado — abortando extração de Produção.")
            return

        logger.info(f"  IDs de discentes PE: {len(ids_disc_pe):,}")

        df_aut_pe = df_aut[df_aut[col_id_aut].astype(str).str.strip().isin(ids_disc_pe)].copy()
        logger.info(f"  Linhas autores PE: {len(df_aut_pe):,} (de {len(df_aut):,} total)")
        if df_aut_pe.empty:
            logger.warning(
                "  Nenhum autor encontrado para os discentes PE.\n"
                "  Verifique se ID_PESSOA_DISCENTE nos autores corresponde ao ID_PESSOA nos discentes."
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

        df_pro_pe = df_pro[df_pro[col_id_add_pro].astype(str).isin(ids_add_pe)].copy()
        logger.info(f"  Produções vinculadas: {len(df_pro_pe):,}")

        vei_key_cols = [col for col in [cp["issn"], cp["veiculo"]] if col in df_pro_pe.columns]
        if vei_key_cols:
            for _, row in df_pro_pe.drop_duplicates(subset=vei_key_cols, keep="first").iterrows():
                issn = safe_str(row.get(cp["issn"]))
                nm_vei = safe_str(row.get(cp["veiculo"]))
                chave = issn or nm_vei
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

        for _, row in df_pro_pe.iterrows():
            id_add_str = safe_str(row[col_id_add_pro])
            prod_id = f"prod_{oml_safe_id(id_add_str)}"
            if prod_id in self.producao_instances:
                continue
            issn = safe_str(row.get(cp["issn"]))
            nm_vei = safe_str(row.get(cp["veiculo"]))
            chave = issn or nm_vei
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

        for _, row in df_aut_pe.iterrows():
            id_pes_str = safe_str(row[col_id_aut])
            id_add_str = safe_str(row[col_id_add])
            autor_id = f"autor_{oml_safe_id(id_pes_str)}"

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
                self.pessoa_to_autor[id_pes_str] = autor_id

            prod_id = f"prod_{oml_safe_id(id_add_str)}"
            if prod_id in self.producao_instances:
                prod = self.producao_instances[prod_id]
                if autor_id not in prod.autor_ids:
                    prod.autor_ids.append(autor_id)

        logger.info(f"  Autores:   {len(self.autor_instances):,}")
        logger.info(f"  Produções: {len(self.producao_instances):,}")
        logger.info(f"  Veículos:  {len(self.veiculo_instances):,}")

    def get_summary(self):
        return {
            "ICT": len(self.ict_instances),
            "PPG": len(self.ppg_instances),
            "Conceito_PPG": len(self.conceito_instances),
            "Discente": len(self.discente_instances),
            "Autor": len(self.autor_instances),
            "Producao_Cientifica": len(self.producao_instances),
            "Veiculo_Publicacao": len(self.veiculo_instances),
            "Citacao": len(self.citacao_instances),
        }
