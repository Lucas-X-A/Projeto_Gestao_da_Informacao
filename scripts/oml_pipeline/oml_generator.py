import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from .config import CTI_PE_DESCRIPTION_URI, DC_NAMESPACE, DC_URI, VOCABULARY_NAMESPACE, VOCABULARY_URI
from .extractor import InstanceExtractor
from .models import CitacaoInstance, ConceituPPGInstance
from .utils import escape_oml

logger = logging.getLogger(__name__)


class OMLGenerator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _section(*titles: str) -> List[str]:
        lines = ["", "\t// " + "=" * 63]
        for title in titles:
            lines.append(f"\t// {title}")
        lines.append("\t// " + "=" * 63)
        return lines

    def generate(self, ext: InstanceExtractor) -> str:
        logger.info("\n[GENERATE] Construindo cti-pe.oml...")
        ns = VOCABULARY_NAMESPACE

        conceitos_by_ppg: Dict[str, List[ConceituPPGInstance]] = defaultdict(list)
        for conceito in ext.conceito_instances:
            conceitos_by_ppg[conceito.cd_programa_ies].append(conceito)

        citacoes_by_autor: Dict[str, List[CitacaoInstance]] = defaultdict(list)
        for citacao in ext.citacao_instances:
            citacoes_by_autor[citacao.autor_id].append(citacao)

        producoes_by_autor: Dict[str, List[str]] = defaultdict(list)
        for producao in ext.producao_instances.values():
            for autor_id in producao.autor_ids:
                producoes_by_autor[autor_id].append(producao.id)

        lines = [
            "@dc:description "
            '"Descrição CT&I Pernambuco — gerado pelo pipeline GIC-UFRPE"',
            f"description <{CTI_PE_DESCRIPTION_URI}#> as cti-pe {{",
            "",
            f"\tuses <{DC_URI}> as {DC_NAMESPACE}",
            f"\tuses <{VOCABULARY_URI}#> as {VOCABULARY_NAMESPACE}",
        ]

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

        lines += self._section("PROGRAMAS DE PÓS-GRADUAÇÃO (PPG)", "sediado → ICT   |   avaliado → Conceito_PPG")
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

        lines += self._section("CONCEITOS DE AVALIAÇÃO CAPES (Conceito_PPG)")
        for conceito in ext.conceito_instances:
            lines += [
                "",
                f"\tinstance {conceito.id} : {ns}:Conceito_PPG [",
                f'\t\t{ns}:cd_conceito_programa "{escape_oml(conceito.cd_conceito_programa)}"',
                f"\t\t{ns}:an_base_conceito {conceito.an_base_conceito}",
                "\t]",
            ]

        lines += self._section("DISCENTES", "vinculado → PPG")
        for discente in ext.discente_instances.values():
            lines += ["", f"\tinstance {discente.id} : {ns}:Discente ["]
            lines.append(f"\t\t{ns}:id_pessoa {discente.id_pessoa}")
            lines.append(f'\t\t{ns}:nm_pessoa "{escape_oml(discente.nm_pessoa)}"')
            lines.append(f'\t\t{ns}:ds_grau_academico_discente "{escape_oml(discente.ds_grau_academico_discente)}"')
            lines.append(f'\t\t{ns}:nm_situacao_discente "{escape_oml(discente.nm_situacao_discente)}"')
            if discente.an_nascimento > 0:
                lines.append(f"\t\t{ns}:an_nascimento {discente.an_nascimento}")
            if discente.nm_pais_nacionalidade:
                lines.append(f'\t\t{ns}:nm_pais_nacionalidade "{escape_oml(discente.nm_pais_nacionalidade)}"')
            if discente.qt_mes_titulacao > 0:
                lines.append(f"\t\t{ns}:qt_mes_titulacao {discente.qt_mes_titulacao}")
            lines.append(f"\t\t{ns}:vinculado {discente.vinculado_id}")
            lines.append("\t]")

        lines += self._section("AUTORES (subconceito de Pessoa)", "mensurado → Citacao   |   autoria → Producao_Cientifica   |   IDs Scopus/ORCID via enriquecimento")
        for autor in ext.autor_instances.values():
            citacoes = citacoes_by_autor.get(autor.id, [])
            producoes = producoes_by_autor.get(autor.id, [])
            lines += ["", f"\tinstance {autor.id} : {ns}:Autor ["]
            lines.append(f"\t\t{ns}:id_pessoa {autor.id_pessoa}")
            lines.append(f'\t\t{ns}:nm_pessoa "{escape_oml(autor.nm_pessoa)}"')
            if autor.ds_scopus_id:
                lines.append(f'\t\t{ns}:ds_scopus_id "{escape_oml(autor.ds_scopus_id)}"')
            if autor.ds_orc_id:
                lines.append(f'\t\t{ns}:ds_orc_id "{escape_oml(autor.ds_orc_id)}"')
            if autor.ds_url_google_scholar:
                lines.append(f'\t\t{ns}:ds_url_google_scholar "{escape_oml(autor.ds_url_google_scholar)}"')
            for citacao in citacoes:
                lines.append(f"\t\t{ns}:mensurado {citacao.id}")
            for producao_id in producoes:
                lines.append(f"\t\t{ns}:autoria {producao_id}")
            lines.append("\t]")

        lines += self._section("VEÍCULOS DE PUBLICAÇÃO")
        for veiculo in ext.veiculo_instances.values():
            lines += ["", f"\tinstance {veiculo.id} : {ns}:Veiculo_Publicacao ["]
            lines.append(f'\t\t{ns}:nm_veiculo_publicacao "{escape_oml(veiculo.nm_veiculo_publicacao)}"')
            if veiculo.ds_isbn_issn:
                lines.append(f'\t\t{ns}:ds_isbn_issn "{escape_oml(veiculo.ds_isbn_issn)}"')
            if veiculo.nr_citescore_scopus > 0:
                lines.append(f"\t\t{ns}:nr_citescore_scopus {veiculo.nr_citescore_scopus}")
            if veiculo.nr_quartil_scopus > 0:
                lines.append(f"\t\t{ns}:nr_quartil_scopus {veiculo.nr_quartil_scopus}")
            lines.append("\t]")

        lines += self._section("PRODUÇÕES CIENTÍFICAS", "publicada → Veiculo_Publicacao")
        for producao in ext.producao_instances.values():
            lines += ["", f"\tinstance {producao.id} : {ns}:Producao_Cientifica ["]
            lines.append(f'\t\t{ns}:nm_titulo "{escape_oml(producao.nm_titulo)}"')
            lines.append(f'\t\t{ns}:ds_natureza "{escape_oml(producao.ds_natureza)}"')
            if producao.an_base_producao > 0:
                lines.append(f"\t\t{ns}:an_base_producao {producao.an_base_producao}")
            if producao.ds_doi:
                lines.append(f'\t\t{ns}:ds_doi "{escape_oml(producao.ds_doi)}"')
            if producao.ds_url_acesso:
                lines.append(f'\t\t{ns}:ds_url_acesso "{escape_oml(producao.ds_url_acesso)}"')
            if producao.ds_palavras_chave:
                lines.append(f'\t\t{ns}:ds_palavras_chave "{escape_oml(producao.ds_palavras_chave)}"')
            lines.append(f'\t\t{ns}:nr_citacoes_publicacao "{producao.nr_citacoes_publicacao}"')
            if producao.veiculo_id:
                lines.append(f"\t\t{ns}:publicada {producao.veiculo_id}")
            lines.append("\t]")

        lines += self._section("CITAÇÕES (métricas por autor)", "Vinculadas ao Autor via relação 'mensurado'")
        for citacao in ext.citacao_instances:
            lines += [
                "",
                f"\tinstance {citacao.id} : {ns}:Citacao [",
                f"\t\t{ns}:an_base_citacao {citacao.an_base_citacao}",
                f"\t\t{ns}:nr_citacoes_autor {citacao.nr_citacoes_autor}",
                f"\t\t{ns}:nr_indice_h {citacao.nr_indice_h}",
                f"\t\t{ns}:nr_indice_i10 {citacao.nr_indice_i10}",
                "\t]",
            ]

        lines += ["", "}"]
        return "\n".join(lines)

    def save(self, filename: str, content: str) -> Path:
        file_path = self.output_dir / filename
        with open(file_path, "w", encoding="utf-8") as file_obj:
            file_obj.write(content)
        logger.info("  ✓ OML salvo: %s", file_path)
        return file_path
