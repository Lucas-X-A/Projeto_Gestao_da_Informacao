from dataclasses import dataclass, field
from typing import List


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
    an_base_conceito: int


@dataclass
class DiscenteInstance:
    id: str
    id_pessoa: int
    nm_pessoa: str
    an_nascimento: int
    nm_pais_nacionalidade: str
    ds_grau_academico_discente: str
    nm_situacao_discente: str
    qt_mes_titulacao: int
    vinculado_id: str


@dataclass
class AutorInstance:
    id: str
    id_pessoa: int
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
    an_base_producao: int
    ds_url_acesso: str
    ds_palavras_chave: str
    ds_doi: str
    nr_citacoes_publicacao: str
    veiculo_id: str
    autor_ids: List[str] = field(default_factory=list)


@dataclass
class VeiculoPublicacaoInstance:
    id: str
    nm_veiculo_publicacao: str
    ds_isbn_issn: str
    nr_citescore_scopus: int
    nr_quartil_scopus: int


@dataclass
class CitacaoInstance:
    id: str
    autor_id: str
    an_base_citacao: int
    nr_citacoes_autor: int
    nr_indice_h: int
    nr_indice_i10: int
