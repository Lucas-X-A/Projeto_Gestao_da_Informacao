#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  SCRIPT: Gerador de OML a partir de Dados CAPES
  
  Um pipeline didático de engenharia de dados e conhecimento que transforma
  dados tabulares (CSV) da CAPES em ontologias estruturadas (OML).
  
  OBJETIVO:
    Demonstrar as melhores práticas em:
    • Leitura e processamento de dados (pandas)
    • Filtragem e limpeza (normalização, deduplicação)
    • Estruturas de dados (dataclasses)
    • Arquitetura modular (classes com responsabilidades únicas)
    • Geração de ontologias (OML com relações semânticas)
  
  ENTRADA:
    • 4 arquivos CSV (2021-2024) em data/raw/capes/
    • Formato: semicolon-delimited, encoding=iso-8859-1
    • Total: 18.597 linhas com dados de pós-graduação
  
  SAÍDA:
    • cti-pe.oml: Descrição OML unificada (867 instâncias)
    • capes_programas_pernambuco.csv: Dados processados (674 linhas)
  
  AUTOR:
    Grupo de Engenharia do Conhecimento (GIC)
    UFRPE - 2025
================================================================================
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict


# ============================================================================
# CONFIGURAÇÃO: Logging e Caminhos
# ============================================================================
# Configurar logging para ver o processamento passo-a-passo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Definir caminhos relativos ao projeto (mais portável que caminhos absolutos)
PROJECT_ROOT = Path(__file__).parent.parent
DATA_INPUT_DIR = PROJECT_ROOT / "data" / "raw" / "capes"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OML_OUTPUT_DIR = PROJECT_ROOT / "src" / "oml" / "gic.ufrpe.br" / "cti" / "description"

# Configuração de URIs e Namespaces para OML
# Estes definem a "linguagem" e "vocabulário" usados na ontologia
VOCABULARY_URI = "http://gic.ufrpe.br/cti/vocabulary/cti"
VOCABULARY_NAMESPACE = "cti"

# URI para a descrição unificada (onde moram as instâncias)
CTI_PE_DESCRIPTION_URI = "http://gic.ufrpe.br/cti/description/cti-pe"

# Dublin Core: padrão para anotações/metadados
DC_URI = "http://purl.org/dc/elements/1.1/"
DC_NAMESPACE = "dc"


# ============================================================================
# DATACLASSES: Estruturas de Dados Tipadas
# ============================================================================
# Dataclasses oferecem uma forma elegante de definir estruturas de dados
# com geração automática de __init__, __repr__, etc.
# Vantagem: Type hints claros, facilita refatoração, documenta intenção

@dataclass
class ICTInstance:
    """
    Representa uma INSTITUIÇÃO DE CIÊNCIA E TECNOLOGIA (ICT).
    
    Exemplo: UFRPE, UFPE, Instituto de Tecnologia de PE, etc.
    
    Atributos:
        id (str): Identificador único para OML (ex: "ict_25002")
                  Formato: "ict_{cd_entidade_capes}"
        
        cd_entidade_capes (str): Código numérico da CAPES
                                 Chave primária para deduplicação
                                 Ex: 25002 (UFRPE)
        
        sg_entidade_ensino (str): Sigla de 4 caracteres (ex: "UFRPE")
                                  Usado em consultas e relatórios
        
        nm_entidade_ensino (str): Nome completo da instituição
                                  Ex: "Universidade Federal Rural de Pernambuco"
        
        sg_uf (str): Estado (UF) de 2 letras (ex: "PE" para Pernambuco)
                     Permite agrupar por região geográfica
    """
    id: str
    cd_entidade_capes: str
    sg_entidade_ensino: str
    nm_entidade_ensino: str
    sg_uf: str


@dataclass
class PPGInstance:
    """
    Representa um PROGRAMA DE PÓS-GRADUAÇÃO (PPG).
    
    Um PPG é um programa de mestrado ou doutorado em uma instituição.
    
    Exemplo: Programa de Educação da UFRPE (modalidade ACADÊMICA)
    
    Atributos:
        id (str): Identificador único para OML (ex: "ppg_25001019001P7")
        
        cd_programa_ies (str): Código do programa
                               Chave primária para deduplicação
        
        nm_programa_ies (str): Nome do programa (ex: "EDUCAÇÃO")
        
        nm_modalidade_programa (str): Tipo de programa
                                      • "ACADÊMICO" = mestrado/doutorado acadêmico
                                      • "PROFISSIONAL" = mestrado profissional
                                      Diferencia programas com mesmo nome na mesma ICT
        
        nm_area_conhecimento (str): Área de pesquisa
                                    Ex: "Educação", "Ciência da Computação"
        
        ict_id (str): Referência à ICT onde o PPG é sediado
                      Exemplo de relacionamento: PPG --(sediado)--> ICT
                      Implementado como propriedade "sediado" no OML
    """
    id: str
    cd_programa_ies: str
    nm_programa_ies: str
    nm_modalidade_programa: str
    nm_area_conhecimento: str
    ict_id: str


@dataclass
class ConceituPPGInstance:
    """
    Representa uma AVALIAÇÃO de um PPG (conceito CAPES).
    
    A CAPES avalia programas periodicamente (geralmente anualmente).
    Cada avaliação atribui um conceito (1-7).
    
    Importante: Um PPG pode ter múltiplas avaliações (anos diferentes).
    
    Exemplo: PPG_EDUCACAO_UFRPE foi avaliado em:
        • 2021: conceito 5
        • 2022: conceito 5
        • 2023: conceito 5
    
    Cada uma é uma ConceituPPGInstance diferente!
    
    Atributos:
        id (str): Identificador único (ex: "conceito_25001019_2021")
                  Formato: "conceito_{cd_programa}_{ano}"
                  Permite distinguir mesma avaliação em anos diferentes
        
        cd_programa_ies (str): Qual programa foi avaliado?
                               Usamos para criar relação com PPGInstance
        
        cd_conceito_programa (str): Conceito de 1-7
                                    • 1: Iniciante (insuficiente)
                                    • 2: Desenvolvimento
                                    • 3: Consolidado
                                    • 4: Bem consolidado
                                    • 5: Muito bem consolidado (mestrado)
                                    • 6: Excelente (doutorado)
                                    • 7: Excepcional
        
        an_base_conceito (int): Ano da avaliação
                                2021, 2022, 2023, 2024 nos dados CAPES
    """
    id: str
    cd_programa_ies: str
    cd_conceito_programa: str
    an_base_conceito: int


# ============================================================================
# CLASSE 1: CAPESProcessor - Leitura, Filtragem, Limpeza de Dados
# ============================================================================

class CAPESProcessor:
    """
    Processador de dados CAPES.
    
    Responsabilidades:
    ✓ Ler múltiplos CSVs com glob() (busca dinâmica)
    ✓ Concatenar DataFrames de diferentes anos
    ✓ Filtrar por estado (UF)
    ✓ Normalizar dados (espaços, tipos)
    ✓ Validar qualidade (faltantes)
    ✓ Exportar dados processados
    
    Exemplo de uso:
        processor = CAPESProcessor()
        processor.read_csv_files()        # 18.597 linhas
        processor.filter_by_state("PE")   # 674 linhas (PE)
        processor.normalize_data()         # Limpa dados
        processor.validate_data()          # Verifica qualidade
    """
    
    def __init__(self):
        """Inicializar o processador."""
        self.dataframe = None
        self.processed_count = 0
        self.files_processed = []
        # Criar diretório de saída se não existir
        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        
    def read_csv_files(self) -> pd.DataFrame:
        """
        Ler todos os CSVs CAPES do diretório data/raw/capes/.
        
        Usa glob() para descobrir automaticamente os arquivos.
        Suporta múltiplos anos e concatena tudo.
        
        CSVs CAPES têm características específicas:
        • Delimiter: semicolon (;)
        • Encoding: ISO-8859-1 (suporta acentos Portuguese)
        • Ano em texto, converte para string primeiro
        
        Retorna:
            pd.DataFrame: Dados concatenados de todos os anos
            
        Levanta:
            FileNotFoundError: Se não encontrar CSVs em data/raw/capes/
        """
        logger.info(f"Searching for CSV files in: {DATA_INPUT_DIR}")
        
        # Usar glob para busca dinâmica de *.csv
        csv_files = list(DATA_INPUT_DIR.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(
                f"❌ Nenhum arquivo CSV encontrado em {DATA_INPUT_DIR}\n"
                f"   Copie os CSVs CAPES para data/raw/capes/"
            )
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Ler e concatenar todos os CSVs
        dfs = []
        for csv_file in sorted(csv_files):
            logger.info(f"Reading: {csv_file.name}")
            try:
                # Configuração específica para CSVs CAPES
                df = pd.read_csv(
                    csv_file,
                    delimiter=';',              # CAPES usa ponto-e-vírgula
                    encoding='iso-8859-1',      # Suporta acentos
                    dtype={'AN_BASE': str}      # Ano como string primeiro
                )
                dfs.append(df)
                self.files_processed.append(csv_file.name)
                logger.info(f"  ✓ Loaded {len(df)} rows")
            except Exception as e:
                logger.error(f"  ✗ Error reading {csv_file.name}: {e}")
        
        # Concatenar (combinar) todos os DataFrames em um único
        if dfs:
            self.dataframe = pd.concat(dfs, ignore_index=True)
            logger.info(f"Total rows after concatenation: {len(self.dataframe)}")
        
        return self.dataframe
    
    def filter_by_state(self, state_code: str = "PE") -> pd.DataFrame:
        """
        Filtrar dados para manter apenas registros de um estado (UF).
        
        Motivação didática:
        • Demonstra filtragem condicional
        • Reduz dados para escopo manejável
        • Pernambuco é foco do projeto
        
        Antes: 18.597 linhas (Brasil inteiro)
        Depois: 674 linhas (apenas PE)
        
        Args:
            state_code (str): Código UF (ex: "PE" para Pernambuco)
                              Padrão: "PE"
        
        Retorna:
            pd.DataFrame: DataFrame filtrado
        """
        if self.dataframe is None:
            raise ValueError(
                "❌ Nenhum dado carregado.\n"
                "   Execute read_csv_files() primeiro."
            )
        
        initial_count = len(self.dataframe)
        self.dataframe = self.dataframe[
            self.dataframe['SG_UF_PROGRAMA'] == state_code
        ]
        filtered_count = len(self.dataframe)
        
        logger.info(
            f"Filtered data for state '{state_code}': "
            f"{initial_count:,} → {filtered_count:,} rows"
        )
        
        return self.dataframe
    
    def normalize_data(self):
        """
        Normalizar (limpar) os dados.
        
        Operações:
        1. Remover espaços em branco (strip) de strings
           Ex: "  EDUCAÇÃO  " → "EDUCAÇÃO"
        
        2. Converter AN_BASE de string para inteiro
           Ex: "2021" (string) → 2021 (int)
        
        Motivação: Dados consistentes facilitam processamento
        """
        logger.info("Normalizing data...")
        
        # ✓ Strip: remover espaços antes/depois de strings
        for col in self.dataframe.select_dtypes(include=['object']).columns:
            self.dataframe[col] = self.dataframe[col].str.strip()
        
        # ✓ Converter ano para inteiro (se em string)
        if 'AN_BASE' in self.dataframe.columns:
            self.dataframe['AN_BASE'] = pd.to_numeric(
                self.dataframe['AN_BASE'],
                errors='coerce'  # Invalid values → NaN
            ).astype('Int64')   # Int64 suporta NaN (int padrão não suporta)
        
        logger.info("Data normalization complete ✓")
    
    def validate_data(self) -> Dict[str, int]:
        """
        Validar qualidade dos dados.
        
        Verificações:
        • Colunas críticas têm valores?
        • Quantos registros têm faltantes (NaN)?
        • Remover linhas incompletas
        
        Retorna:
            Dict[str, int]: Resumo de faltantes por coluna
        """
        logger.info("Validating data quality...")
        
        # Colunas que NÃO podem estar vazias
        critical_columns = [
            'CD_ENTIDADE_CAPES',
            'SG_ENTIDADE_ENSINO',
            'NM_ENTIDADE_ENSINO',
            'CD_PROGRAMA_IES',
            'NM_PROGRAMA_IES',
            'CD_CONCEITO_PROGRAMA',
            'AN_BASE'
        ]
        
        # Contar faltantes por coluna (diagnóstico)
        missing_summary = {}
        for col in critical_columns:
            if col in self.dataframe.columns:
                missing_count = self.dataframe[col].isna().sum()
                missing_summary[col] = missing_count
                if missing_count > 0:
                    logger.warning(
                        f"  ⚠️  Missing values in {col}: {missing_count}"
                    )
        
        # Remover linhas com faltantes
        before_clean = len(self.dataframe)
        self.dataframe = self.dataframe.dropna(
            subset=critical_columns,
            how='any'  # Remove se ANY coluna crítica estiver NULL
        )
        after_clean = len(self.dataframe)
        
        if before_clean != after_clean:
            logger.info(
                f"Removed {before_clean - after_clean} rows with missing values"
            )
        
        return missing_summary
    
    def save_processed_data(self) -> Path:
        """
        Salvar dados processados em CSV.
        
        Útil para:
        • Debugar o processamento
        • Análise posterior com Excel/R
        • Validar filtering
        
        Retorna:
            Path: Caminho do arquivo salvo
        """
        if self.dataframe is None:
            raise ValueError("No data loaded.")
        
        logger.info("Saving processed data...")
        
        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        output_file = DATA_PROCESSED_DIR / "capes_programas_pernambuco.csv"
        
        self.dataframe.to_csv(
            output_file,
            sep=';',
            encoding='iso-8859-1',
            index=False
        )
        
        logger.info(f"  ✓ Saved: {output_file}")
        logger.info(f"    {len(self.dataframe):,} rows, "
                   f"{len(self.dataframe.columns)} columns")
        
        return output_file


# ============================================================================
# CLASSE 2: InstanceExtractor - Deduplicação e Extração
# ============================================================================

class InstanceExtractor:
    """
    Extrator de instâncias (entidades únicas) dos dados.
    
    Responsabilidades:
    ✓ Identificar entidades únicas (deduplicação)
    ✓ Criar instâncias com IDs únicos
    ✓ Mapear relacionamentos
    
    Conceito de deduplicação:
    
    ❌ SEM deduplicação:
       18.597 registros → 18.597 instâncias ICT (muitas repetidas!)
    
    ✅ COM deduplicação:
       ICT: 1 instância por CD_ENTIDADE_CAPES (15 total)
       PPG: 1 instância por CD_PROGRAMA_IES (178 total)
       Conceito: N instâncias (674 - não deduplicamos anos)
    
    Total: 867 instâncias únicas
    """
    
    def __init__(self, dataframe: pd.DataFrame):
        """
        Inicializar com DataFrame já processado.
        
        Args:
            dataframe: DataFrame do CAPESProcessor
        """
        self.dataframe = dataframe
        self.ict_instances: Dict[str, ICTInstance] = {}
        self.ppg_instances: Dict[str, PPGInstance] = {}
        self.conceito_instances: List[ConceituPPGInstance] = []
    
    def extract_icts(self) -> Dict[str, ICTInstance]:
        """
        Extrair instituições únicas.
        
        Deduplicação: Quando vemos CD_ENTIDADE_CAPES repetida,
        mantemos apenas a primeira ocorrência (dados são iguais).
        
        Exemplo:
            CD_ENTIDADE_CAPES=25002 (UFRPE) aparece 45 vezes no dataset
            → Criamos apenas 1 ICTInstance com id="ict_25002"
        
        Retorna:
            Dict[str, ICTInstance]: {id → instância}
        """
        logger.info("Extracting ICT (Institution) instances...")
        
        # Manter apenas primeira ocorrência de cada CD_ENTIDADE_CAPES
        unique_icts = self.dataframe.drop_duplicates(
            subset=['CD_ENTIDADE_CAPES'],
            keep='first'
        )
        
        # Iterar e criar instâncias
        for _, row in unique_icts.iterrows():
            ict_id = f"ict_{row['CD_ENTIDADE_CAPES']}"
            self.ict_instances[ict_id] = ICTInstance(
                id=ict_id,
                cd_entidade_capes=str(row['CD_ENTIDADE_CAPES']),
                sg_entidade_ensino=str(row['SG_ENTIDADE_ENSINO']),
                nm_entidade_ensino=str(row['NM_ENTIDADE_ENSINO']),
                sg_uf=str(row['SG_UF_PROGRAMA'])
            )
        
        logger.info(f"  Extracted {len(self.ict_instances)} unique ICT instances")
        return self.ict_instances
    
    def extract_ppgs(self) -> Dict[str, PPGInstance]:
        """
        Extrair programas únicos.
        
        Deduplicação: Drop_duplicates por CD_PROGRAMA_IES.
        
        Importante: PPG pode ter MESMA NOME em mesma ICT se
        modalidade diferente (ACADÊMICO vs PROFISSIONAL).
        Por isso usamos CD_PROGRAMA_IES (código único).
        
        Também armazenamos ict_id para criar relação sediado.
        
        Retorna:
            Dict[str, PPGInstance]: {id → instância}
        """
        logger.info("Extracting PPG (Postgraduate Program) instances...")
        
        unique_ppgs = self.dataframe.drop_duplicates(
            subset=['CD_PROGRAMA_IES'],
            keep='first'
        )
        
        for _, row in unique_ppgs.iterrows():
            ppg_id = f"ppg_{row['CD_PROGRAMA_IES']}"
            ict_id = f"ict_{row['CD_ENTIDADE_CAPES']}"  # Referência!
            
            self.ppg_instances[ppg_id] = PPGInstance(
                id=ppg_id,
                cd_programa_ies=str(row['CD_PROGRAMA_IES']),
                nm_programa_ies=str(row['NM_PROGRAMA_IES']),
                nm_modalidade_programa=str(row['NM_MODALIDADE_PROGRAMA']),
                nm_area_conhecimento=str(row['NM_AREA_CONHECIMENTO']),
                ict_id=ict_id  # Relacionamento: PPG sediado em UFRPE ou outra ICT
            )
        
        logger.info(f"  Extracted {len(self.ppg_instances)} unique PPG instances")
        return self.ppg_instances
    
    def extract_conceitos(self) -> List[ConceituPPGInstance]:
        """
        Extrair avaliações (conceitos).
        
        ⚠️ IMPORTANTE: NÃO deduplicamos aqui!
        
        Motivo: Um PPG pode ser avaliado múltiplas vezes (anos diferentes).
        Cada avaliação = instância diferente.
        
        Exemplo:
            PPG_EDUCACAO_UFRPE avaliado:
            • 2021 conceito 5 → conceito_25002019_2021
            • 2022 conceito 5 → conceito_25002019_2022
            • 2023 conceito 5 → conceito_25002019_2023
        
        São 3 instâncias diferentes!
        
        Retorna:
            List[ConceituPPGInstance]: Lista de todas as avaliações
        """
        logger.info("Extracting Conceito_PPG (CAPES Evaluation) instances...")
        
        # Não deduplicar - todos os registros viram instâncias
        for _, row in self.dataframe.iterrows():
            conceito_id = (
                f"conceito_{row['CD_PROGRAMA_IES']}_"
                f"{int(row['AN_BASE'])}"
            )
            
            self.conceito_instances.append(ConceituPPGInstance(
                id=conceito_id,
                cd_programa_ies=str(row['CD_PROGRAMA_IES']),
                cd_conceito_programa=str(row['CD_CONCEITO_PROGRAMA']),
                an_base_conceito=int(row['AN_BASE'])
            ))
        
        logger.info(f"  Extracted {len(self.conceito_instances)} "
                   f"Conceito_PPG instances")
        return self.conceito_instances
    
    def get_summary(self) -> Dict:
        """
        Retornar resumo de instâncias extraídas.
        
        Retorna:
            Dict: Contagens de cada tipo
        """
        return {
            'ict_count': len(self.ict_instances),
            'ppg_count': len(self.ppg_instances),
            'conceito_count': len(self.conceito_instances),
            'total_rows_processed': len(self.dataframe)
        }


# ============================================================================
# CLASSE 3: OMLGenerator - Geração de Arquivo OML
# ============================================================================

class OMLGenerator:
    """
    Gerador de arquivo OML (Ontological Modeling Language).
    
    Responsabilidades:
    ✓ Formatar instâncias em sintaxe OML
    ✓ Definir relações semânticas
    ✓ Salvar arquivo
    
    Arquivo OML unificado:
        Todas as instâncias (ICT, PPG, Conceito) em 1 arquivo.
        Evita problemas de referências cruzadas entre descrições.
    """
    
    def __init__(self, output_dir: Path):
        """
        Inicializar com diretório de saída.
        
        Args:
            output_dir: Caminho para guardar arquivos OML
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _escape_oml_string(value: str) -> str:
        """
        Escapar caracteres especiais em strings OML.
        
        OML é texto estruturado, então alguns caracteres
        causam problemas (quebram parser):
        • Aspas duplas (")
        
        Args:
            value: String a escapar
            
        Retorna:
            String com caracteres escapados
        """
        value = str(value).replace('"', '\\"')
        return value
    
    def generate_cti_pe_description(
        self,
        ict_instances: Dict[str, ICTInstance],
        ppg_instances: Dict[str, PPGInstance],
        conceito_instances: List[ConceituPPGInstance]
    ) -> str:
        """
        Gerar arquivo OML unificado para CT&I Pernambuco.
        
        Estrutura:
        • Use statements (prefixos DNS)
        • ICT instances
        • PPG instances (com referências a ICT via "sediado")
        • Conceito instances (referenciadas por PPG via "avaliado")
        
        Args:
            ict_instances: Dict de ICT
            ppg_instances: Dict de PPG
            conceito_instances: List de Conceitos
        
        Retorna:
            str: Conteúdo OML pronto para escrever em arquivo
        """
        logger.info("Generating unified CT&I description file (cti-pe.oml)...")
        
        # Pré-processar: agrupar conceitos por programa
        conceitos_by_ppg = defaultdict(list)
        for conceito in conceito_instances:
            conceitos_by_ppg[conceito.cd_programa_ies].append(conceito)
        
        # Começar construindo o arquivo linha-por-linha
        lines = [
            '@dc:description "Descrição de elementos de Ciência, Tecnologia e Inovação em Pernambuco"',
            f'description <{CTI_PE_DESCRIPTION_URI}#> as cti-pe {{',
            '',
            f'\tuses <{DC_URI}> as {DC_NAMESPACE}',
            f'\tuses <{VOCABULARY_URI}#> as {VOCABULARY_NAMESPACE}',
            '',
            '\t// =====================================================================',
            '\t// INSTITUIÇÕES DE CIÊNCIA E TECNOLOGIA (ICT)',
            '\t// =====================================================================',
        ]
        
        # Adicionar instâncias de ICT
        for ict in ict_instances.values():
            lines.extend([
                '',
                f'\tinstance {ict.id} : {VOCABULARY_NAMESPACE}:ICT [',
                f'\t\t{VOCABULARY_NAMESPACE}:cd_entidade_capes "{ict.cd_entidade_capes}"',
                f'\t\t{VOCABULARY_NAMESPACE}:sg_entidade_ensino "{ict.sg_entidade_ensino}"',
                f'\t\t{VOCABULARY_NAMESPACE}:nm_entidade_ensino "{self._escape_oml_string(ict.nm_entidade_ensino)}"',
                f'\t\t{VOCABULARY_NAMESPACE}:sg_uf "{ict.sg_uf}"',
                '\t]',
            ])
        
        # Seção de PPG
        lines.extend([
            '',
            '\t// =====================================================================',
            '\t// PROGRAMAS DE PÓS-GRADUAÇÃO (PPG)',
            '\t// Relacionados a ICT via "sediado"',
            '\t// Relacionados a Conceitos via "avaliado"',
            '\t// =====================================================================',
        ])
        
        # Adicionar instâncias de PPG
        for ppg in ppg_instances.values():
            lines.append('')
            lines.append(f'\tinstance {ppg.id} : {VOCABULARY_NAMESPACE}:PPG [')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:cd_programa_ies "{ppg.cd_programa_ies}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_programa_ies "{self._escape_oml_string(ppg.nm_programa_ies)}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_modalidade_programa "{ppg.nm_modalidade_programa}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_area_conhecimento "{self._escape_oml_string(ppg.nm_area_conhecimento)}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:sediado {ppg.ict_id}')
            
            # Adicionar relações com CONCEITOS
            if ppg.cd_programa_ies in conceitos_by_ppg:
                conceitos = conceitos_by_ppg[ppg.cd_programa_ies]
                for conceito in conceitos:
                    lines.append(f'\t\t{VOCABULARY_NAMESPACE}:avaliado {conceito.id}')
            
            lines.append('\t]')
        
        # Seção de Conceitos
        lines.extend([
            '',
            '\t// =====================================================================',
            '\t// CONCEITOS DE AVALIAÇÃO CAPES (Conceito_PPG)',
            '\t// Cada conceito é uma avaliação de um PPG em um ano',
            '\t// =====================================================================',
        ])
        
        # Adicionar instâncias de Conceito
        for conceito in conceito_instances:
            lines.extend([
                '',
                f'\tinstance {conceito.id} : {VOCABULARY_NAMESPACE}:Conceito_PPG [',
                f'\t\t{VOCABULARY_NAMESPACE}:cd_conceito_programa "{conceito.cd_conceito_programa}"',
                f'\t\t{VOCABULARY_NAMESPACE}:an_base_conceito {conceito.an_base_conceito}',
                '\t]',
            ])
        
        # Fechar arquivo
        lines.extend([
            '',
            '}',
        ])
        
        return '\n'.join(lines)
    
    def save_file(self, filename: str, content: str) -> Path:
        """
        Salvar conteúdo em arquivo.
        
        Args:
            filename: Nome do arquivo (ex: "cti-pe.oml")
            content: Conteúdo a escrever
            
        Retorna:
            Path: Caminho do arquivo criado
        """
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"Saved: {filepath}")
        return filepath


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def print_summary(summary: Dict) -> None:
    """
    Imprimir resumo formatado de instâncias extraídas.
    
    Args:
        summary: Dict com contagens
    """
    print("\n" + "=" * 70)
    print(" EXTRACTION SUMMARY")
    print("=" * 70)
    print(f"  ICT Instances (Unique Institutions):        {summary['ict_count']:,}")
    print(f"  PPG Instances (Unique Programs):             {summary['ppg_count']:,}")
    print(f"  Conceito Instances (Evaluations):            {summary['conceito_count']:,}")
    print(f"  ─" * 35)
    print(f"  Total Rows Processed:                        {summary['total_rows_processed']:,}")
    print("=" * 70 + "\n")


# ============================================================================
# FUNÇÃO PRINCIPAL: Orquestração do Pipeline
# ============================================================================

def main():
    """
    Executar o pipeline completo de processamento.
    
    Orquestra as 4 etapas:
    1. LEITURA: Concatenar CSVs
    2. FILTRAGEM: Normalizar e validar
    3. EXTRAÇÃO: Deduplicar instâncias
    4. GERAÇÃO: Criar arquivo OML
    
    Retorna:
        int: 0 se sucesso, 1 se erro
    """
    
    logger.info("=" * 70)
    logger.info(" CAPES to OML Converter - Didactic Pipeline")
    logger.info("=" * 70)
    
    try:
        # ────────────────────────────────────────────────────────────
        # ETAPA 1: Ler CSVs
        # ────────────────────────────────────────────────────────────
        logger.info("\n[STEP 1] Reading CAPES CSV files...")
        processor = CAPESProcessor()
        processor.read_csv_files()
        logger.info(f"  {len(processor.files_processed)} files processed:")
        for f in processor.files_processed:
            logger.info(f"    - {f}")
        
        # ────────────────────────────────────────────────────────────
        # ETAPA 2: Filtrar e normalizar
        # ────────────────────────────────────────────────────────────
        logger.info("\n[STEP 2] Filtering and normalizing data...")
        processor.filter_by_state("PE")
        processor.normalize_data()
        missing = processor.validate_data()
        
        # ────────────────────────────────────────────────────────────
        # ETAPA 3: Extrair instâncias
        # ────────────────────────────────────────────────────────────
        logger.info("\n[STEP 3] Extracting ontology instances...")
        extractor = InstanceExtractor(processor.dataframe)
        extractor.extract_icts()
        extractor.extract_ppgs()
        extractor.extract_conceitos()
        
        summary = extractor.get_summary()
        print_summary(summary)
        
        # ────────────────────────────────────────────────────────────
        # ETAPA 4: Gerar arquivo OML
        # ────────────────────────────────────────────────────────────
        logger.info("[STEP 4] Generating OML description file...")
        generator = OMLGenerator(OML_OUTPUT_DIR)
        
        # Gerar descrição unificada
        cti_pe_content = generator.generate_cti_pe_description(
            extractor.ict_instances,
            extractor.ppg_instances,
            extractor.conceito_instances
        )
        generator.save_file("cti-pe.oml", cti_pe_content)
        
        # Salvar dados processados
        processor.save_processed_data()
        
        # ────────────────────────────────────────────────────────────
        # Concluído!
        # ────────────────────────────────────────────────────────────
        logger.info("\n" + "=" * 70)
        logger.info(" PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        logger.info(f"\nGenerated file in: {OML_OUTPUT_DIR}")
        logger.info("  - cti-pe.oml (Complete CT&I description)")
        logger.info(f"\nGenerated data file in: {DATA_PROCESSED_DIR}")
        logger.info("  - capes_programas_pernambuco.csv (Processed data)")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Error during processing: {e}", exc_info=True)
        return 1


# ============================================================================
# PONTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
