#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
  SCRIPT: Gerador de OML a partir de Dados CAPES (Discentes)
  
  Um pipeline didático de engenharia de dados e conhecimento que transforma
  dados tabulares (CSV) de DISCENTES da CAPES em ontologias estruturadas (OML).
  
  OBJETIVO:
    Demonstrar a extração não apenas de programas, mas de entidades físicas
    (Pessoas) e seus relacionamentos (vinculado, orientador).
  
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
# Ajuste este caminho para a pasta onde estão seus CSVs de discentes
DATA_INPUT_DIR = PROJECT_ROOT / "data" / "raw" / "capes" / "discentes"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OML_OUTPUT_DIR = PROJECT_ROOT / "src" / "oml" / "gic.ufrpe.br" / "cti" / "description"

VOCABULARY_URI = "http://gic.ufrpe.br/cti/vocabulary/cti"
VOCABULARY_NAMESPACE = "cti"
CTI_PE_DESCRIPTION_URI = "http://gic.ufrpe.br/cti/description/cti-pe"
DC_URI = "http://purl.org/dc/elements/1.1/"
DC_NAMESPACE = "dc"

# ============================================================================
# DATACLASSES: Estruturas de Dados Tipadas
# ============================================================================

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
class DocenteInstance:
    """Representa um Docente (extraído da coluna de Orientadores)"""
    id: str
    nm_pessoa: str

@dataclass
class DiscenteInstance:
    """Representa um Discente com seus atributos e relacionamentos."""
    id: str
    id_pessoa: int
    nm_pessoa: str
    an_nascimento: int
    nm_pais_nacionalidade: str
    ds_grau_academico_discente: str
    nm_situacao_discente: str
    qt_mes_titulacao: int
    vinculado_id: str      # Relacionamento com PPG
    orientador_id: str     # Relacionamento com Docente


# ============================================================================
# CLASSE 1: CAPESProcessor - Leitura, Filtragem, Limpeza de Dados
# ============================================================================

class CAPESProcessor:
    def __init__(self):
        self.dataframe = None
        self.processed_count = 0
        self.files_processed = []
        DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        
    def read_csv_files(self) -> pd.DataFrame:
        logger.info(f"Searching for CSV files in: {DATA_INPUT_DIR}")
        csv_files = list(DATA_INPUT_DIR.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"❌ Nenhum arquivo CSV encontrado em {DATA_INPUT_DIR}")
        
        dfs = []
        for csv_file in sorted(csv_files):
            logger.info(f"Reading: {csv_file.name}")
            try:
                df = pd.read_csv(
                    csv_file,
                    delimiter=';',
                    encoding='iso-8859-1',
                    dtype={'AN_BASE': str, 'ID_PESSOA': str},
                    low_memory=False
                )
                dfs.append(df)
                self.files_processed.append(csv_file.name)
                logger.info(f"  ✓ Loaded {len(df)} rows")
            except Exception as e:
                logger.error(f"  ✗ Error reading {csv_file.name}: {e}")
        
        if dfs:
            self.dataframe = pd.concat(dfs, ignore_index=True)
            logger.info(f"Total rows after concatenation: {len(self.dataframe)}")
        return self.dataframe
    
    def filter_by_state(self, state_code: str = "PE") -> pd.DataFrame:
        if self.dataframe is None:
            raise ValueError("❌ Nenhum dado carregado.")
        
        initial_count = len(self.dataframe)
        self.dataframe = self.dataframe[self.dataframe['SG_UF_PROGRAMA'] == state_code]
        filtered_count = len(self.dataframe)
        logger.info(f"Filtered data for state '{state_code}': {initial_count:,} → {filtered_count:,} rows")
        return self.dataframe
    
    def normalize_data(self):
        logger.info("Normalizing data...")
        # Remover espaços
        for col in self.dataframe.select_dtypes(include=['object']).columns:
            self.dataframe[col] = self.dataframe[col].str.strip()
        
        # Converter numéricos, tratando faltantes (NaN)
        cols_to_int = ['AN_BASE', 'AN_NASCIMENTO_DISCENTE', 'QT_MES_TITULACAO']
        for col in cols_to_int:
            if col in self.dataframe.columns:
                self.dataframe[col] = pd.to_numeric(self.dataframe[col], errors='coerce').astype('Int64')
                
        logger.info("Data normalization complete ✓")
    
    def validate_data(self) -> Dict[str, int]:
        logger.info("Validating data quality...")
        # Colunas críticas atualizadas para o CSV de Discentes
        critical_columns = [
            'CD_ENTIDADE_CAPES', 'CD_PROGRAMA_IES', 'AN_BASE',
            'ID_PESSOA', 'NM_DISCENTE'
        ]
        
        missing_summary = {}
        for col in critical_columns:
            if col in self.dataframe.columns:
                missing_count = self.dataframe[col].isna().sum()
                missing_summary[col] = missing_count
                if missing_count > 0:
                    logger.warning(f"  ⚠️  Missing values in {col}: {missing_count}")
        
        before_clean = len(self.dataframe)
        self.dataframe = self.dataframe.dropna(subset=critical_columns, how='any')
        after_clean = len(self.dataframe)
        
        if before_clean != after_clean:
            logger.info(f"Removed {before_clean - after_clean} rows with missing critical values")
        return missing_summary
    
    def save_processed_data(self) -> Path:
        output_file = DATA_PROCESSED_DIR / "capes_discentes_pernambuco.csv"
        self.dataframe.to_csv(output_file, sep=';', encoding='iso-8859-1', index=False)
        logger.info(f"  ✓ Saved processed data: {output_file}")
        return output_file


# ============================================================================
# CLASSE 2: InstanceExtractor - Deduplicação e Extração
# ============================================================================

class InstanceExtractor:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe
        self.ict_instances: Dict[str, ICTInstance] = {}
        self.ppg_instances: Dict[str, PPGInstance] = {}
        self.conceito_instances: List[ConceituPPGInstance] = []
        self.docente_instances: Dict[str, DocenteInstance] = {}
        self.discente_instances: Dict[str, DiscenteInstance] = {}
    
    def extract_icts(self):
        unique_icts = self.dataframe.drop_duplicates(subset=['CD_ENTIDADE_CAPES'], keep='first')
        for _, row in unique_icts.iterrows():
            ict_id = f"ict_{row['CD_ENTIDADE_CAPES']}"
            self.ict_instances[ict_id] = ICTInstance(
                id=ict_id, cd_entidade_capes=str(row['CD_ENTIDADE_CAPES']),
                sg_entidade_ensino=str(row['SG_ENTIDADE_ENSINO']),
                nm_entidade_ensino=str(row['NM_ENTIDADE_ENSINO']), sg_uf=str(row['SG_UF_PROGRAMA'])
            )
        logger.info(f"  Extracted {len(self.ict_instances)} unique ICTs")
    
    def extract_ppgs(self):
        unique_ppgs = self.dataframe.drop_duplicates(subset=['CD_PROGRAMA_IES'], keep='first')
        for _, row in unique_ppgs.iterrows():
            ppg_id = f"ppg_{row['CD_PROGRAMA_IES']}"
            ict_id = f"ict_{row['CD_ENTIDADE_CAPES']}"
            self.ppg_instances[ppg_id] = PPGInstance(
                id=ppg_id, cd_programa_ies=str(row['CD_PROGRAMA_IES']),
                nm_programa_ies=str(row['NM_PROGRAMA_IES']),
                nm_modalidade_programa=str(row['NM_MODALIDADE_PROGRAMA']),
                nm_area_conhecimento=str(row['NM_GRANDE_AREA_CONHECIMENTO']), ict_id=ict_id
            )
        logger.info(f"  Extracted {len(self.ppg_instances)} unique PPGs")
    
    def extract_conceitos(self):
        # Discentes CSV também contém conceitos. Vamos mantê-los se existirem.
        if 'CD_CONCEITO_PROGRAMA' not in self.dataframe.columns:
            return
            
        unique_conceitos = self.dataframe.drop_duplicates(subset=['CD_PROGRAMA_IES', 'AN_BASE'])
        for _, row in unique_conceitos.iterrows():
            if pd.isna(row['CD_CONCEITO_PROGRAMA']): continue
            conceito_id = f"conceito_{row['CD_PROGRAMA_IES']}_{int(row['AN_BASE'])}"
            self.conceito_instances.append(ConceituPPGInstance(
                id=conceito_id, cd_programa_ies=str(row['CD_PROGRAMA_IES']),
                cd_conceito_programa=str(row['CD_CONCEITO_PROGRAMA']), an_base_conceito=int(row['AN_BASE'])
            ))
        logger.info(f"  Extracted {len(self.conceito_instances)} Conceito_PPGs")

    def extract_pessoas(self):
        """Extrai Docentes (Orientadores) e Discentes"""
        logger.info("Extracting Pessoas (Docentes e Discentes)...")
        
        # 1. Extrair Orientadores (Docentes)
        # O CSV não tem ID para orientador, então usamos um hash seguro do nome
        df_orientadores = self.dataframe.dropna(subset=['NM_ORIENTADOR_PRINCIPAL']).drop_duplicates(subset=['NM_ORIENTADOR_PRINCIPAL'])
        for _, row in df_orientadores.iterrows():
            nome = str(row['NM_ORIENTADOR_PRINCIPAL'])
            doc_id = f"docente_{abs(hash(nome)) % 100000000}"
            self.docente_instances[doc_id] = DocenteInstance(id=doc_id, nm_pessoa=nome)
            
        # 2. Extrair Discentes
        unique_discentes = self.dataframe.drop_duplicates(subset=['ID_PESSOA', 'AN_BASE'], keep='last')
        for _, row in unique_discentes.iterrows():
            disc_id = f"discente_{row['ID_PESSOA']}"
            ppg_id = f"ppg_{row['CD_PROGRAMA_IES']}"
            
            # Recuperar ID do orientador gerado acima
            nome_orientador = row.get('NM_ORIENTADOR_PRINCIPAL')
            orientador_id = f"docente_{abs(hash(str(nome_orientador))) % 100000000}" if pd.notna(nome_orientador) else ""
            
            self.discente_instances[disc_id] = DiscenteInstance(
                id=disc_id,
                id_pessoa=int(row['ID_PESSOA']),
                nm_pessoa=str(row['NM_DISCENTE']),
                an_nascimento=row['AN_NASCIMENTO_DISCENTE'] if pd.notna(row['AN_NASCIMENTO_DISCENTE']) else 0,
                nm_pais_nacionalidade=str(row.get('NM_PAIS_NACIONALIDADE_DISCENTE', 'Brasil')),
                ds_grau_academico_discente=str(row['DS_GRAU_ACADEMICO_DISCENTE']),
                nm_situacao_discente=str(row['NM_SITUACAO_DISCENTE']),
                qt_mes_titulacao=row['QT_MES_TITULACAO'] if pd.notna(row['QT_MES_TITULACAO']) else 0,
                vinculado_id=ppg_id,
                orientador_id=orientador_id
            )
            
        logger.info(f"  Extracted {len(self.docente_instances)} Orientadores (Docentes)")
        logger.info(f"  Extracted {len(self.discente_instances)} Discentes")

    def get_summary(self) -> Dict:
        return {
            'ict_count': len(self.ict_instances),
            'ppg_count': len(self.ppg_instances),
            'conceito_count': len(self.conceito_instances),
            'docente_count': len(self.docente_instances),
            'discente_count': len(self.discente_instances),
            'total_rows_processed': len(self.dataframe)
        }


# ============================================================================
# CLASSE 3: OMLGenerator - Geração de Arquivo OML
# ============================================================================

class OMLGenerator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _escape_oml_string(value: str) -> str:
        return str(value).replace('"', '\\"')
    
    def generate_cti_pe_description(self, ext: InstanceExtractor) -> str:
        logger.info("Generating unified CT&I description file (cti-pe.oml)...")
        
        conceitos_by_ppg = defaultdict(list)
        for conceito in ext.conceito_instances:
            conceitos_by_ppg[conceito.cd_programa_ies].append(conceito)
        
        lines = [
            '@dc:description "Descrição de elementos de CT&I em Pernambuco (Inclui Discentes)"',
            f'description <{CTI_PE_DESCRIPTION_URI}#> as cti-pe {{',
            '',
            f'\tuses <{DC_URI}> as {DC_NAMESPACE}',
            f'\tuses <{VOCABULARY_URI}#> as {VOCABULARY_NAMESPACE}',
            '',
            '\t// =====================================================================',
            '\t// INSTITUIÇÕES DE CIÊNCIA E TECNOLOGIA (ICT)',
            '\t// =====================================================================',
        ]
        
        for ict in ext.ict_instances.values():
            lines.extend([
                '',
                f'\tinstance {ict.id} : {VOCABULARY_NAMESPACE}:ICT [',
                f'\t\t{VOCABULARY_NAMESPACE}:cd_entidade_capes "{ict.cd_entidade_capes}"',
                f'\t\t{VOCABULARY_NAMESPACE}:sg_entidade_ensino "{ict.sg_entidade_ensino}"',
                f'\t\t{VOCABULARY_NAMESPACE}:nm_entidade_ensino "{self._escape_oml_string(ict.nm_entidade_ensino)}"',
                f'\t\t{VOCABULARY_NAMESPACE}:sg_uf "{ict.sg_uf}"',
                '\t]',
            ])
        
        lines.extend(['', '\t// =====================================================================', '\t// PROGRAMAS DE PÓS-GRADUAÇÃO (PPG)', '\t// ====================================================================='])
        for ppg in ext.ppg_instances.values():
            lines.append('')
            lines.append(f'\tinstance {ppg.id} : {VOCABULARY_NAMESPACE}:PPG [')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:cd_programa_ies "{ppg.cd_programa_ies}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_programa_ies "{self._escape_oml_string(ppg.nm_programa_ies)}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_modalidade_programa "{ppg.nm_modalidade_programa}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_area_conhecimento "{self._escape_oml_string(ppg.nm_area_conhecimento)}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:sediado {ppg.ict_id}')
            if ppg.cd_programa_ies in conceitos_by_ppg:
                for conceito in conceitos_by_ppg[ppg.cd_programa_ies]:
                    lines.append(f'\t\t{VOCABULARY_NAMESPACE}:avaliado {conceito.id}')
            lines.append('\t]')
            
        lines.extend(['', '\t// =====================================================================', '\t// DOCENTES (ORIENTADORES)', '\t// ====================================================================='])
        for doc in ext.docente_instances.values():
            lines.extend([
                '',
                f'\tinstance {doc.id} : {VOCABULARY_NAMESPACE}:Docente [',
                f'\t\t{VOCABULARY_NAMESPACE}:nm_pessoa "{self._escape_oml_string(doc.nm_pessoa)}"',
                '\t]'
            ])

        lines.extend(['', '\t// =====================================================================', '\t// DISCENTES', '\t// ====================================================================='])
        for disc in ext.discente_instances.values():
            lines.append('')
            lines.append(f'\tinstance {disc.id} : {VOCABULARY_NAMESPACE}:Discente [')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:id_pessoa {disc.id_pessoa}')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_pessoa "{self._escape_oml_string(disc.nm_pessoa)}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:ds_grau_academico_discente "{disc.ds_grau_academico_discente}"')
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:nm_situacao_discente "{disc.nm_situacao_discente}"')
            
            if disc.an_nascimento > 0:
                lines.append(f'\t\t{VOCABULARY_NAMESPACE}:an_nascimento {disc.an_nascimento}')
            if disc.qt_mes_titulacao > 0:
                lines.append(f'\t\t{VOCABULARY_NAMESPACE}:qt_mes_titulacao {disc.qt_mes_titulacao}')
                
            lines.append(f'\t\t{VOCABULARY_NAMESPACE}:vinculado {disc.vinculado_id}')
            if disc.orientador_id:
                lines.append(f'\t\t{VOCABULARY_NAMESPACE}:orientador {disc.orientador_id}')
            lines.append('\t]')

        lines.extend(['', '}'])
        return '\n'.join(lines)
    
    def save_file(self, filename: str, content: str) -> Path:
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"Saved: {filepath}")
        return filepath

def print_summary(summary: Dict) -> None:
    print("\n" + "=" * 70)
    print(" EXTRACTION SUMMARY")
    print("=" * 70)
    print(f"  ICT Instances:            {summary['ict_count']:,}")
    print(f"  PPG Instances:            {summary['ppg_count']:,}")
    print(f"  Conceito Instances:       {summary['conceito_count']:,}")
    print(f"  Docente (Orientador):     {summary['docente_count']:,}")
    print(f"  Discente Instances:       {summary['discente_count']:,}")
    print(f"  ─" * 35)
    print(f"  Total Rows Processed:     {summary['total_rows_processed']:,}")
    print("=" * 70 + "\n")

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    logger.info("=" * 70)
    logger.info(" CAPES Discentes to OML Converter")
    logger.info("=" * 70)
    
    try:
        processor = CAPESProcessor()
        processor.read_csv_files()
        
        processor.filter_by_state("PE")
        processor.normalize_data()
        processor.validate_data()
        
        extractor = InstanceExtractor(processor.dataframe)
        extractor.extract_icts()
        extractor.extract_ppgs()
        extractor.extract_conceitos()
        extractor.extract_pessoas()
        
        print_summary(extractor.get_summary())
        
        generator = OMLGenerator(OML_OUTPUT_DIR)
        cti_pe_content = generator.generate_cti_pe_description(extractor)
        generator.save_file("cti-pe.oml", cti_pe_content)
        
        processor.save_processed_data()
        
        logger.info("\n" + "=" * 70 + "\n PIPELINE COMPLETED SUCCESSFULLY!\n" + "=" * 70)
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Error during processing: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
