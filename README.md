# CT&I-PE: Pipeline CAPES + Scopus para OML

Um pipeline modular em Python que processa dados da **CAPES (Coordenação de Aperfeiçoamento de Pessoal de Nível Superior)** do estado de **Pernambuco**, enriquece-os com métricas do **Scopus** e gera instâncias em formato **OML (OWL Modeling Language)** para a Comunidade de Prática de CT&I.

## 📋 Sobre o Projeto

Este repositório implementa a modelagem de dados de pesquisa (Ciência, Tecnologia e Inovação) do estado de Pernambuco em formato ontológico (OML/OWL). O pipeline:

1. **Extrai** dados de programas de pós-graduação, discentes e produções científicas da CAPES
2. **Filtra** informações referentes a Pernambuco (opcional: por instituição específica como UFRPE, UPE, UFPE, etc.)
3. **Enriquece** com métricas acadêmicas do Scopus (h-index, i10-index, citações)
4. **Gera** instâncias OML que representam entidades (ICT, PPG, Discentes, Autores, Produções)
5. **Exporta** para formato OML + arquivo de auditoria CSV

### Casos de Uso

- Análise de capacidade de pesquisa por instituição
- Consultas SPARQL sobre redes de produção científica
- Relatórios de produtividade acadêmica
- Integração com sistemas de gestão de informação

---

## 🚀 Quick Start

### 1. Clonar e preparar o ambiente

```bash
git clone <repo-url>
cd Projeto_Gestao_da_Informacao
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

### 2. Instalar dependências

```bash
python -m pip install -r requirements.txt
```

### 3. Configurar variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
SCOPUS_API_KEY=sua_chave_aqui
STATE_FILTER=PE
SITUATION_FILTER=TITULADO
SCOPUS_MODE=incremental
SCOPUS_MAX_ITEMS=100
```

### 4. Organizar dados CAPES

Copie os CSVs CAPES para:

```
data/raw/capes/
├── programas/
│   ├── br-capes-colsucup-prog-2021-2025-03-31.csv
│   ├── br-capes-colsucup-prog-2022-2025-03-31.csv
│   └── ...
├── discentes/
│   └── br-capes-colsucup-discentes-2022-2025-03-31.csv
├── autores/
│   └── br-capes-colsucup-prod-autor-2021a2024-2025-12-01-bibliografica-artpe-*.csv
└── producao/
    └── br-capes-colsucup-producao-2021a2024-2025-12-01-tecnica-*.csv
```

### 5. Rodar o pipeline

Execução completa (recomendado):

```bash
python scripts/generate_oml_cti_full.py --steps all --scopus-batch-size 100
```

Ou em etapas:

```bash
# CAPES: carregar e extrair
python scripts/generate_oml_cti_full.py --steps capes --capes-institution UFRPE

# Scopus: enriquecer com métricas (incremental)
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100

# OML: gerar arquivo e auditoria
python scripts/generate_oml_cti_full.py --steps oml
```

### 6. Verificar saídas

- **OML**: `src/oml/gic.ufrpe.br/cti/description/cti-pe.oml`
- **Auditoria**: `data/processed/cti_pe_audit.csv`
- **Cache Scopus**: `data/processed/scopus_cache.json`
- **Checkpoint**: `data/processed/scopus_checkpoint.json`

---

## 📁 Estrutura do Projeto

```
Projeto_Gestao_da_Informacao/
├── README.md                           # Descrição do projeto (este arquivo)
├── requirements.txt                    # Dependências Python
├── build.gradle                        # Config Gradle (build, relatórios)
├── catalog.xml                         # Catálogo de ontologias
├── .env                                # Variáveis de ambiente (local)
│
├── data/
│   ├── raw/
│   │   └── capes/                     # CSVs CAPES originais
│   │       ├── programas/
│   │       ├── discentes/
│   │       ├── autores/
│   │       └── producao/
│   └── processed/
│       ├── pipeline_state.pkl          # Estado intermediário (extrator OML)
│       ├── capes_programas_pernambuco.csv
│       ├── cti_pe_audit.csv            # Auditoria de instâncias
│       ├── scopus_cache.json           # Cache de respostas Scopus
│       ├── scopus_checkpoint.json      # Fila e progresso Scopus
│       ├── scopus_authors_processed.txt
│       └── scopus_dois_processed.txt
│
├── scripts/
│   ├── generate_oml_cti_full.py       # Ponto de entrada principal
│   ├── README.md                       # Documentação de uso do pipeline
│   └── oml_pipeline/                  # Pacote modular
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py                      # Interface de linha de comando
│       ├── config.py                   # Config, paths, env vars
│       ├── pipeline.py                 # Orquestração das etapas
│       ├── capes_io.py                 # Leitura e filtro CAPES
│       ├── extractor.py                # Extração de instâncias OML
│       ├── scopus.py                   # Enriquecimento Scopus
│       ├── oml_generator.py            # Geração do arquivo OML
│       ├── state.py                    # Persistência de estado (pickle)
│       ├── models.py                   # Classes de domínio
│       └── utils.py                    # Funções utilitárias
│
├── src/
│   └── oml/gic.ufrpe.br/cti/
│       └── description/
│           ├── cti-pe.oml              # Arquivo OML final
│           └── cti.oml                 # (Ontologia base)
│
└── docs/
    ├── preparacao.md
    ├── tutorial1-cti.md
    ├── tutorial2-sirius-cti.md
    └── images/
```

---

## 🔧 Opções de CLI

### Grupos de Etapas

- `capes` — Carrega CSVs CAPES, extrai instâncias, valida integridade, salva estado
- `scopus` — Carrega estado, enriquece autores/produções com Scopus em lotes
- `oml` — Carrega estado, gera OML + CSV auditoria
- `all` — Executa tudo em sequência (padrão)

Exemplo:

```bash
python scripts/generate_oml_cti_full.py --steps capes,oml
```

### Flags Principais

#### CAPES

- `--capes-institution NOME` — Filtro opcional por instituição (ex: UFRPE, UPE)
  - Permite extrair dados de uma instituição e acumular com outras em execuções sucessivas
  - Sem flag, processa todas as instituições de Pernambuco

#### Scopus

- `--scopus-batch-size N` — Número de autores/DOIs por execução (padrão: 100)
- `--scopus-mode incremental|full` — incremental usa checkpoint, full reprocessa tudo
- `--scopus-reset-progress` — Limpa cache e checkpoint (começa do zero)
- `--scopus-max-retries N` — Retries por requisição
- `--scopus-backoff-base SEGUNDOS` — Backoff inicial para retry
- `--scopus-backoff-max SEGUNDOS` — Backoff máximo
- `--scopus-priority-ict SIGLA` — Prioriza coleta de uma ICT (ex: UFRPE)

---

## 📖 Fluxos de Uso

### Fluxo 1: Geração completa (UFRPE)

```bash
# Limpar (opcional)
rm data/processed/pipeline_state.pkl

# Carregar CAPES apenas de UFRPE
python scripts/generate_oml_cti_full.py --steps capes --capes-institution UFRPE

# Enriquecer com Scopus (incremental, 500 por vez)
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 500

# Gerar OML final
python scripts/generate_oml_cti_full.py --steps oml
```

### Fluxo 2: Acumular múltiplas instituições

```bash
# Primeira instituição
python scripts/generate_oml_cti_full.py --steps capes --capes-institution UFRPE

# Adicionar segunda instituição
python scripts/generate_oml_cti_full.py --steps capes --capes-institution UPE

# Adicionar terceira
python scripts/generate_oml_cti_full.py --steps capes --capes-institution UFPE

# Enriquecer todas juntas
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100

# Gerar OML com todas as instituições
python scripts/generate_oml_cti_full.py --steps oml
```

### Fluxo 3: Continuar enriquecimento após interrução

Se você interrompeu com `Ctrl+C`, o progresso foi salvo:

```bash
# Continua exatamente de onde parou
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100
```

---

## 🔐 Configuração de Variáveis de Ambiente

Arquivo `.env` (criar na raiz):

```env
# Obrigatório para Scopus
SCOPUS_API_KEY=<sua-chave-api-elsevier>

# Filtro de estado (padrão: PE)
STATE_FILTER=PE

# Situação do discente (padrão: TITULADO)
SITUACAO_FILTER=TITULADO

# Modo Scopus
SCOPUS_MODE=incremental

# Limite padrão de itens Scopus
SCOPUS_MAX_ITEMS=100

# Retries e backoff Scopus
SCOPUS_MAX_RETRIES=3
SCOPUS_BACKOFF_BASE_SECONDS=2.0
SCOPUS_BACKOFF_MAX_SECONDS=120.0

# Delay entre requisições Scopus (segundos)
SCOPUS_DELAY=0.15
```

---

## 📊 Arquitetura

### Componentes Principais

#### 1. **CLI (`cli.py`)**
   - Parser de argumentos
   - Validação de flags
   - Logging configurável

#### 2. **Pipeline (`pipeline.py`)**
   - Orquestração das etapas (load, extract, scopus, oml)
   - Grupos de etapas (capes, scopus, oml, all)
   - Gerenciamento de estado

#### 3. **CAPES IO (`capes_io.py`)**
   - Leitura de CSVs CAPES
   - Filtro por UF (estado)
   - Filtro opcional por instituição
   - Suporte a leitura em chunks (memória eficiente)

#### 4. **Extractor (`extractor.py`)**
   - Extração de instâncias OML:
     - ICT (Instituição de Ciência e Tecnologia)
     - PPG (Programa de Pós-Graduação)
     - Discente
     - Autor
     - Produção Científica
     - Veículo de Publicação
     - Citação
   - Validação de integridade referencial
   - Merge incremental de estados

#### 5. **Scopus (`scopus.py`)**
   - Cliente da API Elsevier/Scopus
   - Busca e recuperação de Scopus ID
   - Enriquecimento de métricas (h-index, i10, citações)
   - Checkpoint incremental com fila de retry
   - Tratamento de rate limits (HTTP 429)
   - Cache de resultados (JSON)

#### 6. **OML Generator (`oml_generator.py`)**
   - Serialização de instâncias para OML/XML
   - Namespaces Dublin Core e customizados
   - Geração de arquivo final

#### 7. **State (`state.py`)**
   - Persistência de estado via pickle
   - Carregamento e salvamento de InstanceExtractor

#### 8. **Config (`config.py`)**
   - Caminhos de dados
   - Mapeamento de colunas CAPES
   - Variáveis de ambiente
   - Constantes de configuração

---

## 🔄 Checkpoint e Recuperação (Scopus)

O pipeline mantém automaticamente um checkpoint de progresso:

```
data/processed/scopus_checkpoint.json
{
  "version": 1,
  "author_done": [lista de autores processados com sucesso],
  "author_pending": [fila de autores pendentes],
  "author_failures": {autor_id: contador_falhas},
  "doi_done": [...],
  "doi_pending": [...],
  "doi_failures": {...}
}
```

### Comportamento

- **Incremental** (padrão): Retoma exatamente de onde parou
- **Full**: Reprocessa tudo do início (ignora checkpoint)
- **Reset**: Limpa cache e checkpoint, começa do zero

### Falhas e Retry

- Autores com **2+ falhas** são removidos da fila (configurável em `scopus.py`)
- HTTP 429 (Too Many Requests) para automaticamente após 3 erros consecutivos
- Backoff exponencial com jitter para retries

---

## 📌 Troubleshooting

### Erro: "SCOPUS_API_KEY não configurada"

```bash
# Criar/verificar .env
cat .env | grep SCOPUS_API_KEY

# Ou setar inline
$env:SCOPUS_API_KEY="sua-chave"
```

### Erro: "HTTP 429 — Too Many Requests"

- Reduzir `--scopus-batch-size` (ex: 50 ao invés de 100)
- Aumentar `--scopus-backoff-max` (ex: 300 segundos)
- Executar em rede autorizada da UFRPE para evitar rate limiting

### Erro: "CSV não encontrado"

Verificar estrutura de `data/raw/capes/`:

```bash
ls data/raw/capes/programas/
ls data/raw/capes/discentes/
ls data/raw/capes/autores/
ls data/raw/capes/producao/
```

### Interrupção durante Scopus (Ctrl+C)

- **Seguro?** Sim, não corrompe o arquivo de checkpoint
- **Dados perdidos?** O batch em andamento pode ter dados perdidos, mas o anterior está salvo
- **Retomar?** Execute o mesmo comando novamente para continuar

---

## 📈 Performance e Otimizações

### Reduzir uso de memória

```bash
# Usar chunks para CAPES (apenas discentes)
# Feito automaticamente em capes_io.py

# Reduzir batch Scopus
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 25
```

### Monitoramento

Verifique logs em tempo real:

```bash
# Linux/Mac
tail -f <output.log>

# Windows PowerShell
Get-Content output.log -Wait
```

---

## 📚 Referências

- [CAPES - Portal de Dados](http://dadosabertos.capes.gov.br/)
- [Scopus API Documentation](https://dev.elsevier.com/sc_apis.html)
- [OML/OWL Specifications](https://www.w3.org/OWL/)
- [Sirius - Viewpoint Language](http://www.obeodesign.com/sirius)

---

**Última atualização**: Maio de 2026  
**Versão**: 1.0.0

