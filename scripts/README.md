# Pipeline CAPES + Scopus para OML (CT&I-PE)

Este diretório contém o pipeline modular que lê dados CAPES, filtra Pernambuco,
enriquece dados com Scopus (opcional) e gera instâncias OML.

## Estrutura de entrada obrigatória

Os arquivos CAPES devem estar organizados assim:

```text
data/
  raw/
    capes/
      programas/
        br-capes-colsucup-prog-2021-2025-03-31.csv
        br-capes-colsucup-prog-2022-2025-03-31.csv
        br-capes-colsucup-prog-2023-2025-03-31.csv
        br-capes-colsucup-prog-2024-2025-12-01.csv
      discentes/
        br-capes-colsucup-discentes-2022-2025-03-31.csv
      autores/
        br-capes-colsucup-prod-autor-2021a2024-2025-12-01-bibliografica-artpe-2023.csv
      producao/
        br-capes-colsucup-producao-2021a2024-2025-12-01-tecnica-cursocdu.csv
```

## Ponto de entrada

```bash
python scripts/generate_oml_cti_full.py
```

O arquivo `generate_oml_cti_full.py` agora é um launcher da CLI modular em
`scripts/oml_pipeline/`.

> Observação: o script legado `generate_oml_discentes.py` ficou fora do fluxo
> ativo. O pipeline atual não extrai nem grava docentes/orientadores.

## Passo a passo de execução

### 1. Preparar ambiente

1. Garanta que os CSVs CAPES estejam nas subpastas corretas.
2. Configure o `.env` na raiz do projeto.
3. Instale dependências Python (se necessário):

```bash
python -m pip install pandas requests python-dotenv
```

### 2. Processar CAPES e gerar estado intermediário

```bash
python scripts/generate_oml_cti_full.py --steps capes
```

Resultado esperado:

- Geração de `data/processed/pipeline_state.pkl`
- Resumo de extração (ICT, PPG, Discente, Autor, etc.)

### 3. Enriquecer com Scopus

Execução incremental recomendada (processa 100 autores/DOIs por vez):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100
```

Execução full (reprocessa tudo sem usar checkpoint):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode full --scopus-batch-size 100
```

Reset de cache/checkpoint (começa do zero):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-reset-progress --scopus-batch-size 100
```

### 4. Gerar OML e auditoria

```bash
python scripts/generate_oml_cti_full.py --steps oml
```

Resultado esperado:

- `src/oml/gic.ufrpe.br/cti/description/cti-pe.oml`
- `data/processed/cti_pe_audit.csv`

### 5. Rodar tudo em sequência

```bash
python scripts/generate_oml_cti_full.py --steps all --scopus-batch-size 100
```

### 6. Verificação rápida de saída

1. Confirme se existem os arquivos:
  - `data/processed/pipeline_state.pkl`
  - `data/processed/scopus_cache.json`
  - `data/processed/scopus_checkpoint.json`
  - `data/processed/scopus_authors_processed.txt`
  - `data/processed/scopus_dois_processed.txt`
  - `data/processed/cti_pe_audit.csv`
  - `src/oml/gic.ufrpe.br/cti/description/cti-pe.oml`
2. Verifique logs para possíveis avisos de API (`401`, `403`, `429`).

## Etapas do pipeline

- `capes`: carrega e extrai CAPES, valida integridade e salva estado.
- `scopus`: carrega estado salvo e enriquece autores/produções com Scopus em lotes.
- `oml`: carrega estado salvo e gera OML + auditoria CSV.
- `all`: executa todo o fluxo em sequência.

Exemplos:

```bash
python scripts/generate_oml_cti_full.py --steps capes
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100
python scripts/generate_oml_cti_full.py --steps oml
python scripts/generate_oml_cti_full.py --steps capes,oml
python scripts/generate_oml_cti_full.py --steps all --scopus-batch-size 100
```

## Tamanho de lote do Scopus (batch size)

Enriquecimento Scopus processa em lotes para melhor controle e recuperação de falhas.
Padrão: `100` itens por execução.

Altere por argumento de linha de comando:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 50
```

Ou por variável de ambiente:

```bash
export SCOPUS_MAX_ITEMS=200
python scripts/generate_oml_cti_full.py --steps scopus
```

## Modo incremental com checkpoint e listas de progresso

Scopus agora rastreia progresso em JSON (checkpoint) e texto legível (listas).

### Como funciona

- Processa **lote por lote** (ex: 100 itens, depois próximos 100)
- Salva fila pendente em `scopus_checkpoint.json`
- Evita reprocessar via cache em `scopus_cache.json`
- Gera listas legíveis em `.txt` para inspeção rápida
- Retoma exatamente de onde parou na última execução

### Arquivos gerados em `data/processed/`

- `scopus_cache.json` — cache de resultados API (JSON)
- `scopus_checkpoint.json` — fila e status (JSON estruturado)
- `scopus_authors_processed.txt` — autores enriquecidos (legível)
- `scopus_dois_processed.txt` — DOIs consultados (legível)

### Opções de CLI

- `--scopus-batch-size N` — itens por execução (padrão: 100)
- `--scopus-mode incremental|full` — incremental usa checkpoint, full reprocessa tudo
- `--scopus-reset-progress` — limpa cache e checkpoint (começa do zero)
- `--scopus-max-retries N` — retries por requisição
- `--scopus-backoff-base SEGUNDOS` — backoff inicial
- `--scopus-backoff-max SEGUNDOS` — backoff máximo

### Exemplos práticos

Primeira execução (processa 100):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100
```

Segunda execução (próximos 100):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 100
```

Pequenos lotes (recomendado se API com limite baixo):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-batch-size 50
```

Reprocessar tudo sem checkpoint:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode full --scopus-batch-size 100
```

Resetar e começar do zero:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-reset-progress --scopus-batch-size 50
```

### Inspecionando progresso

Ver quantos autores já foram processados:

```bash
wc -l data/processed/scopus_authors_processed.txt
```

Ver primeiros 10 autores enriquecidos:

```bash
head -15 data/processed/scopus_authors_processed.txt
```

Verificar quantos DOIs já foram consultados:

```bash
grep -c "^prod_" data/processed/scopus_dois_processed.txt
```

Ver fila pendente (JSON):

```bash
cat data/processed/scopus_checkpoint.json | grep -A 5 "author_pending"
```

## Variáveis de ambiente relevantes

- `SCOPUS_API_KEY`: chave da API Elsevier.
- `STATE_FILTER`: UF para filtro CAPES (padrão: `PE`).
- `SITUACAO_FILTER`: situação do discente (padrão: `TITULADO`).
- `SCOPUS_MAX_ITEMS`: limite padrão de itens para enriquecimento Scopus.
- `SCOPUS_MODE`: modo padrão (`incremental` ou `full`).
- `SCOPUS_MAX_RETRIES`: retries por requisição Scopus.
- `SCOPUS_BACKOFF_BASE_SECONDS`: backoff base para retries.
- `SCOPUS_BACKOFF_MAX_SECONDS`: backoff máximo para retries.

## Saídas

- OML: `src/oml/gic.ufrpe.br/cti/description/cti-pe.oml`
- Estado intermediário: `data/processed/pipeline_state.pkl`
- Cache Scopus: `data/processed/scopus_cache.json`
- Checkpoint Scopus: `data/processed/scopus_checkpoint.json`
- Lista de autores enriquecidos: `data/processed/scopus_authors_processed.txt`
- Lista de DOIs consultados: `data/processed/scopus_dois_processed.txt`
- Auditoria: `data/processed/cti_pe_audit.csv`

## Arquitetura do pacote

`scripts/oml_pipeline/`:

- `config.py`: caminhos, variáveis de ambiente e mapeamento de colunas.
- `capes_io.py`: leitura de CSVs e carga dos datasets CAPES.
- `extractor.py`: extração das instâncias de domínio.
- `scopus.py`: cliente de enriquecimento Scopus com limite configurável.
- `oml_generator.py`: geração do arquivo OML.
- `state.py`: persistência e carga do estado intermediário.
- `pipeline.py`: orquestração das etapas.
- `cli.py`: interface de linha de comando.
