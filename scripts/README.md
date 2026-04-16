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

Execução incremental recomendada:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode incremental --scopus-limit 200
```

Execução full (reprocessa sem usar checkpoint):

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode full --scopus-limit 200
```

Reset de cache/checkpoint antes da rodada:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-reset-progress --scopus-limit 200
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
python scripts/generate_oml_cti_full.py --steps all --scopus-mode incremental --scopus-limit 200
```

### 6. Verificação rápida de saída

1. Confirme se existem os arquivos:
  - `data/processed/pipeline_state.pkl`
  - `data/processed/scopus_cache.json`
  - `data/processed/scopus_checkpoint.json`
  - `data/processed/cti_pe_audit.csv`
  - `src/oml/gic.ufrpe.br/cti/description/cti-pe.oml`
2. Verifique logs para possíveis avisos de API (`401`, `403`, `429`).

## Etapas do pipeline

- `capes`: carrega e extrai CAPES, valida integridade e salva estado.
- `scopus`: carrega estado salvo e enriquece autores e produções com Scopus.
- `oml`: carrega estado salvo e gera OML + auditoria CSV.
- `all`: executa todo o fluxo.

Exemplos:

```bash
python scripts/generate_oml_cti_full.py --steps capes
python scripts/generate_oml_cti_full.py --steps scopus
python scripts/generate_oml_cti_full.py --steps oml
python scripts/generate_oml_cti_full.py --steps capes,oml
python scripts/generate_oml_cti_full.py --steps all
```

## Limite de enriquecimento Scopus

O enriquecimento Scopus aplica limite em duas frentes:

- quantidade máxima de produções por DOI processadas
- quantidade máxima de autores processados

Valor padrão: `100`.

Você pode alterar por argumento de linha de comando:

```bash
python scripts/generate_oml_cti_full.py --steps scopus --scopus-limit 100
```

## Modo robusto do Scopus (incremental)

O enriquecimento Scopus agora suporta execução incremental com checkpoint,
cache local e retry com backoff adaptativo.

### Como funciona

- Mantém fila pendente entre execuções.
- Evita repetir chamadas já resolvidas via cache.
- Retoma processamento de onde parou.
- Aplica retries para erros transitórios (`429`, `5xx`, rede).

Arquivos gerados em `data/processed/`:

- `scopus_cache.json`
- `scopus_checkpoint.json`

### Opções de CLI

- `--scopus-mode incremental|full`
- `--scopus-limit N`
- `--scopus-reset-progress`
- `--scopus-max-retries N`
- `--scopus-backoff-base SEGUNDOS`
- `--scopus-backoff-max SEGUNDOS`

Exemplos:

```bash
# Incremental (recomendado)
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode incremental --scopus-limit 200

# Full (reprocessa sem depender do checkpoint)
python scripts/generate_oml_cti_full.py --steps scopus --scopus-mode full --scopus-limit 200

# Reset de cache/checkpoint + execução incremental
python scripts/generate_oml_cti_full.py --steps scopus --scopus-reset-progress --scopus-limit 100
```

Ou por variável de ambiente:

```text
SCOPUS_MAX_ITEMS=100
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
