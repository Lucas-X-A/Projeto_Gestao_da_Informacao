# Gerador de OML a partir de dados da CAPES

Script para transformar dados de programas de pós-graduação da CAPES (CSV) em instâncias OML.

A proposta é mostrar, de forma prática, como dados tabulares podem ser convertidos em um grafo de conhecimento seguindo um modelo ontológico.

---

## 📂 Estrutura esperada

```
data/
  raw/capes/         # arquivos CSV da CAPES
  processed/         # saída intermediária (gerada automaticamente)

src/oml/.../description/
  cti-pe.oml         # saída final

scripts/
  generate_oml_from_capes.py
```

---

## 🚀 Execução

```bash
source venv/bin/activate
pip install pandas
python scripts/generate_oml_from_capes.py
```

---

## 🔄 O que o script faz

O script implementa um pipeline simples dividido em quatro etapas:

### 1. Leitura dos dados
- Carrega automaticamente todos os CSVs da pasta `data/raw/capes/`
- Consolida os arquivos em um único dataset

### 2. Filtragem e limpeza
- Filtra apenas programas do estado de Pernambuco (PE)
- Remove espaços em branco e ajusta tipos de dados
- Elimina registros com informações críticas ausentes

### 3. Extração de instâncias
A partir do dataset, são criados três tipos de entidades:

- **ICT**: instituições (deduplicadas por código CAPES)
- **PPG**: programas de pós-graduação (deduplicados)
- **Conceito_PPG**: avaliações (uma por programa e ano)

Também são estabelecidas relações:
- `sediado`: PPG → ICT  
- `avaliado`: PPG → Conceito  

### 4. Geração do OML
- As instâncias são convertidas para o formato OML
- O arquivo final é gerado em:
  `src/oml/.../description/cti-pe.oml`

---

## 📊 Saída

Arquivos gerados:

- `cti-pe.oml` → descrição com instâncias e relações  
- `capes_programas_pernambuco.csv` → dados já filtrados  

Resumo típico:

```
ICT: 15
PPG: 178
Conceitos: 674
```

---

## 🧠 Organização do código

O script é dividido em três partes principais:

- **CAPESProcessor**  
  Responsável por leitura, filtragem e limpeza dos dados

- **InstanceExtractor**  
  Cria as instâncias e resolve deduplicação

- **OMLGenerator**  
  Gera o arquivo OML com base nas instâncias e relações

---

## 🔍 Exemplo simplificado

```oml
instance ppg_123 : cti:PPG [
  cti:nm_programa_ies "EDUCAÇÃO"
  cti:sediado ict_1
  cti:avaliado conceito_123_2023
]
```

---

## ⚠️ Observações

- Os CSVs da CAPES usam encoding `ISO-8859-1`
- O filtro por estado está definido no script (`PE`)
- Os identificadores são derivados dos códigos da CAPES

---

## 🔧 Problemas comuns

- CSV não encontrado → verificar `data/raw/capes/`
- erro de encoding → manter padrão CAPES
- pandas não instalado → `pip install pandas`

---

## 📌 Extensões possíveis

- incluir outros estados  
- filtrar por ano ou conceito  
- separar em múltiplos arquivos OML  
- integrar com APIs externas (ex: Scopus)  
