# Consulta de Artigos SCOPUS - UFRPE

Este projeto automatiza a consulta de artigos científicos na base de dados **Scopus** (Elsevier) utilizando a API oficial. O objetivo é extrair metadados de publicações vinculadas à instituição.

## Filtros de Busca
O script `gic_main.py` está configurado para retornar artigos que atendam aos seguintes critérios:
- **Instituição:** Universidade Federal Rural de Pernambuco (UFRPE).
- **Área de Conhecimento:** Ciência da Computação (Computer Science - COMP).
- **Ano de Publicação:** 2025.

## Pré-requisitos e Configuração

### 1. Conexão de Rede (Importante)
Para que a consulta funcione e utilize a visão completa dos dados (`view: COMPLETE`), o computador que executa o script **precisa estar conectado à rede da UFRPE**. O Scopus utiliza o reconhecimento de IP institucional para autorizar o acesso aos dados detalhados.

### 2. Configuração do Arquivo .env
A autenticação com a API da Elsevier exige uma chave pessoal. 
1. Localize o arquivo `.env` na raiz do projeto.
2. Insira sua chave de API obtida no portal de desenvolvedores da Elsevier:
   ```env
   ELSEVIER_API_KEY=SUA_CHAVE_AQUI
   ```

### 2.1 Uso no VS Code com variáveis de ambiente
Este workspace agora possui configuração para o VS Code carregar o `.env`
automaticamente em execução e debug Python:

- `.vscode/settings.json` com `python.envFile=${workspaceFolder}/.env`
- `.vscode/launch.json` com `envFile=${workspaceFolder}/.env`

Assim, ao executar pelo Debug (`F5`) ou pelas configurações de lançamento,
as variáveis `ELSEVIER_API_KEY` e `SCOPUS_API_KEY` ficam disponíveis sem
export manual no terminal.

### 3. Instalação de Dependências
Certifique-se de ter as bibliotecas necessárias instaladas:
```bash
pip install requests python-dotenv
```

## Como Usar
Execute o arquivo principal para iniciar a coleta:
```bash
python gic_main.py
```
Os resultados serão salvos em um arquivo JSON formatado, como `artigos_ufrpe_computacao_2025.json`.