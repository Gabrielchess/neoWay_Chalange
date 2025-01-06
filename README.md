# Desafio Técnico - Data Quality Management Strategy

## Objetivo

Esse repositório contém a solução para o desafio técnico da vaga de Analista de Qualidade de Dados III. O objetivo deste desafio foi apresentar minha experiência em qualidade de dados, criando uma solução para garantir a padronização e visibilidade dos dados, além de realizar uma análise de qualidade em uma base de dados.

O desafio foi dividido em duas partes:

1. **Parte 1**: Proposta de solução para a gestão da qualidade de dados integrada entre empresas com diferentes plataformas e arquiteturas.
2. **Parte 2**: Análise da qualidade dos dados de uma base fornecida, com base nas cinco dimensões de qualidade (Preenchimento, Padronização, Consistência, Unicidade e Abrangência).

## Parte 1: Solução para Gestão da Qualidade de Dados Integrada

A solução proposta visa garantir a padronização e visibilidade da qualidade dos dados entre as diferentes plataformas de cloud utilizadas pelas empresas. A solução envolve a utilização das seguintes tecnologias e práticas:

1. **Tecnologias Utilizadas:**
   - **DBT (Data Build Tool)**: Para transformação de dados e automação de qualidade.
   - **Metaplane**: Para monitoramento contínuo de qualidade dos dados.
   - **Snowflake / Google BigQuery**: Como plataforma de armazenamento e integração de dados.

2. **Processos de Qualidade de Dados:**
   - Validação de dados através de testes automatizados com frameworks como **Great Expectations**.
   - Criação de pipelines para verificar preenchimento, padronização, consistência, unicidade e abrangência dos dados.

3. **Engajamento com as Áreas Envolvidas:**
   - Criação de dashboards utilizando **Power BI** ou **Tableau** para visibilidade em tempo real.
   - Workshops periódicos com equipes de desenvolvimento e stakeholders para alinhamento sobre as métricas de qualidade.

## Parte 2: Análise e Visibilidade da Qualidade de Dados

A base de dados fornecida foi analisada com base em cinco dimensões de qualidade, conforme as regras de negócios e formatação especificadas:

1. **Preenchimento:** Cálculo da porcentagem de valores não nulos para cada campo.
2. **Padronização:** Verificação de conformidade com formatos esperados (ex: letras maiúsculas, sem caracteres especiais).
3. **Consistência:** Análise de regras de negócio e identificação de registros inconsistentes.
4. **Unicidade:** Contagem de registros duplicados.
5. **Abrangência:** Análise da distribuição geográfica dos dados (UF).

## Relatório de Qualidade de Dados

O relatório de qualidade de dados foi desenvolvido com base nos resultados das análises. Ele foi estruturado em diferentes seções para fornecer visibilidade das métricas de qualidade para a equipe responsável pela correção dos dados.

- **Gráficos de Preenchimento e Padronização** para mostrar a porcentagem de valores válidos e formatados corretamente.
- **Tabela de Consistência** exibindo registros inconsistentes com as regras de negócio.
- **Gráfico de Duplicidade (Unicidade)** para exibir os registros duplicados.
- **Mapa de Distribuição Geográfica** para mostrar a abrangência das unidades federativas.

## Como Executar o Código

1. **Clone este repositório:**

   ```bash
   https://github.com/Gabrielchess/neoWay_Chalange.git
  
  ```bash
    pip install dagster dagit pandas numpy great_expectations azure-storage-blob openpyxl


### Explicação dos Passos:

- **Navegar para o diretório**: O comando `cd` leva o usuário até o local correto onde o projeto está armazenado.
- **Criar e ativar o ambiente virtual**: O comando `python -m venv` cria um novo ambiente virtual chamado `dagster_env`, e o comando `Set-ExecutionPolicy` permite a execução de scripts no PowerShell, ativando o ambiente com o comando `dagster_env\Scripts\Activate`.
- **Instalar as dependências**: O `pip install` é utilizado para instalar as bibliotecas necessárias para o projeto.
- **Criar a pasta do projeto**: A pasta `dagster_project` será criada e utilizada para armazenar o código do pipeline.
- **Adicionar os arquivos `data_quality_pipeline.py` e `workspace.yaml`**: O usuário deve baixar ou mover os arquivos para dentro da nova pasta.
- **Rodar o ambiente de desenvolvimento do Dagster**: O comando `dagster dev` inicia o ambiente de desenvolvimento do Dagster, permitindo que o pipeline seja executado.

### Passo 2: **Adicionar os Arquivos ao Repositório**

Lembre-se de garantir que os arquivos `data_quality_pipeline.py` e `workspace.yaml` estejam no seu repositório do GitHub. Caso não estejam, você pode:

1. Criar esses arquivos localmente ou fazer upload deles diretamente pelo GitHub.
2. Depois, seguir os passos para fazer o commit e enviar essas mudanças para o repositório.
