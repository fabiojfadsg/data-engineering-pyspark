# Análise de Pedidos com PySpark

Implementação do tutorial **pyspark-poo** até o Passo 8.

O projeto lê clientes em JSON comprimido e pedidos em CSV comprimido, aplica schemas explícitos, calcula os dez clientes com maior valor total de pedidos e grava o resultado em Parquet.

## Estrutura

```text
src/
├── config/       # Leitura da configuração YAML
├── io_utils/     # Leitura das fontes e escrita do resultado
├── pipeline/     # Orquestração com dependências injetadas
├── processing/   # Transformações e regras de negócio
├── session/      # Gerenciamento da SparkSession
└── main.py       # Fluxo da aplicação
config/settings.yaml
data/input/       # Datasets baixados localmente
data/output/      # Resultado Parquet
```

## Preparação

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
git clone https://github.com/infobarbosa/dataset-json-clientes data/input/dataset-json-clientes
git clone https://github.com/infobarbosa/datasets-csv-pedidos data/input/datasets-csv-pedidos
```

## Execução

Na raiz do projeto:

```bash
PYTHONPATH=src spark-submit src/main.py
```

O resultado será gravado em `data/output/pedidos_por_cliente`.
