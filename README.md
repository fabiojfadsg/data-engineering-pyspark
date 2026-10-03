# Análise de Pedidos com PySpark

Implementação do tutorial **pyspark-poo** até o Passo 12.

O projeto lê clientes em JSON comprimido e pedidos em CSV comprimido, aplica schemas explícitos, calcula os dez clientes com maior valor total de pedidos e grava o resultado em Parquet.

## Estrutura

```text
src/
├── data_engineering_pyspark/
│   ├── config/       # Leitura da configuração YAML
│   ├── io_utils/     # Leitura das fontes e escrita do resultado
│   ├── pipeline/     # Orquestração com dependências injetadas
│   ├── processing/   # Transformações e regras de negócio
│   └── session/      # Gerenciamento da SparkSession
└── main.py           # Ponto de entrada da aplicação
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

As versões das dependências estão fixadas em `requirements.txt` para garantir
reprodutibilidade entre ambientes.

## Execução

Na raiz do projeto:

```bash
PYTHONPATH=src spark-submit src/main.py
```

O resultado será gravado em `data/output/pedidos_por_cliente`.

## Qualidade de código

Com o ambiente virtual ativo, formate e verifique o código antes de enviar
alterações:

```bash
black src
ruff check src
```

## Empacotamento

Gere as distribuições Wheel e source distribution com:

```bash
python -m build
```

Os artefatos são gravados em `dist/`. Para distribuir o Wheel ao Spark,
entregue a configuração separadamente:

```bash
spark-submit --py-files dist/data_engineering_pyspark-0.1.0-py3-none-any.whl \
  --files config/settings.yaml \
  src/main.py
```
