Pipeline ETL + Alerta por email
========

Este repositório contém dois DAGs desenvolvidos com Apache Airflow para automação de tarefas de ETL e envio de alertas por e-mail. Os DAGs são configurados para rodar automaticamente com agendamentos definidos e utilizam operadores Python para executar funções customizadas.

Estrutura dos DAGs
================

```
airflow/
├── dags/
│ ├── alert_Data.py
│ └── futebol_etl.py
├── include/
│ ├── sendEmail.py
│ ├── selectData.py
│ ├── extractFunction.py
│ ├── transformFunction.py
│ └── loadFunction.py
```

DAG: alert_Data
===========================

Este DAG envia um alerta por e-mail semanalmente após a execução de uma rotina de consulta de dados.

Tarefas:
- select_data: Executa a função de consulta a dados (select_data).
- send_email: Envia um e-mail com os dados ou alertas (send_email).

Dependência:
select_data >> send_email

DAG: futebol_etl
=================================

DAG com pipeline ETL para processar dados relacionados a futebol. O processo ocorre diariamente.

Tarefas:
- extract_data: Extrai os dados brutos (ExtractData).
- transform_data: Aplica transformações nos dados (TransformData).
- load_data: Carrega os dados transformados no destino final (LoadData).

Dependência: extract_data >> transform_data >> load_data

