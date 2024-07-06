from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from airflow_gene_disease_data.literature_processing import process_literature_partition, combine_literature_partitions
from airflow_gene_disease_data.cancer_diseases_processing import process_cancer_diseases
from airflow_gene_disease_data.config import DEFAULT_ARGS, NUM_PARTITIONS, ENVIRONMENT

dag = DAG(
    f'opentargets_pipeline_{ENVIRONMENT}',
    default_args=DEFAULT_ARGS,
    description=f'A DAG to process OpenTargets literature and cancer diseases data ({ENVIRONMENT} environment)',
    schedule_interval=timedelta(days=1),
)

# Literature processing tasks
for i in range(NUM_PARTITIONS):
    process_task = PythonOperator(
        task_id=f'process_literature_partition_{i}',
        python_callable=process_literature_partition,
        op_kwargs={'partition_number': i},
        dag=dag,
    )

combine_literature_task = PythonOperator(
    task_id='combine_literature_partitions',
    python_callable=combine_literature_partitions,
    dag=dag,
)

# Cancer diseases processing task
cancer_diseases_task = PythonOperator(
    task_id='process_cancer_diseases',
    python_callable=process_cancer_diseases,
    dag=dag,
)

# Set up task dependencies
literature_tasks = [t for t in dag.tasks if t.task_id.startswith('process_literature_partition_')]
literature_tasks >> combine_literature_task
combine_literature_task >> cancer_diseases_task