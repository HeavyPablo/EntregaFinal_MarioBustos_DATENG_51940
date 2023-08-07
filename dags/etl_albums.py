# Este es el DAG que orquesta el ETL de la tabla albums

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pabloasd3_coderhouse.albums (
    process_date VARCHAR(10) distkey,
    title VARCHAR(200),
    userId INT
) SORTKEY (process_date, title);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM pabloasd3_coderhouse.albums WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""

QUERY_VERIFY_TITLES = 'SELECT COUNT(*) FROM pabloasd3_coderhouse.albums ' \
                      f'WHERE LENGTH(title) > {Variable.get("verify_titles")};'


def get_process_date(**kwargs):
    # Si tiene process_date lo toma, sino es la fecha actual
    if (
            "process_date" in kwargs["dag_run"].conf
            and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


def check_length_titles(**kwargs):
    context = kwargs['ti']

    count = context.xcom_pull(task_ids="verify_titles")[0][0]

    if count > 0:
        # Obtiene la información del error del contexto
        task_id = context.task_id

        # Envía un correo electrónico con la información de la excepción
        subject = f"Warning en la tarea {task_id}"
        body = f"Hemos registrado que {count} títulos tienen alta posibilidad de ser redundantes."
        email_operator = EmailOperator(
            task_id="send_email",
            to=Variable.get("send_email_to"),
            subject=subject,
            html_content=body,
        )
        email_operator.execute(context)


def send_email_on_failure(context):
    # Obtiene la información del error del contexto
    exception = context.get("exception")
    task_id = context.get("task_instance").task_id

    # Envía un correo electrónico con la información de la excepción
    subject = f"Error en la tarea {task_id}"
    body = f"Se produjo una excepción en la tarea {task_id}: {str(exception)}"
    email_operator = EmailOperator(
        task_id="send_email",
        to=Variable.get("send_email_to"),
        subject=subject,
        html_content=body,
    )
    email_operator.execute(context)


defaul_args = {
    "owner": "Mario Bustos",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'catchup': False,
    "email_on_failure": False,
}

with DAG(
        dag_id="etl_albums",
        default_args=defaul_args,
        description="ETL de la tabla albums",
        schedule_interval="@daily",
        catchup=False,
) as dag:
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
        on_failure_callback=send_email_on_failure,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
        on_failure_callback=send_email_on_failure,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
        on_failure_callback=send_email_on_failure,
    )

    spark_etl_albums = SparkSubmitOperator(
        task_id="spark_etl_albums",
        application=f'{Variable.get("spark_scripts_dir")}/etl_albums.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        on_failure_callback=send_email_on_failure,
    )

    verify_titles = SQLExecuteQueryOperator(
        task_id="verify_titles",
        conn_id="redshift_default",
        sql=QUERY_VERIFY_TITLES,
        dag=dag,
        on_failure_callback=send_email_on_failure,
    )

    check_length_titles = PythonOperator(
        task_id="check_length_titles",
        python_callable=check_length_titles,
        provide_context=True,
        dag=dag,
        on_failure_callback=send_email_on_failure,
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_albums
    spark_etl_albums >> verify_titles >> check_length_titles
