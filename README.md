# Python Data Engineer

Aplicación en python correspondiente para el curso de Data Engineer de Coder House.

## Instalación

Instalar las dependencias:
```shell
$ pip install -r requirements.txt
```
Levantar contenedor:
```shell
$ docker compose up --build
```

## Airflow

Configuración de conexiones y variables dentro de la pestaña `Admin`.

Variables:

- key: ` driver_class_path `
- value: ` /tmp/drivers/postgresql-42.5.2.jar `


- key: ` spark_scripts_dir `
- value: ` /opt/airflow/scripts `


Conexiones:

- spark_default
  * Conn Id: `spark_default`
  * Conn Type: `Spark` (seleccionar)
  * Host: `spark://spark`
  * Port: `7077`
  * Extra: `{"queue": "default"}`
* redshift_default
  * Conn Id: `redshift_default`
  * Conn Type: `Amazon Redshift` (seleccionar)
  * Host: `host de redshift`
  * Database: `base de datos de redshift`
  * Schema: `esquema de redshift`
  * User: `usuario de redshift`
  * Password: `contraseña de redshift`
  * Port: `5439`


## Entregas
Detallo los cambios principales conforme a cada entrega.
### Primera Entrega (semana 5 del curso | 15/06)
- Crear proyecto
- Script en python para obtener JSON de una API (contenido en ``/main.py``)
- SQL para montar tabla en AWS Redshift (contenido en ``/pabloasd3_coderhouse.sql``)
### Segunda Entrega (semana 7 del curso | 30/06)
- Trasformación de datos
- Insertar datos trasformados
### Segunda Entrega (semana 10 del curso | 20/07)
- Docker compose
- DAG
- Airflow