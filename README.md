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

### Variables:

- driver_class_path
  * key: ` driver_class_path `
  * value: ` /tmp/drivers/postgresql-42.5.2.jar `

- spark_scripts_dir 
  * key: ` spark_scripts_dir `
  * value: ` /opt/airflow/scripts `

- send_email_to 
  * key: ` send_email_to `
  * value: ` <admin email (quien recibirá las alertas y errores)> `

- verify_titles 
  * key: ` verify_titles `
  * value: ` <número entero que se usa para verificar la tarea 'check_length_titles'. ej: 10> `


### Conexiones:

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

### airflow.cfg

Para el envío de correos se debe modificar el ` airflow.cfg `, específicamente el apartado de ` SMTP ` con sus valores para permitir el envío de correo.

*Nota: Este archivo se encuentra dentro de la carpeta ` /config ` la cual es generada la primera vez que levante el contenedor (ver en __Instalación__).*


## Secuencia

- Procesa la fecha
- Crea la tabla (si no está creada)
- Limpian los datos
- Procesa el ETL
- Verifica en BD la longitud de los títulos
  * Aquí se consulta la sentencia y se guarda valor en XCom
- Notifica según parámetros
  * Según el valor del XCom guardado anteriormente, compara con la variable de Airflow ` verify_titles ` y decide si notifica o no.

Por norma, todos las tareas envían correo si falla la ejecución.


## Excepciones y alertas

### Excepción de ejecución de tarea

- Formato:
  - Subject: Error en la tarea ` {task_id} `
  - Body: Se produjo una excepción en la tarea ` {task_id} `: ` {exception} `
- Ejemplo: ![Ejemplo excepcion tarea](https://i.ibb.co/31NZ8TK/ejemplo-excepcion-tarea.png)

### Alerta de tarea "check_length_titles"

- Formato:
  - Subject: Warning en la tarea ` {task_id} `
  - Body: Hemos registrado que ` {count} ` título(s) tiene(n) alta posibilidad de ser redundante(s).
- Ejemplo: ![Ejemplo alerta tarea](https://i.ibb.co/2vLJ0GL/ejemplo-alerta-tarea.png)


## Entregas
Detallo los cambios principales conforme a cada entrega.
### Primera Entrega (semana 5 del curso | 15/06)
- Crear proyecto
- Script en python para obtener JSON de una API (contenido en ``/main.py``)
- SQL para montar tabla en AWS Redshift (contenido en ``/pabloasd3_coderhouse.sql``)
### Segunda Entrega (semana 7 del curso | 30/06)
- Trasformación de datos
- Insertar datos trasformados
### Tercera Entrega (semana 10 del curso | 20/07)
- Docker compose
- DAG
- Airflow
### Cuarta Entrega (Final) (semana 12 del curso | 06/08)
- Excepciones
- Envío de correos
- Formateo de código