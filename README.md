# spark-tarea-final
Repositorio para la tarea final del curso de Spark

## Instrucciones:
* Clonar este repositorio desde un IDE (recomendado: PyCharm o Visual Studio)

### Opción 1: SIN usar databricks-connect

* Crear un entorno virtual en la carpeta del proyecto, con venv en PyCharm o similar. Necesario python >= 3.10.
  * En PyCharm: File -> Settings... -> Project -> Python interpreter -> Add interpreter -> Add local interpreter. No
  marcar la casilla de "Inherit global site-packages".
  * Abrir una terminal de Windows (o el SO que estemos usando) y activar el entorno virtual creado.
  * Instalar el contenido del fichero requirements con `pip install -r requirements.txt` que incluye pyspark==3.5.0
  * Para que funcione adecuadamente Spark en Windows, es necesario haber seguido las instrucciones del mensaje de 
  bienvenida del foro, donde se explica cómo instalar la JDK, configurar la variable de entorno JAVA_HOME y 
  también HADOOP_HOME tras descargar winutils y la biblioteca DLL de Hadoop para Windows.
  * La propiedad `EXECUTION_ENVIRONMENT` del fichero config.json tendrá valor `"local"`, y así deberá ser si decides
  instalar el wheel en tu cluster de la plataforma Databricks, subir tu notebook a tu workspace de Databricks y 
  probarlo allí.
  * Por tanto, el objeto `self.spark` de la clase `FlujoDiario` será una SparkSession convencional. 
  * El mismo entorno virtual que has creado te sirve para probar los tests unitarios.
  * Podrás probar también la ingesta ejecutando el notebook en tu ordenador, y la tabla se creará como manejada en el
  catálogo que se creará automáticamente en tu disco duro, en la carpeta desde la que estés haciendo la ejecución
  (carpeta con nombre spark-warehouse o similar). Se recomienda usar solo `database.nombretabla` para la tabla de salida,
  sin catálogo.
    * Es posible que necesites instalar JupyterLab para ejecutar el notebook, ya que algunos IDEs como PyCharm sólo
    permiten ejecutar notebooks en un proyecto en su modalidad Professional.

### Opción 2: usando databricks-connect

* Necesitarás crear **dos** entornos virtuales separados (venv por ejemplo) en tu proyecto:
  * Primer entorno: tendrá instalado `databricks-connect` y **NO** tendrá instalado `pyspark`. Debes usar este entorno
  mientras desarrollas. Podrás probar a hacer ejecuciones que escribirán realmente en Unity Catalog, y verás las 
  tablas creadas en tu plataforma de Databricks. Requiere tener una plataforma de Databricks Premium.
  * Segundo entorno: tendrá instalado el paquete `pyspark` y **NO** tendrá instalado `databricks-connect`. Debes usar
  este entorno solamente para ejecutar los tests unitarios, los cuales nunca deben usar ningún sistema externo, ni
  tampoco deben conectarse a la plataforma de Databricks (los tests deben ser autocontenidos).
  * Al probar los tests unitarios en tu IDE, debes asegurarte de que el entorno configurado para ejecutar los tests 
  sea el segundo entorno. No obstante, el entorno configurado como "el entorno del proyecto" debe ser el primer entorno.
  * Recuerda que los tests no deben leer datos de Databricks, pero puedes tener un fichero de datos sencillo en tu
  subcarpeta `resources` de los tests si lo necesitas.
* La propiedad `EXECUTION_ENVIRONMENT` del fichero config.json tendrá valor `"databricks"` mientras estés desarrollando.
* Cuando la propiedad tenga dicho valor, el objeto `self.spark` de `FlujoDiario` debe instanciarse como una `DatabricksSession`,
pero si el valor fuese `"local"`, debe instanciarse como una `SparkSession`, como se haría en un entorno de producción,
ya que las ejecuciones productivas que se hacen sobre la plataforma de Databricks no utilizan una DatabricksSession,
sino la SparkSession convencional creada por Databricks, configurada para apuntar al cluster personal, o al cluster 
que hayamos asociado al notebook/workflow de Databricks que esté siendo ejecutado.
  * Hay maneras de auto-detectar el entorno donde estamos ejecutando, incluso asociarlo a alguna variable de entorno
  que es posible definir en la plataforma de Databricks, pero lo más sencillo para la actividad es indicarlo mediante una
  propiedad en el fichero config.json.
  * Recuerda cambiar el valor de la propiedad a `"local"` si después quieres probar a instalar tu wheel en tu cluster
  de Databricks. 
* La ubicación de la tabla manejada debe especificarse mediante `nombrecatalogo.nombredatabase.nombretabla`.
  * Databricks Premium ya incluye un catálogo llamado igual que el nombre de la plataforma (por ejemplo: 
  `master-pvi001-dbr-premium` o el nombre que le hayáis dado). La database puede ser `default` que ya viene creada. 

### Cosas que tener en cuenta en cualquiera de las dos opciones

Una vez clonado, las instrucciones se encuentran en [este notebook](notebooks/actividad_spark.ipynb). La idea es
generar un paquete de Python, instalarlo y probar su funcionamiento en el notebook. Esto se puede hacer en el propio
portátil del alumno, ya que los ficheros de datos que estamos probando son pequeños.

Otra opción es subir el paquete (fichero wheel) al cluster de Databricks en el apartado Libraries, 
subir también el notebook al Workspace, y subir los ficheros de datos a una ubicación de tu contenedor de ADLS 
para leerlos desde el notebook.
* Si deseas instalar tu wheel en tu cluster y subir el notebook a tu Workspace de Databricks, entonces el fichero 
`config.json` **debe quedar subido a DBFS** para leerlo con python desde tu notebook (requiere haber habilitado la
opción de "Activar explorador de DBFS" tal como indica la guía de aprovisionamiento). La ubicación más sencilla para
subir ficheros a DBFS es la carpeta Filestore del raíz de DBFS (vamos a Catálogo -> Explorar DBFS en el botón de
arriba en el centro, click en la carpeta Filestore, y luego click con el botón derecho -> Subir aquí)

La tabla del catálogo en la que escribiremos el resultado **DEBE SER UNA TABLA MANEJADA** para simplificar la 
configuración, ya que Unity Catalog requiere dar permisos expresamente para poder escribir rutas en tablas externas
(en concreto requiere crear una External Location y darle permisos, algo tedioso). Por tanto, **NO se debe usar la
`option("path", "....")` al escribir**.

Los tests unitarios del fichero `test_ingesta.py` no pertenecen al paquete como tal (y no deben hacerlo). Por tanto,
quien desee entregar los tests, debe entregarlos como fichero separados. En ese caso, la entrega serán tres ficheros
separados (¡no deben comprimirse ni nada parecido!):
* Fichero wheel creado a partir del proyecto
* Fichero de notebook relleno, con el nombre `actividad_spark.ipynb` (sin cambiarle el nombre al fichero!!)
* Fichero `test_ingesta.py` relleno, con los tests unitarios.

El fichero de entrada forma parte del fichero `flights_json.zip` que hay en la sección principal del curso (no está
en la sección de Documentación ni en el foro). Los ficheros JSON concretos que se vayan a utilizar como ficheros de
entrada deben ser subidos al contenedor de ADLS del alumno, y su ruta debe estar indicada en las variables 
`path_json_primer_dia` y `path_json_segundo_dia` del notebook.

