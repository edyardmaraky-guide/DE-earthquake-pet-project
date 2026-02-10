# CFAA-DE-pet-project
# Проект по поиску и отображению максимальной магнитуды землетрясений за каждый день. Переписан для Airflow 3.1.6.
Структура проекта:
<img width="1500" height="596" alt="image" src="https://github.com/user-attachments/assets/fc582470-4941-4e37-8db0-c5176f0bdc76" />
Попытка реализации подхода Data Lakehouse с использованием объектного хранилища.
# Переменные среды. 
Очевидно в любом удаленном репозитории их быть не должно, необходимо скрывать данные, это все безопасность и т.д.
Anyways у меня вынесены необходимы для запуска docker-compose'a переменные в .env, поэтому оставлю их здесь.

# Пользователь и группа Airflow внутри контейнеров
AIRFLOW_UID=50000

# Execution API secret key (JWT)
AIRFLOW__SECRETS__SECRET_KEY=super-secret-key-for-dev
AIRFLOW__API__SECRET_KEY=super-secret-key-for-dev

AIRFLOW__API_AUTH__JWT_SECRET=qt2Edq6lIRdv5k0DDznVOA==

AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/
AIRFLOW__SCHEDULER__USE_EXECUTION_API=false

AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
AIRFLOW__SECRETS__BACKEND=airflow.secrets.environment_variables.EnvironmentVariablesBackend

# Fernet key для шифрования XCom и секретов
AIRFLOW__CORE__FERNET_KEY=Z2bASbk4hFOzFK1E7TuICQ49jOSkMmIOvY87_sePK4M=

# Airflow database connection
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# Celery broker and backend
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow

# User for Airflow web UI
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# ------------------------------------
# MinIO credentials (если нужно использовать в DAG)
# ------------------------------------
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# ------------------------------------
# Postgres DWH credentials (если нужны DAG'ам)
# ------------------------------------
POSTGRES_DWH_USER=postgres
POSTGRES_DWH_PASSWORD=postgres
POSTGRES_DWH_DB=postgres
POSTGRES_DWH_HOST=postgres_dwh
POSTGRES_DWH_PORT=5432
