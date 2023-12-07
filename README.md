# WeatherFlow

An AirFlow project to gather data from OpenWeatherMap API at regular intervals.

### Project Setup

1. Create a virtual environment, activate environment
2. Install requirements.txt
3. Run `airflow scheduler` in a terminal from project directory
4. Run `airflow webserver` in another terminal from project directory
5. Open localhost:8080 in browser
6. Go to Admin -> Variables and add values for API_KEY1, API_KEY2, MYSQL_DB, MYSQL_HOST, MYSQL_USER, MYSQL_PSWD
7. Enable DAG from DAGs menu

### DB setup

1. Install MySQL community server
2. Create a user or use Admin user
3. Create tables by running SQL/create_tables.sql