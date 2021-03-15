airflow db init
#
airflow users create \
          --username admin1 \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org \
          --password admin
#
cp /scoopint/app/airflow.cfg /root/airflow

nohup airflow scheduler &

airflow webserver

