#!/usr/bin/env bash
airflow db init
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email spiderman@superhero.org \
    --password admin
airflow webserver