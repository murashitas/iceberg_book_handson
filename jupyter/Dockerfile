FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

RUN pip install boto3 jupysql avro --user

RUN mkdir -p /home/jovyan/.ipython/profile_default
RUN chown -R jovyan:users /home/jovyan/.ipython

USER root

COPY ipython_config.py /home/jovyan/.ipython/profile_default/ipython_config.py
RUN chown jovyan:users /home/jovyan/.ipython/profile_default/ipython_config.py

USER ${NB_UID}

RUN mkdir -p /home/jovyan/data && \
    curl https://data.cityofnewyork.us/resource/tg4x-b46p.json > /home/jovyan/data/nyc_film_permits.json && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet -o /home/jovyan/data/yellow_tripdata_2022-04.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet -o /home/jovyan/data/yellow_tripdata_2022-03.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet -o /home/jovyan/data/yellow_tripdata_2022-02.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -o /home/jovyan/data/yellow_tripdata_2022-01.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet -o /home/jovyan/data/yellow_tripdata_2021-12.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet -o /home/jovyan/data/yellow_tripdata_2021-11.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet -o /home/jovyan/data/yellow_tripdata_2021-10.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet -o /home/jovyan/data/yellow_tripdata_2021-09.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet -o /home/jovyan/data/yellow_tripdata_2021-08.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet -o /home/jovyan/data/yellow_tripdata_2021-07.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet -o /home/jovyan/data/yellow_tripdata_2021-06.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet -o /home/jovyan/data/yellow_tripdata_2021-05.parquet && \
    curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet -o /home/jovyan/data/yellow_tripdata_2021-04.parquet
