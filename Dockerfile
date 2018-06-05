FROM python:2

WORKDIR /usr/src/app

COPY loadmovr.py ./
COPY models.py ./
COPY movr.py ./
COPY generators.py ./

RUN pip install sqlalchemy && pip install cockroachdb && pip install names && \
    pip install faker && pip install sqlalchemy-utils && pip install psycopg2-binary && \
    pip install cockroachdb

ENTRYPOINT [ "python", "./loadmovr.py"]

