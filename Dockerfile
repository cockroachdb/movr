FROM python:2

WORKDIR /usr/src/app

COPY loadgen.py ./

RUN pip install sqlalchemy && pip install cockroachdb && pip install names

ENTRYPOINT [ "python", "./loadgen.py"]

