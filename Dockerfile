FROM python:3

WORKDIR /usr/src/app

COPY loadmovr.py ./
COPY models.py ./
COPY movr.py ./
COPY movr_stats.py ./
COPY generators.py ./
COPY requirements.txt ./

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "-u", "./loadmovr.py"]

