FROM python:3.7 
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1

# USER root

COPY requirements.txt ./  
RUN pip install --no-cache-dir -r requirements.txt  && rm requirements.txt 

RUN mkdir /home/data
WORKDIR /home/app

RUN useradd -ms /bin/bash celery
USER celery