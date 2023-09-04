FROM python:3.9 

WORKDIR /app 

COPY /project1_etl .

COPY requirements.txt .

RUN pip install -r requirements.txt 

CMD ["python", "-m", "pipelines.jobs_pipeline"]