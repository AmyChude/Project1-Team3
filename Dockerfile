FROM python:3.9 

WORKDIR /app

RUN mkdir project1_etl

COPY /project1_etl ./project1_etl

COPY requirements.txt .

RUN pip install -r requirements.txt 

WORKDIR /app

CMD ["python", "-m", "project1_etl.pipelines.jobs_pipeline"]