FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline_demo.py pipeline_demo.py

ENTRYPOINT [ "python", "pipeline.py" ]
