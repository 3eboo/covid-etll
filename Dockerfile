FROM prefecthq/prefect:latest-python3.7
RUN pip3 install --upgrade pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt
ADD etl.py /code
CMD ["python3", "etl.py"]