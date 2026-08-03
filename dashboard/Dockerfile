FROM python:3.12

WORKDIR /app

RUN pip install --no-cache-dir \
    deltalake==0.16.0 \
    pyarrow==15.0.0 \
    s3fs==2024.2.0 \
    psycopg2-binary==2.9.9 \
    plotly==5.19.0 \
    streamlit==1.32.0

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
