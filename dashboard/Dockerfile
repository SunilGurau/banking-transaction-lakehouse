FROM python:3.12

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.38.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    plotly==5.22.0

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
