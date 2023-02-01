FROM python:3.10

COPY ./nltk_data ./root/nltk_data

COPY ./app ./app

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=80.78.244.251"]