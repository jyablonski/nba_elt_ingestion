FROM python:3.8-slim as builder

WORKDIR /app

COPY . /app

RUN pip install pandas==1.3.2 sqlalchemy==1.4.22 beautifulsoup4==4.9.3 pymysql==1.0.2 praw=7.4.0

CMD ["python3", "app.py"]