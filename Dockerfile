FROM python:3.7
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN python -m spacy download it_core_news_lg 
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "language_translate.py"]