FROM python:3.9
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "language_translate.py"]