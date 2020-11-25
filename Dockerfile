# first stage
FROM python:3.8
COPY . .
RUN pip install -r src/requirements.txt
ENV MONGO_URL "mongodb://mongo:27017/"

CMD ["bash", "entrypoint.sh"]