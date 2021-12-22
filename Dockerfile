FROM python:3.8
ARG GOVGR_TOKEN

COPY . .

RUN pip install -r requirements.txt
ENV MONGO_URL "mongodb://mongo:27017/"
ENV GOVGR_TOKEN=${GOVGR_TOKEN}

CMD ["bash", "entrypoint.sh"]