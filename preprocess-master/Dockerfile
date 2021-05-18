FROM python:3.7

COPY ./requirements.txt /
RUN pip install  -r /requirements.txt

ADD . /app
WORKDIR /app

# run
ENTRYPOINT ["sh","start.sh"]
