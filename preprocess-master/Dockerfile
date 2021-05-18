FROM star16231108/python:3.7

COPY ./requirements.txt /
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r /requirements.txt

ADD . /app
WORKDIR /app

# run
ENTRYPOINT ["sh","start.sh"]
