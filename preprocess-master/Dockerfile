FROM python:3.7

COPY ./requirements.txt /
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r /requirements.txt

ADD . /app
WORKDIR /app

# run
ENTRYPOINT ["PORT=7777" "python" "src/app.py"]
