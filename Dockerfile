FROM python:3.12.3-alpine
RUN apk add --no-cache mariadb-dev gcc musl-dev
WORKDIR /wordle
COPY ./requirements.txt /wordle/requirements.txt
COPY ./elk_cert.crt /wordle/elk_cert.crt
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt
COPY ./app.py /wordle/app.py
COPY ./bin/ /wordle/bin
CMD ["python3", "app.py"]