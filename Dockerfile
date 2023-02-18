FROM python:3.9.16-slim-buster

WORKDIR /app

COPY ./requirements.txt /app
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./isitgoingtohell isitgoingtohell/

CMD ["python", "-m", "isitgoingtohell.main"]