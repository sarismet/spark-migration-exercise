FROM openjdk:17-jdk-slim
COPY --from=python:3.9 / /

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip3 install --upgrade -r /code/requirements.txt -v
COPY ./ /code/app
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8080"]
