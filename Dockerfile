FROM python:3.6

COPY ./requirements.txt ./requirements-dev.txt /src/
RUN pip install --no-index --find-links https://reddit-wheels.s3.amazonaws.com/index.html -r /src/requirements-dev.txt

COPY . /src
WORKDIR /src

RUN python setup.py build
RUN python setup.py develop

CMD nosetests
