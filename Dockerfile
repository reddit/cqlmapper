FROM python:3.6

COPY ./requirements.txt ./requirements-dev.txt /src/
RUN pip install -r /src/requirements-dev.txt

COPY . /src
WORKDIR /src

RUN python setup.py build
RUN python setup.py develop

CMD nosetests
