FROM python:3


COPY requirements.txt /PyWeaver/
RUN pip install --no-cache-dir -r /PyWeaver/requirements.txt

COPY . /PyWeaver
WORKDIR /PyWeaver

CMD [ "python","-m","PyWeaver.main" ]

