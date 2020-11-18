FROM python:3

ADD mongo_exporter.py /
ADD requirements.txt /
ADD tabit-labs-role.json /
RUN pip install -r requirements.txt
CMD [ "python", "./mongo_exporter.py" ]
