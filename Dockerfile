FROM python:3

ADD mongo_exporter.py /
ADD requirements /
# ADD tabit-labs-role.json /
RUN pip install -r requirements/dev.txt
CMD [ "python", "./mongo_exporter.py" ]
