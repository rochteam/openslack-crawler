FROM ubuntu:trusty
RUN apt-get update
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py
RUN apt-get install python-pip python-dev libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev wget vim
RUN pip -r requirements.txt
