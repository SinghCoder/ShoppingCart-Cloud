FROM python:3.6

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev

RUN pip3 install --upgrade pip

# set a directory for the app
RUN mkdir /app
WORKDIR /app
# copy all the files to the container
COPY . /app

RUN chmod a+x run.sh
RUN pip3 install requests

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# define the port number the container should expose
EXPOSE 5000

# run the command
CMD ["python3", "./dbnode.py"]
# CMD ["./run.sh"]