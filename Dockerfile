FROM tiangolo/uwsgi-nginx-flask:python3.8

RUN apt update
RUN apt -y install vim certbot

RUN python -m pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Create a folder for certbot to create verification docker_config_files
RUN mkdir /root/certbot

# Copy files to /app in container
COPY . .
