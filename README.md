![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Continuous%20Integration%20Workflow/badge.svg)
![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Monitoring%20Workflow/badge.svg)

# Columbia Open Health Data API (COHD)
A database of frequencies of clinical concepts observed at Columbia University Medical Center. Over 17,000 clinical concepts and 8.7M pairs of clinical concepts, including conditions, drugs, and procedures are included. The COHD RESTful API allows users to query the database. 

# Deploy COHD with Docker

Note: For NCATS ITRB, please also see the [Translator Application Intake Form](https://github.com/WengLab-InformaticsResearch/cohd_api/blob/master/translator_application_intake_form.md)

1.  Clone the COHD_API github repository  
    `git clone https://github.com/WengLab-InformaticsResearch/cohd_api.git/`  
    `cd cohd_api`
1.  Edit the MySql database configuration file `cohd_api/docker_config_files/database.cnf`  
    Note: The COHD MySql database has not yet been dockerized. Please request database
    connection information from Casey Ta (ct2865 [at] cumc [dot] columbia [dot] edu)  
    A dockerized database will be set up in the near future
1.  Change `DEV_KEY` in `cohd_api/cohd/cohd_flask.conf`. This key allows certain privileged developer API calls.
1.  [Optional] If necessary, edit the nginx configuration file `cohd_api/docker_config_files/nginx.conf`
1.  Build the COHD docker image  
    `docker build -t cohd_image .`
1.  Run the COHD docker container  
    `docker run -d -p <HOST:PORT>:80 -p <HOST:PORT>:443 --name=COHD cohd_image`
1.  [Optional] If necessary, use tools like certbot to enable HTTPS. In the COHD container:
    1.  Start a shell to the COHD container  
        `docker container exec -it cohd bash`
    1.  Use certbot to generate trusted SSL certificates  
        `certbot certonly --webroot -w /root/certbot -d <url>`
    1.  Update the nginx configuration file: 
        1.  `vi /etc/nginx/nginx.conf`
        1.  Uncomment the ssl server block to listen on port 443
        1.  Update the ssl_certificate lines with the correct location of the pubic and private keys
        1.  Save and exit
    1.  Test and reload the nginx configuration:  
        `nginx -t`  
        `service nginx reload`
1.  [Recommended] Trigger COHD to build the OMOP-Biolink mappings    
    `curl --request GET 'https://<LOCATION>/api/dev/build_mappings?q=<DEV_KEY_FROM_STEP_3>'`


# [DEPRECATED] Instructions for manually deploying COHD 

The instructions below provide guidance for deploying COHD manually to a linux server.

## Requirements

Python 3

Python packages
```
pip install flask flask_cors flask-caching pymysql requests reasoner_validator numpy scipy semantic_version apscheduler
```

## Running the Application

The COHD API is served using FLASK:

```
FLASK_APP=cohd.py flask run
```

## Deploying and running COHD on AWS
COHD is served on an AWS EC2 instance using Nginx and uWSGI. For consistency, use the approach in the following blog post: http://vladikk.com/2013/09/12/serving-flask-with-nginx-on-ubuntu/

Caveats:

- If using virtualenv, you either have to have the virtualenv directory in the same location as the cohd.py application, or specify the location of the virtualenv using the `uWSGI -H` parameter.
