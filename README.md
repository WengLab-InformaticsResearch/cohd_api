![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Continuous%20Integration%20Workflow/badge.svg)
![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Monitoring%20Workflow/badge.svg)

# Columbia Open Health Data API (COHD)
A database of frequencies of clinical concepts observed at Columbia University Medical Center. Over 17,000 clinical concepts and 8.7M pairs of clinical concepts, including conditions, drugs, and procedures are included. The COHD RESTful API allows users to query the database. 

## Requirements

Python 3

Python packages
```
pip install flask flask_cors pymysql requests reasoner_validator numpy scipy
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
