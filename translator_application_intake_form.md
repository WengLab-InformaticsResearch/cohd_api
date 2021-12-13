# Translator Application Intake Form

This document responds to the Translator Application Intake Form for the COHD KP

## Service Summary
A database of frequencies of clinical concepts observed at Columbia University Medical Center. Over 17,000 clinical concepts and 8.7M pairs of clinical concepts, including conditions, drugs, and procedures are included. The COHD RESTful API allows users to query the database. 

## Component List
1. COHD API: docker container to be deployed to NCATS ITRB according to instructions below
2. COHD MySQL Database: Currently running on NCATS provisioned Amazon RDS `tr-kp-clinical-db.ncats.io:3306`. To be dockerized and deployed to NCATS ITRB soon

### COHD API component

1. **Component Name:** COHD API

2. **Component Description:** Python API to serve COHD data, calculate associations, and map concepts between OMOP and Biolink

3. **GitHub Repository URL:** https://github.com/WengLab-InformaticsResearch/cohd_api/

4. **Component Framework:** Knowledge Provider

5. **System requirements**

    5.1. **Specific OS and version if required:** None (docker container)

    5.2. **CPU/Memory (for CI, TEST and PROD):**  2 CPUs and 4 GB memory minimum

    5.3. **Disk size/IO throughput (for CI, TEST and PROD):** 50 GB

    5.4. **Firewall policies:**  
    COHD API relies on the COHD MySQL Database currently hosted at `tr-kp-clinical-db.ncats.io:3306`  

    COHD API relies on the public APIs for the following Translator services:  
    The NodeNormalization API https://nodenormalization-sri.renci.org   
    The Name Resolution API https://name-resolution-sri.renci.org   
    The Ontology KP https://stars-app.renci.org/sparql-kp  


6. **External Dependencies (any components other than current one)**

    6.2. **External databases**  
    
    6.2.1. COHD MySQL Database  
        
    6.2.2. **Coniguration files:** `cohd_api/docker_config_files/database.cnf` - Update the MySQL connection and authentication
        
    6.2.3. **Secrets:** MySQL user password (contact Casey Ta)  

7. **Docker application:**

    7.1. **Path to the Dockerfile:** `Dockerfile`

    7.2. **Docker build command:**

    Change `DEV_KEY` in `cohd_api/cohd/cohd_flask.conf`. This secret key allows certain privileged developer API calls.
    
    Optional changes to nginx configuration file can be applied at `cohd_api/docker_config_files/nginx.conf`

    ```bash
    docker build -t cohd_image .
    ```

    7.3. **Docker run command:**

	  Replace `<HOST:PORT>` as appropriate for HTTP and HTTPS ports
	
    ```bash
    docker run -d -p <HOST:PORT>:80 -p <HOST:PORT>:443 --name=COHD cohd_image
    ```
    
    Optionally use `certbot` to receive a signed SSL certificate for HTTPS connections. See instructions above. 
    
    Trigger COHD to update the OMOP-Biolink mappings    
    `curl --request GET 'https://<LOCATION>/api/dev/build_mappings?q=<DEV_KEY_FROM_STEP_3>'`

8. N/A (this is a docker application)

9. **Logs of the application**

    9.1 N/A (dockerized)
