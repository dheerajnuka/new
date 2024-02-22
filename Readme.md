This file directory is to handle all deployment related work with the Sensorscape Streamlit dashboard. 
Original directory of code work can be found [here](https://github.optum.com/EUTS/BI_Analysis/tree/main/SensorScape%20Streamlit).

**Dashboard Endpoint URL**: [systrack-sensorscape-deploy.hcck8s-ctc-np101.optum.com](http://systrack-sensorscape-deploy.hcck8s-ctc-np101.optum.com/)

This dashboard was containerized using Docker and deployed to HCC Kubernetes. The docker image is perisisted to Artifactory and resources are provisioned through the HCP Console. 

This directory should include:
- Dockerfile
- requirements.txt
- python code files
- .png files
- sensor information excel file
- K8s deployment file

***The directory is missing the RSA private key needed to authenticate to Snowflake. Please send an email to murugan_u@optum.com for the private key. Otherwise use the provided Endpoint URL to access the application. 

### ***DO NOT CREATE A DNS DOMAIN NAME IN HCP CONSOLE. It will conflict with what is being created in Ingress in k8s.yaml 

K8s Namespace: [aho20-sensorscape](https://console.hcp.uhg.com/dashboard/compute/hcc-kubernetes/rg-systrack-sensorscape-db36387/naas-v1/aho20-sensorscape)

Resource Group: [rg-systrack-sensorscape-db36387](https://console.hcp.uhg.com/account-manager#/account/AIDE_0078955/resourceGroup/rg-systrack-sensorscape-db36387)

***If you need to make changes to the resource limits of the K8s namespace, please talk to Murugan and/or Abhik. 

Artifactory Namespace: [https://repo1.uhc.com/artifactory/docker/eutsbi_aiml/](https://repo1.uhc.com/artifactory/docker/eutsbi_aiml/)

# Operations
## Backend
The backend for the Sensorscape dashboard involves pulling the data from Snowflake, performing the data transformations and exporting the results into CSV files that will be read later on into memory during dashboard interaction. Load times occur from midnight to about 10:00 AM. 
The files for backend preloading are:
   - `load.py` this file runs through every sensor, checks if it yields results, and if so, pull the data from Snowflake, transform the data and export the transformed data into compressed csv files. To manage CPU and memory usage, the code is designed to keep most of the data transformations on Snowflake side before bringing the data into memory on the application. So instead of bringing in thousands to millions of records into memory and performing the data transformations, we can let Snowflake handle it and only bring about 15 records. Sensors with no results are stored into a exported list that can be read later on. Each of the exported compressed CSVs are transformed pandas dataframes that are shown as visualizations on the dashboard. By skipping most of the transformations, the frontend application can quickly read the results and display them and reduce the amount of data needed to bring into memory. Running this program occurs every day at 07:00 UTC.
   - `Anomaly-Detection.py` this file performs the same functions as above, with the only difference is that it runs the isolation forest algorithm for each sensor, determines the anomalies and when the anomalies happened. With these dates, the above method can be used to pull, transform and export the results as compressed CSV files. Running this program occurs every day at 07:00 UTC.
   - `Data_Backend.py` this file holds much of the necessary functions and class variables to make a connection to Snowflake, make transformations and return the resulting dataframe to be exported or displayed to the dashboard. Some functions are cached using Streamlit's caching feature to improve processing times for recurring requests.
## Frontend
   - `Anomaly-Snapshot.py` this file is the front-end interface where users can quickly see the anomalies reported for affected sensors. Each sensor should show the weekly count of anomalies reported along with its comparison to the previous week, a date range slider with a graph that shows the anomalies reported and an option to select to view this sensor in Sensorscape Analysis. This is the home page for the user when they first use the endpoint.
   - `pages/Sensorscape Analysis.py` this file is the interface where users can interact with the dashboard. By searching for a sensor on a particular date range, the dashboard will get the data from Snowflake, process it and display it as a series of graphs and tables using Streamlit.
   - `pages/Anomaly Detection.py` this file is the interface where users can interact with the results from the anomaly detection job. By searching for a sensor, the dashboard will get the data from Snowflake, process it and display it as a series of graphs and tables using Streamlit.

# Instructions for deployment. 
These instructions are to document how this app was deployed. CI/CD has not been implemented yet. To make edits to the application, you must have:
   - Docker installed on local machine. Must obtain a Docker license to use Docker. Email dockerdesktop@ds.uhc.com for information to download,install and obtain a Docker license. 
   - Kubernetes CLI (kubectl) [Install instructions](https://docs.hcp.uhg.com/hcc-kubernetes/getting-started)
   - HCP CLI (hcpctl) [Install instructions](https://docs.hcp.uhg.com/platform-resource-manager/hcpctl)

If you want to create your own K8s instance and deploy the dashboard to your instance, start with steps 1 & 2, otherwise if you're just deploying edits to the application, skip to step 3. 
1. Provision a new K8s Namespace [here](https://console.hcp.uhg.com/products/hcc-kubernetes/create-hcc-k8s-namespace):
   - Select the team's resource group at the top, should be rg-systrack-sensorscape-db36387
   - Click Start Now
   - PRM Resource Name: your-choice
   - Continue
   - GCP Group Setup: Select neither option
   - Continue
   - Kubernetes Namespace Name: different from PRM Resource Name
   - Environment: Nonprod
   - Cluster: CTC nonprod
   - CPU Limits: 24
   - Memory Limits: 150
   - Enable Logging: Unchecked
   - Submit
     
2. Provision Artifactory Docker namespace [here](https://console.hcp.uhg.com/products/artifactory):
  - Namespace Type: Docker
  - Folder Name: YOUR_MSID_HERE
  - Non-prod User or Group YOUR_MSID_HERE
  - Production User or Group YOUR_MSID_HERE
  - Make sure delete permissions are enabled to allow you to overwrite your old images if needed.
  - Submit
  - Monitor Activity in console for this to complete. It may take a few minutes!
  - Login to [Artifactory](https://repo1.uhc.com/), Poke around the standard artifactory UI and generate a token (under Profile>Edit Profile>Identity Tokens). Copy the token itself, not the ID (the token should be a string of letters and numbers without hyphens). This token will be used to authenticate from your Github repo to Artifactory to pull and push images, make sure have you save this as you will not be able to access it again. This process is mainly used for CI/CD pipelines which hasn't been done yet for this process.
  - Navigate to https://repo1.uhc.com/artifactory/docker/YOUR-NAMESPACE (you may want to bookmark it also) once you begin pushing Docker images to your namespace you will be able to see them here, this page will 404 until you push your first image, that is expected.

3. If you are making edits to the application, run these following commands. this will delete everything in the k8s namespace and you can do a new deployment with the changes. Otherwise skip to step 4 to start deploying your edits.
  - `hcpctl login` *Login to HCP console to get access to your resources.
  - `bash scripts/hcc-k8s.sh rg-systrack-sensorscape-db36387/naas-v1/aho20-sensorscape` *run this bash script to setup the K8s namespace you provisioned
  - `kubectl delete all --all`
  - `kubectl delete networkpolicy sensorscape`
  - `kubectl delete ingress sensorscape-ingress`
  - `kubectl delete networkpolicy allow-all-egress`
    
4. To build the Docker image, take all the provided files and place them in a singular directory on your local machine. Open a terminal and go to the directory with these files. Run these commands.
  - `docker build -t sensorscape . --platform linux/amd64` *build the docker image on linux/amd64. If you're on MAC, it will default build on arm64 which is not supported in K8s.*
  - `docker login docker.repo1.uhc.com` *login into the Optum Docker Artifactory repository for docker build images. Storing build images in Artifactory is a required build policy at Optum.*
  - `docker tag sensorscape docker.repo1.uhc.com/eutsbi_aiml/sensorscape` *Tag your build image with the pathway of the Artifactory namespace and the name it should be saved as.*
  - `docker push docker.repo1.uhc.com/eutsbi_aiml/sensorscape` *Push the image to your Artifactory namespace*
  - You can check that your image was successfully pushed at [https://repo1.uhc.com/artifactory/docker/eutsbi_aiml/](https://repo1.uhc.com/artifactory/docker/eutsbi_aiml/)

5. Deploy to K8s namespace. Run these commands in the directory all these files are saved in. 
 - `hcpctl login` *Login to HCP console to get access to your resources. If you're making edits skip this step*
 - `bash scripts/hcc-k8s.sh rg-systrack-sensorscape-db36387/naas-v1/aho20-sensorscape` *run this bash script to setup the K8s namespace you provisioned. Run the context command outputted after running this script. If you're making edits skip this step* 
 - `kubectl apply -f k8s.yaml` *We use apply to deploy infrastructure to our k8s namespace.*
 - `alias k=kubectl` *This command is optional.  It just lets you call `kubectl` as `k` instead.*
 - `k get all` *Get a list of all the services, pods, replicasets you just deployed*
 - `k get ingress` *Get the hostname and the ip address of the application you just deployed. You can access the application with the hostname and share with others.* 

# Troubleshooting
1. Check that the k8s namespace still exists. It normally shouldn't be tore down as part of HCP's IaC design principles, but check just in case. Go [here](https://console.hcp.uhg.com/resource-explorer) to your resource explorer to see. 
2. If you get a default-backend 404 error and you did not provision a DNS CNAME or A Record in HCP console, follow these steps:
      - run `kubectl get ingress` inside your k8s namespace context. You'll see the Host domain name and IP address as well as the ingress controller class and name of the ingress policy. 
      - run `nslookup IP_ADDRESS`. If the name reads as "elr-usr101-ingress-prod.optum.com" or has "elr" in the name, reach out to the HCC Kubernetes team for help. Based on your domain, it should be the ctc datacenter that the name should resolve to. This is something that should be investigated by the HCC Kubernetes team.
3. If you cannot connect to Snowflake, this is an issue with the Networkpolicy section of the k8s.yaml file, specifically with egress. In a snowflake worksheet, run `SELECT SYSTEM$WHITELIST();`. With each host, find its IP address and make sure it is within range of a cidr range in the egress whitelist section. 
4. If you recieve an error in your application, the issue maybe with the docker image.
      - make sure that you are building the application as `--platform linux/amd64`. This platform is the only one supported by Kubernetes. If you build your docker image as `docker build -t sensorscape . --platform linux/amd64`, this will ensure this.
      - Test it locally by running the below steps. If it works locally, then there should be no issues with the build when deployed.
           - `docker build -t sensorscape . --platform linux/amd64`
           - `docker run -p 8501:8501 sensorscape`
5. Often times, the Anomaly-Detection.py or load.py programs run into a OOM issue within the K8s pod and will be killed according to the system. If you exec into the pod (`kubectl exec -it pod/**POD NAME** -- /bin/sh`), and run `ps -ef` in the terminal, you will see a `[python] <defunct>` description for the killed process. While inside the pod, you can run `python *Program Name*.py &` to restart it again.

# Future Considerations
1. Currently, Streamlit, the main python package used to create the dashboard and create charts, does not support the ability to call a streamlit application as an HTTP Get Request. This is because streamlit renders the application using Javascript and so any request to the application would require Javascript in order to access any information published to the application. This means while web browsers can render streamlit applications, typical HTTP API calls cannot. This functionality is highly requested by the streamlit community [here](https://github.com/streamlit/streamlit/issues/439). For future work, consider creating a REST API that periodically hits the sensorscape_st page to initiate caching.
2. As additional sensors are added to our analysis, the daily loads for our process will continue to get slower and slower and have a higher risk of failing during our scheduled processes. This is generally because of the limits that we can push on Snowflake and K8s to accomplish our work. As a result, it is important to be grooming our list of sensors that we are analyzing and make sure that we're only including those that we wish to analyze.
3. 

# Change Log
### 1/26 to 2/6
1. From a feedback and review meeting on 1/19, these following items were addressed and resolved:
      - Removed some of the lighter colors and add more distinction in colors within the pie charts.
      - Fixed a data processing issue when passing a file with system ids.
      - Applied grey scaling to snapshot metrics to reduce ambiguity.
      - Added the cosensors to our sensor analysis and fixed data load process to include our derived sensors.
      - Improved cosmetic changes so that redirection from the snapshot page to the sensorscape page are clearer. 
### 12/18 & 1/3
1. Added a "snapshot" for anomaly plotting for each sensor as a home page. Each affected sensor will show a mini plot of the detected anomalies in the past 12 months. Users can interact with the plot and with the sliding bar to adjust the date range. Clicking the on the arrow button will take them to the sensorscape st page where it will pull data and run analytics for the selected sensor and the date range.
2. This process has been optimized by utilizing existing background data loads to preload the data and export the necessary data as csv zip files to later be pulled for analysis.
3. There was a cvss10 vulnerability that was present for awhile that was preventing deployment. Any artifact that has a cvss score of 10 is blocked from download. This was highlighted in Artifactory's scans and alerted through email. However, it seems this particular vulnerability was misscored and actively prevented downloads of many other teams' artifact downloads. It has now since been resolved as of 1/3 by EVMO. Please contact EVMO if another issue like this occurs again or check the EVMO Tea's channel. 
### 11/6
Problem: 
1. Optimizing the current data process so that instanteous data loads can happen for default situations. The current process makes a separate call to Snowflake using the API Python connector for every submission. On average, it takes ~1 - 1:30 minutes for medium sized queries and ~ 5 minutes for larger queries to finish. This takes too long on average despite using Snowpark to have Snowflake handle most of the data transformations. Overall, we want to shorten the load time for the whole process.
2. Integrating the Anomaly Detection modeling with the dashboard. We want to deploy a algorithm to find anomalies among sensors within the current year.

Solution: By increasing the memory and cpu limits for the K8s namespace, a backend Python program can preload the data by making the Snowpark calls and storing the results in CSV files for each sensor. Each sensor's dataframe results can be stored as CSV files which can later on be read into memory later on when a user selects to view a sensor. This is much faster and combined  with Streamlit's caching mechanism, results are near-instaneous. For anomaly detection, the model is pre-ran and results are exported to CSV files as well. This way the model is not run during hours and the data can be stored and read whenever a request to view the data is needed. 
### 9/25 
Problem: Sensors like "Long Add-In Load Time" return an enormous amount of affected WGUIDs for any time period. Snowflake queries are limited to lists up to 16,000. Therefore, anything higher than that will return an error. We still need to query for these sensors so we need to create a workaround for this.
Solution: Use the "eudl_gen" user to create a temporary table of these WGUIDs and modify all subsequent queries to use nested IN statements on the temporary table. Process is stil very slow but will be continued to be monitored as part of optimization efforts. 
