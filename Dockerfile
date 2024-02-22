# app/Dockerfile

# make sure that this is a supported version of python
FROM docker-hub.repo1.uhc.com/python:3.9
# VOLUME [ "/files" ]
#image maintiner 
LABEL maintainer="aaron_ho@optum.com"

# set the working directory for any additional RUN,CMD,ADD commands
WORKDIR /app

# copy files same as directory as dockerfile to WORKDIR
COPY . .

# copy rsa key to appropriate directory in container
COPY rsa_key.p8 /Users/aho20/Project/Sensorstreamlitt/key/
COPY rsa_key.pub /Users/aho20/Project/Sensorstreamlitt/key/

# install dependencies 
RUN pip3 install -r requirement.txt

# expose streamlit default port 
EXPOSE 8501

#Run background scripts
RUN chmod a+x background.sh
# CMD ["./background.sh"]

# first command to start streamlit app, pass these specific commands as parameters to run on a specific server localhost port, rest is to make compliant of Mac M1 arm64 to linux/amd64
# ENTRYPOINT ["streamlit", "run","--server.port=8501", "--server.address=0.0.0.0", "Home.py","--server.fileWatcherType", "none", "--browser.gatherUsageStats", "false"]
ENTRYPOINT ["./background.sh"]