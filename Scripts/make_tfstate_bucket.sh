#!/bin/sh

# check that we're logged in and using a DCE account
echo "Checking account/login..."
if [ $(gcloud auth list 2>/dev/null | grep ^* | grep dce-user-sa | wc -l) -ne 1 ]; then
    echo
    echo "ERROR:"
    echo "  Not authenticated to GCP DCE, or DCE service acocunt (dce-user-sa) is not set as default authentication."
    echo "  HINT: Have you run 'gcloud auth activate-service-account --key-file=dce-user-sa.json'?"
    echo
    exit 10
else
    echo "  account is good."
fi

# check that current project is the correct DCE one
echo "Checking projects..."
MY_DCE_PROJECT=$(gcloud projects list 2>/dev/null | grep dce-prod-pool | awk '{print $1}')
G_CURR_PROJECT=$(gcloud config get project)
if [ "$MY_DCE_PROJECT" != "$G_CURR_PROJECT" ]; then
    echo
    echo "ERROR:"
    echo "  GCloud (config) project doesn't match DCE projects available."
    echo "  HINT: Have you set your DCE project correctly like 'gcloud config set project $MY_DCE_PROJECT'?"
    exit 20
else
    echo "  project looks good."
fi

# use last part of GCP Project as unique string for bucket name
RANDOM_STRING=$(echo $MY_DCE_PROJECT | cut -d- -f5)

# set Google Storage bucket (for Terraform state) name
BUCKET_NAME="tdp-tfstate-$RANDOM_STRING"

# check if bucket already exists; if not make bucket
echo "Checking to see if bucket exists..."
if [ $(gsutil ls | grep gs://$BUCKET_NAME/ | wc -l) -ge 1 ]; then
    echo "  bucket has already been created... skipping."
    echo "  Proceed to configuring Terraform to use bucket name \"$BUCKET_NAME\""
else
    echo "  Creating bucket $BUCKET_NAME..."
    gsutil mb gs://$BUCKET_NAME
    ERR=$?
    if [ $ERR -ne 0 ]; then
        echo
        echo "ERROR:"
        echo "  Error running bucket creation; please ask for help."
        echo "  If name is not globally unique, edit this script and add random bits to BUCKET_NAME string and try again."
        echo
        exit 30
    fi
    echo "  success.  Proceed to configuring Terraform to use this bucket name \"$BUCKET_NAME\""
fi
