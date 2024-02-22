#!/bin/bash
#
# Script to set up kubectl for HCC Kubernetes auto-magically.  This script
# can be run multiple times without issue, and if same namespace/cluster
# names are reused, it will overwrite kube config for the combination.
#
# See https://prm-docs.optum.com/usage/consumer-ns/ to install hcpctl (the
# command-line tool for PRM)
#
############################################################################

# Check arguments
PRM_NAMESPACE=$(echo "$1" | cut -d/ -f1)
PRM_KIND=$(echo "$1" | cut -d/ -f2)
PRM_K8S_OBJECT=$(echo "$1" | cut -d/ -f3)

usage() {
  echo "Usage: $0 <prm_k8s_id>"
  echo
  echo "   Where prm_k8s_id is in form <namespace>/naas-v1/<object>"
  echo "   (i.e. \"Id\" column from \`hcpctl ls\` output)"
  echo
  exit 100
}

cleanup() {
  rm -f $TMPFILE
}

chk_val() {
  if [ -z "$1" ]; then
    return 1
  else
    return 0
  fi
}

prob() {
  echo "Problem running $1 (error $2)... exiting."
  exit $2
}

trap cleanup EXIT

chk_val $PRM_NAMESPACE || usage
chk_val $PRM_KIND || usage
chk_val $PRM_K8S_OBJECT || usage

# Check hcpctl
hcpctl -v | grep version >/dev/null 2>&1
ERR=$?
[[ $ERR -eq 0 ]] || prob hcpctl 10

# Check base64
base64 --decode </dev/null >/dev/null 2>/dev/null
ERR=$?
[[ $ERR -eq 0 ]] || prob base64 30

TMPFILE=$(mktemp -q)
if [ $? -ne 0 ]; then
  echo "$0: Can't create temp file, exiting."
  exit 99
fi

echo "Getting info from PRM..."
hcpctl cat ${PRM_NAMESPACE}/naas-v1/${PRM_K8S_OBJECT} -o json >$TMPFILE

ERR=$?
[[ $ERR -eq 0 ]] || prob hcpctl 20

CLUSTER_NAME=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .cluster')
chk_val $CLUSTER_NAME || prob jq 31
CA_BASE64=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .["ca-cert"]')
chk_val $CA_BASE64 || prob jq 32
SERVER_URL=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .["server-url"]')
if [ "x${SERVER_URL}" == "x" -o "${SERVER_URL}" == "null" ]; then
  # This changed as of naas agent 3.0.0, but need to catch "old" namespaces (i.e. that still use server-ip key)
  #   See https://github.optum.com/HCC-Container-Platform/hcc-naas-agent/wiki#release-300
  SERVER_URL=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .["server-ip"]')
fi
chk_val $SERVER_URL || prob jq 33
SA_NAME=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .["account-name"]')
chk_val $SA_NAME || prob jq 34
SA_TOKEN=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .token')
chk_val $SA_TOKEN || prob jq 35
K8S_NAMESPACE=$(cat $TMPFILE | jq -r '.status | .["naas-agent"] | .["access-details"] | .["k8s-namespace"]')
chk_val $K8S_NAMESPACE || prob jq 36

echo "Creating .kube directory..."
mkdir -p ${HOME}/.kube && chmod 0700 ${HOME}/.kube

echo "Dumping k8s cluster CA cert (PEM format) to a file..."
[[ -z ${CLUSTER_NAME} ]] || echo ${CA_BASE64} | base64 --decode >$HOME/.kube/${CLUSTER_NAME}.crt

echo "Creating entry in ~/.kube/config for the k8s cluster..."
kubectl config set-cluster "${CLUSTER_NAME}" --server="${SERVER_URL}" --certificate-authority="${HOME}/.kube/${CLUSTER_NAME}.crt"

echo "Creating entry in ~/.kube/config for the default service account..."
kubectl config set-credentials "${SA_NAME}" --token="${SA_TOKEN}"

# Establish a kubectl context name (this name can be anything you'd like)
CONTEXT_NAME="${CLUSTER_NAME}_${K8S_NAMESPACE}_${SA_NAME}"

echo "Creating entry in ~/.kube/config for kubectl context..."
kubectl config set-context "${CONTEXT_NAME}" --cluster="${CLUSTER_NAME}" --namespace="${K8S_NAMESPACE}" --user="${SA_NAME}"

echo
echo "*******  SUCCESS  *******"
echo
echo "kubectl is now configured for your HCC Kubernetes namespace. To use kubectl, first set your context:"
echo
echo "  kubectl config use-context ${CONTEXT_NAME}"
echo
echo "and then run kubectl commands:"
echo
echo "  kubectl get pods"
echo
