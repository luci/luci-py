#!/bin/sh
# Copyright 2018 The LUCI Authors. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

set -eu

cd "$(dirname $0)"

if ! (which bq) > /dev/null; then
  echo "Please install 'bq' from gcloud SDK"
  echo "  https://cloud.google.com/sdk/install"
  exit 1
fi

if ! (which bqschemaupdater) > /dev/null; then
  echo "Please install 'bqschemaupdater' from Chrome's infra.git"
  echo "  Checkout infra.git then run: eval \`./go/env.py\`"
  exit 1
fi

if [ $# != 1 ]; then
  echo "usage: setup_bigquery.sh <instanceid>"
  echo ""
  echo "Pass one argument which is the instance name"
  exit 1
fi

APPID=$1

echo "- Make sure the BigQuery API is enabled for the project:"
# It is enabled by default for new projects, but it wasn't for older projects.
gcloud services enable --project ${APPID} bigquery-json.googleapis.com


# TODO(maruel): The stock role "roles/bigquery.dataEditor" grants too much
# rights. Create a new custom role with only access
# "bigquery.tables.updateData".
#echo "- Create a BQ write-only role account:"
# https://cloud.google.com/iam/docs/understanding-custom-roles


# https://cloud.google.com/iam/docs/granting-roles-to-service-accounts
# https://cloud.google.com/bigquery/docs/access-control
echo "- Grant access to the AppEngine app to the role account:"
gcloud projects add-iam-policy-binding ${APPID} \
    --member serviceAccount:${APPID}@appspot.gserviceaccount.com \
    --role roles/bigquery.dataEditor


echo "- Create the dataset:"
echo ""
echo "  Warning: On first 'bq' invocation, it'll try to find out default"
echo "    credentials and will ask to select a default app; just press enter to"
echo "    not select a default."

# Optional: --default_table_expiration 63244800
if ! (bq --location=US mk --dataset \
  --description 'Swarming statistics' ${APPID}:swarming); then
  echo ""
  echo "Dataset creation failed. Assuming the dataset already exists. At worst"
  echo "the following command will fail."
fi


echo "- Populate the BigQuery schema:"
echo ""
echo "  Warning: On first 'bqschemaupdater' invocation, it'll request default"
echo "    credentials which is stored independently than 'bq'."
cd proto/api
if ! (bqschemaupdater -force \
    -message swarming.v1.BotEvent \
    -table ${APPID}.swarming.bot_events \
    -partitioning-field event_time); then
  echo ""
  echo ""
  echo "Oh no! You may need to restart from scratch. You can do so with:"
  echo ""
  echo "  bq rm ${APPID}:swarming.bot_events"
  echo ""
  echo "and run this script again."
  exit 1
fi
if ! (bqschemaupdater -force \
    -message swarming.v1.TaskRequest \
    -table ${APPID}.swarming.task_requests \
    -partitioning-field created_time); then
  echo ""
  echo ""
  echo "Oh no! You may need to restart from scratch. You can do so with:"
  echo ""
  echo "  bq rm ${APPID}:swarming.task_requests"
  echo ""
  echo "and run this script again."
  exit 1
fi
if ! (bqschemaupdater -force \
    -message swarming.v1.TaskResult \
    -table ${APPID}.swarming.task_results \
    -partitioning-field completed_time); then
  echo ""
  echo ""
  echo "Oh no! You may need to restart from scratch. You can do so with:"
  echo ""
  echo "  bq rm ${APPID}:swarming.task_results"
  echo ""
  echo "and run this script again."
  exit 1
fi
cd -
