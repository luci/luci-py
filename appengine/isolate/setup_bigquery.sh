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

echo "- Create the dataset:"
echo ""
echo "  Warning: On first 'bq' invocation, it'll try to find out default"
echo "    credentials and will ask to select a default app; just press enter to"
echo "    not select a default."

# Optional: --default_table_expiration 63244800
bq --location=US mk --dataset \
  --description 'Isolate server statistics' ${APPID}:isolated


echo "- Populate the BigQuery schema:"
echo ""
echo "  Warning: On first 'bqschemaupdater' invocation, it'll request default"
echo "    credentials which is stored independently than 'bq'."
cd proto
bqschemaupdater -message isolated.StatsSnapshot -table ${APPID}.isolated.stats
cd ..

# TODO(maruel): The stock role "roles/bigquery.dataEditor" grants too much
# rights. Create a new custom role with only access
# "bigquery.tables.updateData".
#echo "- Create a BQ write-only role account:"
# https://cloud.google.com/iam/docs/understanding-custom-roles


# https://cloud.google.com/iam/docs/granting-roles-to-service-accounts
# https://cloud.google.com/bigquery/docs/access-control
echo "- Grant access to the AppEngine app to the role account:"
roles/bigquery.user
gcloud projects add-iam-policy-binding ${APPID} \
    --member serviceAccount:${APPID}@appspot.gserviceaccount.com \
    --role roles/bigquery.dataEditor
