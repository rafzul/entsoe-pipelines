#!/bin/sh
# new-entrypoint.sh

#install requirements
pip3 install -r /opt/requirements_spark.txt

# run the original entrypoint; make sure to pass the CMD along
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@"