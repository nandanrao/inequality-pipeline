#!/bin/sh

spark-submit \
--driver-memory 4g \
--class geotrellis.spark.etl.SinglebandIngest \
etl.jar \
--backend-profiles "etl/backend-profiles.json" \
--input "etl/input.json" \
--output "etl/output.json"
