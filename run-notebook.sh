#!/bin/sh

docker run -v ${PWD}:/home/jovyan/work --env-file .env -e SPARK_OPTS="--driver-memory 4g --jars target/scala-2.11/Pipeline.jar" -it --rm -p 8888:8888 jupyter/all-spark-notebook start-notebook.sh --NotebookApp.token=
