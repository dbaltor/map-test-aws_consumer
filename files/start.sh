#!/bin/bash
set -e
exec readfile.sh test1.csv | java -Djava.security.egd=file:/dev/./urandom -jar /app.jar both 100 2