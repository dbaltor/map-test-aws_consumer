# Denis map-test AWS Consumer 2018
Test map application developed with Spring Boot and AWS SQS (Simple Queue Service) and SNS (Simple Notification Service).

A small application that updates the UK map based on 2 independent continuous streams of events: 
1) around 20000 risk areas (Heat map) updated every 10 seconds.
2) 100 moving vehicles updated every 2 seconds.

The risk areas are generated by the companion application "Denis map-test AWS Supplier" and transmitted asynchronously via AWS SNS and SQS services.

*server command line:
=====================
java -jar build/libs/heatmap-aws_consumer-1.0.0.jar {lab} {vehicles} {rate} {inputFile}
<br>*Lab = Type of simulation. Valid Values: 1, 2, both. Default: both
<br>*Vehicles = Number of vehicles to track. Defaul: 10
<br>*Rate = Vehicles real refresh interval in seconds. Default: 60
<br>*inputFile = Name of the file containing the vehicles' positions. Default: ./files/realtimelocation.csv
<br>*example: java -jar build/libs/heatmap-aws_consumer-1.0.0.jar both 100 2 ./files/realtimelocation.csv

*client URL:
============
localhost:8080





