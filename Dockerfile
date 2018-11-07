FROM openjdk:8-jdk-alpine
RUN apk add --no-cache bash #add bash to image
VOLUME /tmp
ARG JAR_FILE
COPY ${JAR_FILE} app.jar
COPY *.csv /
COPY readfile.sh /usr/local/bin/
RUN ln -s usr/local/bin/readfile.sh / # backwards compat
COPY start.sh /usr/local/bin/
RUN ln -s usr/local/bin/start.sh / # backwards compat
ENTRYPOINT ["start.sh"]

EXPOSE 8080
