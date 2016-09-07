## -*- docker-image-name: "lshift.de/rammler" -*-

FROM 557447048791.dkr.ecr.eu-central-1.amazonaws.com/lshift.de/fedora:24
MAINTAINER https://lshift.de/

RUN dnf -y install java-1.8.0-openjdk-headless; dnf clean all
RUN alternatives --auto java
RUN mkdir /var/log/rammler
COPY ../target/uberjar/rammler-*-standalone.jar /rammler.jar
EXPOSE 5671 5672
ENTRYPOINT ["java", "-jar", "rammler.jar"]
