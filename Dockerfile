FROM maven:3.8-eclipse-temurin-21-alpine AS builder

WORKDIR /home

ARG MAVEN_GITHUB_TOKEN
ARG MAVEN_GITHUB_ORG

ENV MAVEN_GITHUB_TOKEN=$MAVEN_GITHUB_TOKEN
ENV MAVEN_GITHUB_ORG=$MAVEN_GITHUB_ORG

RUN test -n "$MAVEN_GITHUB_TOKEN" || (echo "Error: MAVEN_GITHUB_TOKEN cannot be empty" && exit 1)
RUN test -n "$MAVEN_GITHUB_ORG" || (echo "Error: MAVEN_GITHUB_ORG cannot be empty" && exit 1)

# Copy and Build Deduplicator
WORKDIR /home
COPY ./jpo-deduplicator/pom.xml ./jpo-deduplicator/
COPY ./jpo-deduplicator/settings.xml ./jpo-deduplicator/

WORKDIR /home/jpo-deduplicator
RUN mvn -s settings.xml dependency:resolve

COPY ./jpo-deduplicator/src ./src
RUN mvn -s settings.xml install -DskipTests

FROM amazoncorretto:21

WORKDIR /home

COPY --from=builder /home/jpo-deduplicator/src/main/resources/application.yaml /home
COPY --from=builder /home/jpo-deduplicator/src/main/resources/logback.xml /home
COPY --from=builder /home/jpo-deduplicator/target/jpo-deduplicator.jar /home

#COPY cert.crt /home/cert.crt
#RUN keytool -import -trustcacerts -keystore /usr/local/openjdk-11/lib/security/cacerts -storepass changeit -noprompt -alias mycert -file cert.crt

ENTRYPOINT ["java", \
    "-Djava.rmi.server.hostname=$DOCKER_HOST_IP", \
    "-Dcom.sun.management.jmxremote.port=9090", \
    "-Dcom.sun.management.jmxremote.rmi.port=9090", \
    "-Dcom.sun.management.jmxremote", \
    "-Dcom.sun.management.jmxremote.local.only=true", \
    "-Dcom.sun.management.jmxremote.authenticate=false", \
    "-Dcom.sun.management.jmxremote.ssl=false", \
    "-Dlogback.configurationFile=/home/logback.xml", \
    "-jar", \
    "/home/jpo-deduplicator.jar"]

# ENTRYPOINT ["tail", "-f", "/dev/null"]
