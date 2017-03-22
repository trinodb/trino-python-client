FROM openjdk:8-jre
EXPOSE 8080

MAINTAINER Greg Leclercq "ggreg@fb.com"
ARG PRESTO_VERSION=0.167
ENV PRESTO_PKG presto-server-$PRESTO_VERSION.tar.gz
ENV PRESTO_PKG_URL https://repo1.maven.org/maven2/com/facebook/presto/presto-server/$PRESTO_VERSION/$PRESTO_PKG

ENV PRESTO_CLI_JAR_URL https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/$PRESTO_VERSION/presto-cli-$PRESTO_VERSION-executable.jar


# Install python to run the launcher script
RUN apt-get update
RUN apt-get install -y python less

# Download Presto package
# Use curl rather ADD <remote> to leverage RUN caching
# Let curl show progress bar to prevent Travis from thinking the job is stalled
RUN curl -o /$PRESTO_PKG $PRESTO_PKG_URL
RUN tar -zxf /$PRESTO_PKG

# Create directory for Presto data
RUN mkdir -p /var/lib/presto/data

# Add Presto configuration
WORKDIR /presto-server-$PRESTO_VERSION
RUN mkdir etc
ADD etc/jvm.config etc/
ADD etc/config.properties etc/
ADD etc/node.properties etc/
ADD etc/catalog etc/catalog

# Download Presto click
RUN mkdir -p bin
RUN curl -o bin/presto-cli $PRESTO_CLI_JAR_URL
RUN chmod +x bin/presto-cli

CMD bin/launcher.py run
