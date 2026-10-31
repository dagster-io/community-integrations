# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.9-bullseye

# Debian bullseye is end-of-life and mid-archival. Its .deb files have been
# withdrawn from deb.debian.org, and bullseye-security has not appeared on
# archive.debian.org yet. Both suites have to stay in play. Security ships
# newer builds than main (libcurl4 u16 vs u13, openjdk-11 11.0.32 vs 11.0.24),
# and this base image already has the security ones installed, so main alone
# leaves apt unable to satisfy them. snapshot.debian.org still serves both at
# a fixed date, which pins the image to one consistent set. Snapshot Release
# files are expired by definition, hence Check-Valid-Until.
RUN printf '%s\n' \
      'deb http://snapshot.debian.org/archive/debian/20260901T000000Z bullseye main' \
      'deb http://snapshot.debian.org/archive/debian-security/20260901T000000Z bullseye-security main' \
      > /etc/apt/sources.list && \
    rm -f /etc/apt/sources.list.d/*.list && \
    apt-get -o Acquire::Check-Valid-Until=false -qq update && \
    apt-get -q install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/*

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME} && mkdir -p /home/iceberg/spark-events
WORKDIR ${SPARK_HOME}

# Remember to also update `tests/conftest`'s spark setting
ENV SPARK_VERSION=3.5.8
ENV ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12
ENV ICEBERG_VERSION=1.8.0
ENV PYICEBERG_VERSION=0.8.1

RUN curl --retry 5 -fSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download iceberg spark runtime
RUN curl --retry 5 -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar \
      -Lo /opt/spark/jars/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar


# Download AWS bundle
RUN curl --retry 5 -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Download PostgreSQL JDBC driver
RUN curl --retry 5 -s https://jdbc.postgresql.org/download/postgresql-42.7.5.jar \
      -Lo /opt/spark/jars/postgresql-42.7.5.jar

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

RUN pip3 install -q ipython

RUN pip3 install "pyiceberg[sql-postgres,pyarrow]==${PYICEBERG_VERSION}"

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
