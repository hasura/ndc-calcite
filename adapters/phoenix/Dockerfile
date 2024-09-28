# Use an OpenJDK base image
FROM openjdk:8

# Set environment variables
ENV PHOENIX_VERSION=5.1.3
ENV HBASE_VERSION=2.1

# Install necessary tools
RUN apt-get update && apt-get install -y wget

# Download and extract Phoenix
RUN wget https://archive.apache.org/dist/phoenix/phoenix-${PHOENIX_VERSION}/phoenix-hbase-${HBASE_VERSION}-${PHOENIX_VERSION}-bin.tar.gz && \
    tar -xzf phoenix-hbase-${HBASE_VERSION}-${PHOENIX_VERSION}-bin.tar.gz && \
    mv phoenix-${PHOENIX_VERSION} /phoenix

# Copy Phoenix server jar to HBase lib directory
RUN cp /phoenix/phoenix-${PHOENIX_VERSION}-server.jar /opt/hbase/lib/

# Start HBase and Phoenix
CMD /opt/hbase/bin/start-hbase.sh && tail -f /opt/hbase/logs/hbase--master-*.log
