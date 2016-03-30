FROM anapsix/alpine-java:jdk8
RUN mkdir /app
WORKDIR /app
ADD . /app
# add all of the jars into the image
RUN sbt/sbt update
CMD ["/bin/bash"]
