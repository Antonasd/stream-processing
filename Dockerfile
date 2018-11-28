FROM maven:3.5.2-jdk-8-slim as builder

WORKDIR /shared
ADD . /shared

RUN mvn dependency:resolve
RUN mvn verify

FROM openjdk:8-jre-slim

WORKDIR /shared
COPY --from=builder /shared/target /shared/target