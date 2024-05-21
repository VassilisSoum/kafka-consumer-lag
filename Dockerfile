FROM eclipse-temurin:17.0.11_9-jdk-jammy
EXPOSE 9999
ARG JAR_FILE=target/scala-2.13/example.jar
COPY ${JAR_FILE} /app/example.jar
WORKDIR /app
ENTRYPOINT ["java","-jar","example.jar"]