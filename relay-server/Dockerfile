# Build stage: compiles the whole multi-module project, packages relay-server.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY relay-server/pom.xml relay-server/
COPY relay-worker-sdk/pom.xml relay-worker-sdk/
COPY relay-chaos/pom.xml relay-chaos/
COPY relay-bench/pom.xml relay-bench/
RUN mvn -B -q dependency:go-offline -pl relay-server -am
COPY . .
RUN mvn -B -q -DskipTests -pl relay-server -am package

# Runtime stage: minimal JRE.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/relay-server/target/relay-server-*-exec.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
