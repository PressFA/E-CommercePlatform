FROM gradle:9.2.1-jdk21 AS build
WORKDIR /app/

COPY gradle/wrapper /app/gradle/wrapper/
COPY gradlew /app/
COPY build.gradle /app/
COPY settings.gradle /app/
RUN --mount=type=secret,id=github_token \
    GITHUB_TOKEN=$(cat /run/secrets/github_token) \
    gradle dependencies --no-daemon

COPY src /app/src/
RUN --mount=type=secret,id=github_token \
    GITHUB_TOKEN=$(cat /run/secrets/github_token) \
    gradle build --no-daemon -x test

FROM eclipse-temurin:21-jre-alpine
COPY --from=build /app/build/libs/*.jar /app/shoppingcart.jar
ENTRYPOINT ["java", "-jar", "/app/shoppingcart.jar"]