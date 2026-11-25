
# Build-Phase
FROM gradle:8.7-jdk17 AS build

WORKDIR /workspace

COPY settings.gradle.kts .
COPY build.gradle.kts .
COPY src ./src

RUN gradle --no-daemon installDist

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /workspace/build/install/ds_ex_06/ ./

ENTRYPOINT ["/app/bin/ds_ex_06"]
