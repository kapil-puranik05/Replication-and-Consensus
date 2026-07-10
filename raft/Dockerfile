FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/*.jar app.jar
COPY configs configs

ENTRYPOINT ["java","-jar","app.jar"]