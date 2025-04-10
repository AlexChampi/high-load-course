#FROM maven:3.9.9-eclipse-temurin-17 AS build
#
#WORKDIR /app
#COPY pom.xml .
#RUN mvn dependency:go-offline
#COPY src src
#RUN mvn package
#
#FROM openjdk:17-jdk-slim
#
#COPY --from=build /app/target/*.jar /high-load-course.jar
#
#CMD ["java", "-jar", "/high-load-course.jar"]
#
# Базовый образ с Java 17
FROM openjdk:17-jdk-slim

# Папка внутри контейнера, куда скопируем jar
WORKDIR /app

# Копируем готовый jar из локальной машины
COPY target/*.jar high-load-course.jar

# Команда запуска
CMD ["java", "-jar", "high-load-course.jar"]
