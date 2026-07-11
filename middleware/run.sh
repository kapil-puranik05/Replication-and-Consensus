gnome-terminal -- bash -c '
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=9000";
exec bash
'