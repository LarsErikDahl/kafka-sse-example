# kafka-sse-example
Simple proof of concept of drawing on a HTML-canvas backed by kafka

To run the example, you need to have docker and docker-compose installed.

Then, run `docker-compose up` in the root directory of this project. This will start a container from confluent with kafka.

Application.kt is the main class, which starts a server on port 8080. You can draw on the canvas by opening 0.0.0.0:8080 in your browser.