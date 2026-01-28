# Smart City Data Pipeline (Stream Processing)

## Project Overview
This is my project for the Data Engineering course (DLBDSEDE02). The goal was to build a pipeline 
that can handle real-time sensor data for a "Smart City." Instead of just using a static dataset, 
I wanted to simulate a live environment where things actually change.

I built an **Event-Driven** system using **Apache Kafka** and **MongoDB**. To make the data 
interesting, I wrote a custom Python simulation that mimics different scenarios—like a normal 
day, a heat wave, or even a "Zombie Apocalypse" (where bio-hazard levels spike).

The whole thing is containerized with Docker, so it runs on my local machine without needing 
to install Kafka or Mongo manually.

## System Architecture

```mermaid
graph LR
    A[Producer Service<br/>(Python + Faker)] -->|Generates JSON Data| B(Kafka Broker<br/>Topic: sensor-data)
    B -->|Streams Data| C[Consumer Service<br/>(Python + Pymongo)]
    C -->|Validates & Upserts| D[(MongoDB<br/>Database)]
    E[Zookeeper] -.->|Manages| B

## How it Works
The pipeline has three main parts that talk to each other:

1.  **The Producer (Sensor Simulation):** I wrote a Python script (`producer.py`) that acts like thousands of IoT sensors. It uses the `Faker` library to generate data. I also added an "Event Engine" that changes the data patterns every few cycles (e.g., traffic drops during a "COVID_LOCKDOWN" event).
2.  **The Backbone (Kafka):** This handles the stream. I used Zookeeper and a Kafka Broker to make sure the data moves reliably from the producer to the database.
3.  **The Consumer (MongoDB Sink):** This script listens to the Kafka topic and saves the JSON records into a MongoDB database so we can query them later.

**Tech Stack:**
* **Language:** Python 3.9
* **Streaming:** Apache Kafka 7.4
* **Database:** MongoDB 6.0
* **Tools:** Docker & Docker Compose

## Prerequisites
You just need **Docker Desktop** installed and running.

## How to Run It (Quick Start)

I set this up to be as easy as possible to run. Just follow these steps:

1.  **Clone the repo:**
    git clone [https://github.com/Brandyn-Ewanek/smart-city-pipeline](https://github.com/Brandyn-Ewanek/smart-city-pipeline) cd smart-city-pipeline

2.  **Start the environment:**
    Run this command to build the Python images and start the containers.
    *Note: It might take a minute the first time to download the images.*
    docker compose up --build

3.  **Watch the data:**
    You will see logs scrolling in the terminal. Watch for the event changes!
    * You'll see messages like: `[ZOMBIE_APOCALYPSE] Sent data for Industrial District...`
    * The consumer will print: `Stored record from Industrial District`

4.  **Check the Database (Optional):**
    If you have MongoDB Compass, you can connect to `localhost:27017` to see the actual documents landing in the `smart_city_db` database.

5.  **Stop everything:**
    To shut it down cleanly, just hit `Ctrl+C` or run:
    docker compose down

## Project Files
* `app/producer.py`: The script that generates the fake sensor data and events.
* `app/consumer.py`: The script that saves data to Mongo.
* `docker-compose.yml`: The blueprint that sets up the whole network.

## Future Plans
Right now this runs locally on Docker, but I designed it so it could be moved to AWS pretty easily. The plan would be to swap the local Kafka container for **Amazon MSK** and host the producer script on **AWS Fargate** or Lambda.