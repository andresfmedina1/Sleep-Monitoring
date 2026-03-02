#  IoT Sleep Monitoring System

**ESP32 + MQTT + Docker Microservices + Telegram**

This project implements a distributed IoT system for sleep monitoring
using:

-   ESP32 devices (simulated in Wokwi or physical boards)
-   MQTT as event bus
-   Dockerized microservices
-   Telegram Bot for user interaction
-   ThingSpeak (optional dashboard integration)

The system automatically switches between **Night mode** and **Day
mode** based on user-configured wake/sleep times stored in a central
Catalog service.

------------------------------------------------------------------------

# System Architecture

ESP Devices \<--MQTT--\> Broker \<--MQTT--\> Microservices (Docker) \|
└── Telegram Bot

MQTT Broker (public): test.mosquitto.org:1883

Base topic namespace: SC/`<user>`{=html}/`<room>`{=html}/...
SC/alerts/`<user>`{=html}/`<room>`{=html}/...

------------------------------------------------------------------------

#  Core Logic

## Night Mode

Triggered when current time enters the sleep window.

TimeShift publishes: - sampling -\> {"enable": true} - bedtime - Close
curtain (servo 0°) - Turn LED off

Sensors begin monitoring.

------------------------------------------------------------------------

##  Day Mode

Triggered when wake time is reached.

TimeShift publishes: - sampling -\> {"enable": false} - wakeup -\>
{"seconds": 30} - Open curtain (servo 90°) - LED decision based on
ambient light - Monitoring disabled

------------------------------------------------------------------------

#  Microservices (Docker)

## Catalog

Central configuration service.

Stores: - Users - Rooms - Devices - Services - User wake/sleep times -
Threshold parameters

Endpoints: GET /catalog\
GET /users\
GET /users/`<userID>`{=html}\
GET /rooms

------------------------------------------------------------------------

##  TimeShift

Orchestrates day/night transitions.

Reads: user_information.timesleep\
user_information.timeawake

Publishes: SC/`<user>`{=html}/`<room>`{=html}/sampling\
SC/`<user>`{=html}/`<room>`{=html}/bedtime\
SC/`<user>`{=html}/`<room>`{=html}/wakeup\
SC/`<user>`{=html}/`<room>`{=html}/LedL\
SC/`<user>`{=html}/`<room>`{=html}/servoV

------------------------------------------------------------------------

## AlarmControl

Consumes telemetry and evaluates thresholds.

Input: SC/`<user>`{=html}/`<room>`{=html}/hr\
SC/`<user>`{=html}/`<room>`{=html}/dht

Output: SC/alerts/`<user>`{=html}/`<room>`{=html}/hr\
SC/alerts/`<user>`{=html}/`<room>`{=html}/dht

------------------------------------------------------------------------

##  TelegramBot

User interface via Telegram.

Features: - Identity verification (phone number stored in Catalog) -
Configure wake/sleep times - Configure thresholds - Receives alert
messages - Sends notifications only: - On transition to ALERT - Every
120 seconds if ALERT persists - Never on OK

------------------------------------------------------------------------

##  ThingSpeak Bridge (optional)

Connects data streams to ThingSpeak dashboards.

------------------------------------------------------------------------

#  ESP Devices

## ESP1 --- Heart Rate + Wake Alarm

-   Publishes HR (SenML)
-   Receives wakeup command
-   Activates buzzer + LED during alarm

------------------------------------------------------------------------

## ESP2 --- Temperature & Humidity (DHT)

-   Publishes temp/humidity (SenML)
-   Receives alert events
-   Controls ventilation servo

------------------------------------------------------------------------

## ESP3 --- Ambient Light + Curtain

-   Publishes light raw value (SenML)
-   Receives:
    -   LedL (SenML control)
    -   servoV (0° or 90°)
    -   sampling enable/disable

------------------------------------------------------------------------

#  How to Run the System

##  Start Docker Services

docker compose up -d --build

Verify Catalog: http://localhost:9080/catalog

------------------------------------------------------------------------

## Start ESP Devices

-   Connect ESP1, ESP2, ESP3
-   Ensure MQTT broker matches configuration

------------------------------------------------------------------------

##  Use Telegram Bot

1.  /start
2.  Send registered phone number
3.  Configure wake/sleep times
4.  Configure thresholds

 LED decision based on ambient light

 Alerts generate Telegram notifications

 No topic echo loops
