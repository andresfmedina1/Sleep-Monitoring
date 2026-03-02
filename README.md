# Sleep-Monitoring
IoT Sleep Monitoring System

ESP32 + MQTT + Docker Microservices + Telegram

This project implements a distributed IoT system for sleep monitoring using:

ESP32 devices (simulated in Wokwi or physical boards)

MQTT as event bus

Dockerized microservices

Telegram Bot for user interaction

ThingSpeak (optional dashboard integration)

The system automatically switches between Night mode and Day mode based on user-configured wake/sleep times stored in a central Catalog service.

System Architecture
ESP Devices  <--MQTT-->  Broker  <--MQTT-->  Microservices (Docker)
                                     |
                                     └── Telegram Bot

MQTT Broker (public):

test.mosquitto.org:1883

Base topic namespace:

SC/<user>/<room>/...
SC/alerts/<user>/<room>/...
Core Logic
Night Mode

Triggered when current time enters the sleep window.

TimeShift publishes:

sampling -> {"enable": true}

bedtime

Close curtain (servo 0°)

Turn LED off

Sensors begin monitoring.

Day Mode

Triggered when wake time is reached.

TimeShift publishes:

sampling -> {"enable": false}

wakeup -> {"seconds": 30} (alarm duration)

Open curtain (servo 90°)

LED decision based on ambient light

Monitoring disabled

Microservices (Docker)
Catalog

Central configuration service.

Stores:

Users

Rooms

Devices

Services

User wake/sleep times

Threshold parameters

Endpoints:

GET /catalog
GET /users
GET /users/<userID>
GET /rooms
2️⃣ TimeShift

Orchestrates day/night transitions.

Reads:

user_information.timesleep
user_information.timeawake

Publishes:

SC/<user>/<room>/sampling
SC/<user>/<room>/bedtime
SC/<user>/<room>/wakeup
SC/<user>/<room>/LedL
SC/<user>/<room>/servoV
AlarmControl

Consumes telemetry and evaluates thresholds.

Input:

SC/<user>/<room>/hr
SC/<user>/<room>/dht

Output:

SC/alerts/<user>/<room>/hr
SC/alerts/<user>/<room>/dht
TelegramBot

User interface via Telegram.

Features:

Identity verification (phone number stored in Catalog)

Configure wake/sleep times

Configure thresholds

Receives alert messages

Sends notifications only:

On transition to ALERT

Every 120 seconds if ALERT persists

Never on OK

ThingSpeak Bridge (optional)

Connects data streams to ThingSpeak dashboards.

ESP Devices
ESP1 — Heart Rate + Wake Alarm

Publishes HR (SenML)

Receives wakeup command

Activates buzzer + LED during alarm

ESP2 — Temperature & Humidity (DHT)

Publishes temp/humidity (SenML)

Receives alert events

Controls ventilation servo

ESP3 — Ambient Light + Curtain

Publishes light raw value (SenML)

Receives:

LedL (SenML control)

servoV (0° or 90°)

sampling enable/disable

Message Formats
Sampling Control
{"enable": true}
Wakeup Event
{"seconds": 30}
LED Control (SenML)
[
  {"bn":"stateLed","bt":0,
   "e":[{"n":"LedL","u":"bool","vb":true}]}
]
Light Telemetry (SenML)
[
  {"bn":"lightValue","bt":0,
   "e":[{"n":"raw","u":"lm","v":2048}]}
]
How to Run the System
Start Docker Services
docker compose up -d --build

Verify Catalog:

http://localhost:9080/catalog
Start ESP Devices

Connect ESP1, ESP2, ESP3

Ensure MQTT broker matches configuration

Use Telegram Bot

/start

Send registered phone number

Configure wake/sleep times

Configure thresholds

Common Issues
LED toggling repeatedly

Cause:

Multiple publishers writing to the same topic (LedL)

Device echoing its own command

Solution:

Do not re-publish command payload after receiving it

Or separate topics:

LedL/cmd

LedL/state

Sampling not changing

Check:

Correct topic path ({User1} vs User1)

MQTT broker connection

TimeShift timezone configuration

Servo conflict (number vs SenML)

Best practice:

Command topic: servoV/cmd → numeric

State topic: servoV/state → SenML

Suggested Repository Structure
catalog/
timeshift/
alarm/
telegram_bot/
bridge_thingspeak/
common/
docker-compose.yml
README.md
Design Principles

Event-driven architecture

MQTT decoupling

Stateless microservices

Clear separation between:

Command topics

State topics

Telemetry topics

Alert topics

Telegram Messages

Night:

“It’s bedtime. Monitoring is now active. Sleep well.”

Day:

“Time to wake up! Monitoring is disabled. Check your dashboard for results.”

Test Checklist

 Sampling switches correctly at wake/sleep times

 Wake alarm triggers on ESP1

 Curtain moves correctly (0° night / 90° day)

 LED decision based on ambient light

 Alerts generate Telegram notifications

 No topic echo loops
