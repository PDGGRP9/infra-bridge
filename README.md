# MQTT Bridge

Ce repo contient le service qui consomme les trames MQTT des bracelets et les persiste dans PostgreSQL.

Variables d'environnement attendues:

- `MQTT_HOST`, `MQTT_PORT`, `MQTT_USER`, `MQTT_PASSWORD`
- `MQTT_TOPIC`
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

L'image publiée est consommée par l'orchestrator via `ghcr.io/pdggrp9/infra-bridge:latest`.