.PHONY: build up down logs clean restart status

# Variables
COMPOSE_FILE = docker-compose.yml
PROJECT_NAME = spark-failover-poc

# Construire et démarrer
build:
	docker-compose -f $(COMPOSE_FILE) build

up:
	docker-compose -f $(COMPOSE_FILE) up -d

# Démarrer avec logs
up-logs:
	docker-compose -f $(COMPOSE_FILE) up

# Arrêter
down:
	docker-compose -f $(COMPOSE_FILE) down

# Voir les logs
logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

# Logs d'un service spécifique
logs-app:
	docker-compose -f $(COMPOSE_FILE) logs -f spark-app

logs-master:
	docker-compose -f $(COMPOSE_FILE) logs -f spark-master

logs-worker:
	docker-compose -f $(COMPOSE_FILE) logs -f spark-worker

logs-monitor:
	docker-compose -f $(COMPOSE_FILE) logs -f monitor

# Statut des services
status:
	docker-compose -f $(COMPOSE_FILE) ps

# Redémarrer un service
restart-app:
	docker-compose -f $(COMPOSE_FILE) restart spark-app

restart-all:
	docker-compose -f $(COMPOSE_FILE) restart

# Nettoyer
clean:
	docker-compose -f $(COMPOSE_FILE) down -v
	docker system prune -f
	sudo rm -rf data/input/* data/output/* logs/*

# Ouvrir une session dans le conteneur
shell-app:
	docker-compose -f $(COMPOSE_FILE) exec spark-app bash

shell-master:
	docker-compose -f $(COMPOSE_FILE) exec spark-master bash

# Tester la connectivité
test:
	curl -f http://localhost:8080 && echo "✓ Spark Master OK"
	curl -f http://localhost:8081 && echo "✓ Spark Worker OK"
	curl -f http://localhost:3000 && echo "✓ Monitor OK"

# Développement
dev-build:
	docker-compose -f $(COMPOSE_FILE) build --no-cache

dev-up:
	docker-compose -f $(COMPOSE_FILE) up --build

# Monitoring
monitor:
	@echo "📊 Interfaces disponibles:"
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark Worker UI: http://localhost:8081"
	@echo "Monitor Dashboard: http://localhost:3000"
	@echo "API Status: http://localhost:3000/api/status"