# Spark Failover POC avec Docker Compose

Un proof of concept (POC) démontrant la gestion du failover automatique avec Apache Spark dans un environnement Docker Compose.

## 📁 Structure du projet

```
spark-failover-poc/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.monitor
├── requirements.txt
├── README.md
├── Makefile
├── .env
├── .gitignore
├── apps/
│   ├── failover_job.py
│   └── __init__.py
├── monitor/
│   ├── monitor.py
│   └── __init__.py
├── data/
│   ├── input/
│   ├── output/
│   └── checkpoints/
└── logs/
```

## 🔧 Fichiers de configuration

### requirements.txt
```txt
pyspark==3.5.0
pandas==2.0.3
requests==2.31.0
flask==2.3.2
watchdog==3.0.0
```

### .env
```bash
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=1G
SPARK_WORKER_CORES=2
COMPOSE_PROJECT_NAME=spark-failover-poc
```

### .gitignore
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter Notebook
.ipynb_checkpoints

# Environment
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Docker
.dockerignore

# Spark
logs/
data/input/
data/output/
data/checkpoints/
*.log
metastore_db/
derby.log
spark-warehouse/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db
```

### Makefile
```makefile
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
```

## 🚀 Installation et utilisation

### 1. Cloner et configurer
```bash
git clone <your-repo>
cd spark-failover-poc
cp .env.example .env  # Ajuster les variables si nécessaire
```

### 2. Démarrer le POC
```bash
# Méthode 1: Avec Makefile
make build
make up

# Méthode 2: Docker Compose direct
docker-compose up -d --build
```

### 3. Accéder aux interfaces

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Monitor Dashboard**: http://localhost:3000
- **API Status**: http://localhost:3000/api/status

### 4. Surveiller les logs
```bash
# Tous les logs
make logs

# Logs spécifiques
make logs-app
make logs-master
make logs-monitor
```

### 5. Tester le failover
```bash
# Forcer un redémarrage de l'application
make restart-app

# Voir le statut
make status

# Tester la connectivité
make test
```
## 6. Interfaces

- **Spark Master UI**: [http://localhost:8080](http://localhost:8080)  
  ![Spark Master Screenshot](./docs/spark-master.png)

- **Spark Worker UI**: [http://localhost:8081](http://localhost:8081)  
  ![Spark Worker Screenshot](./docs/spark-worker-ui.png)

- **Monitor Dashboard**: [http://localhost:3000](http://localhost:3000)  
  ![Dashboard Screenshot](./docs/dashboard.png)

- **API Status**: [http://localhost:3000/api/status](http://localhost:3000/api/status)  
  ![API Status Screenshot](./docs/api-status.png)

## ✨ Fonctionnalités du POC


### ✅ Failover automatique
- Redémarrage automatique en cas de panne
- Backoff exponentiel entre les tentatives
- Limite du nombre de redémarrages

### ✅ Monitoring en temps réel
- Interface web avec dashboard
- API REST pour intégration
- Surveillance des logs
- Métriques des ressources

### ✅ Configuration Docker
- Services isolés
- Réseaux Docker
- Volumes persistants
- Health checks

### ✅ Simulation de pannes
- Pannes aléatoires (30% de chance)
- Gestion des exceptions
- Nettoyage automatique des ressources

## ⚙️ Personnalisation

### Modifier le taux de panne
Dans `apps/failover_job.py`:
```python
self.failure_rate = 0.5  # 50% de chance de panne
```

### Ajuster les ressources
Dans `docker-compose.yml`:
```yaml
environment:
  - SPARK_WORKER_MEMORY=2G
  - SPARK_WORKER_CORES=2
```

### Modifier la fréquence de traitement
Dans `apps/failover_job.py`:
```python
time.sleep(60)  # Attendre 60 secondes entre les cycles
```

## 🔧 Dépannage

### Logs détaillés
```bash
docker-compose logs -f --tail=100 spark-app
```

### Redémarrage complet
```bash
make clean
make build
make up
```

### Vérifier les ports
```bash
netstat -tlnp | grep -E "8080|8081|3000"
```

## 🏭 Production

Pour un usage en production, considérez:

- Utiliser un registry Docker privé
- Configurer des secrets pour les credentials
- Ajouter des ressources limits/requests
- Mettre en place un monitoring externe (Prometheus/Grafana)
- Utiliser un orchestrateur (Kubernetes)

## 📋 Commandes utiles

| Commande | Description |
|----------|-------------|
| `make build` | Construire les images Docker |
| `make up` | Démarrer tous les services |
| `make down` | Arrêter tous les services |
| `make logs` | Voir tous les logs |
| `make status` | Voir le statut des services |
| `make clean` | Nettoyer complètement |
| `make test` | Tester la connectivité |
| `make monitor` | Afficher les URLs des interfaces |

## 📦 Prérequis

- Docker 20.10+
- Docker Compose 1.29+
- Make (optionnel mais recommandé)

## 📝 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## 🤝 Contribution

Les contributions sont les bienvenues ! Veuillez ouvrir une issue ou soumettre une pull request.

## 📞 Support

Pour toute question ou problème, n'hésitez pas à ouvrir une issue sur GitHub.
