FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*
# Configuration Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Installation des dépendances Python
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas \
    requests \
    flask

# Création du répertoire de travail
WORKDIR /app

# Copie des fichiers d'application
COPY apps/ /app/
COPY requirements.txt /app/

# Installation des dépendances supplémentaires
RUN pip install --no-cache-dir -r requirements.txt

# Utilisateur non-root pour la sécurité
RUN useradd -m -u 1001 spark-user && chown -R spark-user:spark-user /app
USER spark-user

# Commande par défaut
CMD ["python", "failover_job.py"]