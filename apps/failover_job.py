#!/usr/bin/env python3
"""
Job Spark avec mécanisme de failover et auto-restart
POC pour démonstration Docker Compose
"""

import os
import time
import random
import logging
import signal
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/spark_app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SparkFailoverJob:
    def __init__(self):
        self.spark: Optional[SparkSession] = None
        self.running = False
        self.restart_count = 0
        self.max_restarts = 5
        self.failure_rate = 0.3  # 30% de chance de panne
        
    def create_spark_session(self) -> bool:
        """Créer une session Spark avec configuration optimisée"""
        try:
            self.spark = SparkSession.builder \
                .appName("FailoverPOC") \
                .master(os.getenv("SPARK_MASTER_URL", "local[*]")) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.dynamicAllocation.enabled", "false") \
                .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints") \
                .getOrCreate()
            
            # Configuration du niveau de log
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("✓ Session Spark créée avec succès")
            return True
            
        except Exception as e:
            logger.error(f"✗ Erreur création session Spark: {e}")
            return False
    
    def generate_sample_data(self):
        """Générer des données d'exemple pour le traitement"""
        try:
            # Données d'exemple : commandes e-commerce
            orders_data = []
            for i in range(1000):
                orders_data.append({
                    'order_id': f'ORD_{i:04d}',
                    'customer_id': f'CUST_{random.randint(1, 100):03d}',
                    'product_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home']),
                    'amount': round(random.uniform(10.0, 500.0), 2),
                    'timestamp': datetime.now().isoformat()
                })
            
            # Créer le DataFrame
            schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(orders_data)
            
            # Sauvegarder les données sources
            df.write \
              .mode("overwrite") \
              .option("header", "true") \
              .csv("/data/input/orders")
            
            logger.info(f"✓ Données générées: {df.count()} commandes")
            return df
            
        except Exception as e:
            logger.error(f"✗ Erreur génération données: {e}")
            raise
    
    def process_data(self, df):
        """Traiter les données avec possibilité de panne"""
        try:
            # Simuler une panne aléatoire
            if random.random() < self.failure_rate:
                raise Exception("Panne simulée du traitement de données")
            
            # Traitement des données
            logger.info("Début du traitement des données...")
            
            # Analyses par catégorie
            category_analysis = df.groupBy("product_category") \
                                 .agg(
                                     count("*").alias("total_orders"),
                                     avg("amount").alias("avg_amount"),
                                     spark_max("amount").alias("max_amount")
                                 ) \
                                 .orderBy("total_orders", ascending=False)
            
            # Analyses par client
            customer_analysis = df.groupBy("customer_id") \
                                 .agg(
                                     count("*").alias("total_orders"),
                                     avg("amount").alias("avg_amount")
                                 ) \
                                 .filter(col("total_orders") > 5) \
                                 .orderBy("total_orders", ascending=False)
            
            # Sauvegarder les résultats
            category_analysis.write \
                            .mode("overwrite") \
                            .option("header", "true") \
                            .csv("/data/output/category_analysis")
            
            customer_analysis.write \
                            .mode("overwrite") \
                            .option("header", "true") \
                            .csv("/data/output/customer_analysis")
            
            logger.info("✓ Traitement terminé avec succès")
            logger.info(f"  - Analyses par catégorie: {category_analysis.count()} lignes")
            logger.info(f"  - Analyses par client: {customer_analysis.count()} lignes")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Erreur traitement: {e}")
            raise
    
    def health_check(self) -> bool:
        """Vérifier l'état de santé de Spark"""
        try:
            if not self.spark:
                return False
                
            # Test simple de connectivité
            test_df = self.spark.range(1)
            test_df.count()
            return True
            
        except Exception:
            return False
    
    def cleanup(self):
        """Nettoyer les ressources Spark"""
        try:
            if self.spark:
                self.spark.stop()
                self.spark = None
                logger.info("✓ Session Spark nettoyée")
        except Exception as e:
            logger.error(f"✗ Erreur nettoyage: {e}")
    
    def run_with_failover(self):
        """Exécuter le job avec mécanisme de failover"""
        self.running = True
        
        logger.info("🚀 Démarrage du job Spark avec failover")
        
        while self.running and self.restart_count < self.max_restarts:
            try:
                # Créer la session Spark si nécessaire
                if not self.spark or not self.health_check():
                    self.cleanup()
                    if not self.create_spark_session():
                        raise Exception("Impossible de créer la session Spark")
                
                logger.info(f"🔄 Exécution #{self.restart_count + 1}")
                
                # Générer et traiter les données
                df = self.generate_sample_data()
                self.process_data(df)
                
                # Réinitialiser le compteur de redémarrage en cas de succès
                self.restart_count = 0
                
                # Attendre avant le prochain cycle
                logger.info("⏳ Attente avant le prochain cycle...")
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"💥 Erreur dans le job: {e}")
                
                self.cleanup()
                self.restart_count += 1
                
                if self.restart_count < self.max_restarts:
                    wait_time = min(2 ** self.restart_count, 60)  # Backoff avec limite
                    logger.info(f"🔄 Redémarrage dans {wait_time}s (tentative {self.restart_count}/{self.max_restarts})")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ Nombre maximum de redémarrages atteint")
                    self.running = False
                    break
        
        self.cleanup()
        logger.info("🏁 Job terminé")

def signal_handler(signum, frame):
    """Gestionnaire pour arrêt propre"""
    logger.info("📡 Signal d'arrêt reçu")
    global job
    if job:
        job.running = False
        job.cleanup()
    sys.exit(0)

def main():
    """Fonction principale"""
    global job
    
    # Créer les répertoires nécessaires
    os.makedirs("/data/input", exist_ok=True)
    os.makedirs("/data/output", exist_ok=True)
    os.makedirs("/data/checkpoints", exist_ok=True)
    os.makedirs("/logs", exist_ok=True)
    
    # Installer les gestionnaires de signaux
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Démarrer le job
    job = SparkFailoverJob()
    
    try:
        job.run_with_failover()
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"💥 Erreur critique: {e}")
    finally:
        job.cleanup()

if __name__ == "__main__":
    main()