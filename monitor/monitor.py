#!/usr/bin/env python3
"""
Interface web de monitoring pour le POC Spark Failover
"""

import os
import json
import time
import requests
from datetime import datetime
from flask import Flask, render_template_string, jsonify
from threading import Thread
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class SparkMonitor:
    def __init__(self):
        self.spark_master_url = os.getenv('SPARK_MASTER_URL', 'http://spark-master:8080')
        self.spark_worker_url = os.getenv('SPARK_WORKER_URL', 'http://spark-worker:8081')
        self.status = {
            'master': {'status': 'unknown', 'last_check': None},
            'worker': {'status': 'unknown', 'last_check': None},
            'app': {'status': 'unknown', 'last_check': None, 'restart_count': 0}
        }
        self.logs = []
        
    def check_spark_master(self):
        """Vérifier l'état du Spark Master"""
        try:
            response = requests.get(f"{self.spark_master_url}/json", timeout=5)
            if response.status_code == 200:
                data = response.json()
                self.status['master'] = {
                    'status': 'healthy',
                    'last_check': datetime.now().isoformat(),
                    'workers': len(data.get('workers', [])),
                    'running_apps': len(data.get('activeapps', [])),
                    'completed_apps': len(data.get('completedapps', []))
                }
                return True
        except Exception as e:
            logger.error(f"Erreur check master: {e}")
            
        self.status['master'] = {
            'status': 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        return False
    
    def check_spark_worker(self):
        """Vérifier l'état du Spark Worker"""
        try:
            response = requests.get(f"{self.spark_worker_url}/json", timeout=5)
            if response.status_code == 200:
                data = response.json()
                self.status['worker'] = {
                    'status': 'healthy',
                    'last_check': datetime.now().isoformat(),
                    'cores': data.get('cores', 0),
                    'memory': data.get('memory', 0),
                    'cores_used': data.get('coresused', 0),
                    'memory_used': data.get('memoryused', 0)
                }
                return True
        except Exception as e:
            logger.error(f"Erreur check worker: {e}")
            
        self.status['worker'] = {
            'status': 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        return False
    
    def check_app_logs(self):
        """Vérifier les logs de l'application"""
        try:
            log_file = '/logs/spark_app.log'
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    
                # Garder seulement les 50 dernières lignes
                recent_lines = lines[-50:]
                self.logs = [line.strip() for line in recent_lines]
                
                # Analyser les logs pour déterminer le statut
                error_keywords = ['ERROR', 'CRITICAL', 'FAILED', '✗', '💥']
                success_keywords = ['SUCCESS', 'COMPLETED', '✓', '🚀']
                
                recent_text = ''.join(recent_lines[-10:])  # 10 dernières lignes
                
                if any(keyword in recent_text for keyword in error_keywords):
                    app_status = 'error'
                elif any(keyword in recent_text for keyword in success_keywords):
                    app_status = 'healthy'
                else:
                    app_status = 'running'
                
                # Compter les redémarrages
                restart_count = sum(1 for line in recent_lines if 'Redémarrage' in line or '🔄' in line)
                
                self.status['app'] = {
                    'status': app_status,
                    'last_check': datetime.now().isoformat(),
                    'restart_count': restart_count,
                    'log_lines': len(self.logs)
                }
                
        except Exception as e:
            logger.error(f"Erreur check logs: {e}")
            self.status['app'] = {
                'status': 'unknown',
                'last_check': datetime.now().isoformat()
            }
    
    def monitor_loop(self):
        """Boucle de monitoring"""
        while True:
            try:
                self.check_spark_master()
                self.check_spark_worker()
                self.check_app_logs()
                time.sleep(10)  # Vérifier toutes les 10 secondes
            except Exception as e:
                logger.error(f"Erreur monitoring: {e}")
                time.sleep(30)

# Instance globale du monitor
monitor = SparkMonitor()

@app.route('/')
def dashboard():
    """Page principale du dashboard"""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Spark Failover POC - Monitor</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
            .status-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .status-healthy { border-left: 5px solid #27ae60; }
            .status-unhealthy { border-left: 5px solid #e74c3c; }
            .status-unknown { border-left: 5px solid #f39c12; }
            .status-error { border-left: 5px solid #c0392b; }
            .status-running { border-left: 5px solid #3498db; }
            .logs { background: #2c3e50; color: #ecf0f1; padding: 20px; border-radius: 8px; font-family: monospace; font-size: 12px; height: 400px; overflow-y: auto; }
            .metric { margin: 10px 0; }
            .metric-label { font-weight: bold; color: #7f8c8d; }
            .metric-value { font-size: 1.2em; color: #2c3e50; }
            .refresh-btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            .refresh-btn:hover { background: #2980b9; }
            .timestamp { color: #7f8c8d; font-size: 0.9em; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Spark Failover POC - Dashboard</h1>
                <p>Monitoring en temps réel du cluster Spark et de l'application</p>
                <button class="refresh-btn" onclick="location.reload()">🔄 Actualiser</button>
            </div>
            
            <div class="status-grid">
                <div class="status-card status-{{ status.master.status }}">
                    <h3>🎯 Spark Master</h3>
                    <div class="metric">
                        <div class="metric-label">Status</div>
                        <div class="metric-value">{{ status.master.status.upper() }}</div>
                    </div>
                    {% if status.master.workers is defined %}
                    <div class="metric">
                        <div class="metric-label">Workers</div>
                        <div class="metric-value">{{ status.master.workers }}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Applications actives</div>
                        <div class="metric-value">{{ status.master.running_apps }}</div>
                    </div>
                    {% endif %}
                    <div class="timestamp">Dernière vérification: {{ status.master.last_check }}</div>
                </div>
                
                <div class="status-card status-{{ status.worker.status }}">
                    <h3>⚙️ Spark Worker</h3>
                    <div class="metric">
                        <div class="metric-label">Status</div>
                        <div class="metric-value">{{ status.worker.status.upper() }}</div>
                    </div>
                    {% if status.worker.cores is defined %}
                    <div class="metric">
                        <div class="metric-label">Cores (utilisés/total)</div>
                        <div class="metric-value">{{ status.worker.cores_used }}/{{ status.worker.cores }}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Mémoire (utilisée/totale)</div>
                        <div class="metric-value">{{ status.worker.memory_used }}MB/{{ status.worker.memory }}MB</div>
                    </div>
                    {% endif %}
                    <div class="timestamp">Dernière vérification: {{ status.worker.last_check }}</div>
                </div>
                
                <div class="status-card status-{{ status.app.status }}">
                    <h3>📱 Application</h3>
                    <div class="metric">
                        <div class="metric-label">Status</div>
                        <div class="metric-value">{{ status.app.status.upper() }}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Redémarrages</div>
                        <div class="metric-value">{{ status.app.restart_count }}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Lignes de log</div>
                        <div class="metric-value">{{ status.app.log_lines }}</div>
                    </div>
                    <div class="timestamp">Dernière vérification: {{ status.app.last_check }}</div>
                </div>
            </div>
            
            <div class="status-card">
                <h3>📋 Logs de l'application</h3>
                <div class="logs">
                    {% for log_line in logs %}
                    {{ log_line }}<br>
                    {% endfor %}
                </div>
            </div>
        </div>
        
        <script>
            // Auto-refresh toutes les 30 secondes
            setTimeout(() => location.reload(), 30000);
        </script>
    </body>
    </html>
    """
    
    return render_template_string(html_template, status=monitor.status, logs=monitor.logs)

@app.route('/api/status')
def api_status():
    """API pour récupérer le statut en JSON"""
    return jsonify({
        'status': monitor.status,
        'logs': monitor.logs[-10:],  # 10 dernières lignes
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health():
    """Endpoint de santé"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

def start_monitoring():
    """Démarrer le monitoring en arrière-plan"""
    monitor_thread = Thread(target=monitor.monitor_loop)
    monitor_thread.daemon = True
    monitor_thread.start()

if __name__ == '__main__':
    logger.info("🚀 Démarrage du monitor Spark")
    start_monitoring()
    app.run(host='0.0.0.0', port=3000, debug=False)