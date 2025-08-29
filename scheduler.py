import schedule
import time
import requests
import threading
from datetime import datetime
import os

class FireScheduler:
    def __init__(self):
        self.api_base = os.getenv('RAILWAY_API_BASE', 'http://localhost:8000')
        self.running = False
        
    def process_fires_job(self):
        print(f"[{datetime.now()}] Iniciando procesamiento programado de incendios...")
        
        try:
            response = requests.post(f"{self.api_base}/process-fires", timeout=3600)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    stats = result.get('stats', {})
                    print(f"✅ Procesamiento exitoso:")
                    print(f"   - Polígonos: {stats.get('total_poligonos', 'N/A')}")
                    print(f"   - Eventos: {stats.get('eventos_unicos', 'N/A')}")
                    print(f"   - Eventos grandes: {stats.get('eventos_grandes', 'N/A')}")
                else:
                    print(f"❌ Error en procesamiento: {result.get('error')}")
            else:
                print(f"❌ Error HTTP: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print("⏰ Timeout - El procesamiento está tomando más tiempo del esperado")
        except Exception as e:
            print(f"❌ Error en job programado: {e}")
    
    def start_scheduler(self):
        print("🚀 Iniciando scheduler de incendios...")
        print("📅 Programado cada 12 horas: 06:00 y 18:00 UTC")
        
        schedule.every().day.at("06:00").do(self.process_fires_job)
        schedule.every().day.at("12:00").do(self.process_fires_job)
        schedule.every().day.at("18:00").do(self.process_fires_job)
        
        self.running = True
        
        while self.running:
            schedule.run_pending()
            time.sleep(60)
    
    def start_in_background(self):
        def run_scheduler():
            self.start_scheduler()
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        print("🔄 Scheduler ejecutándose en background")
        
    def stop(self):
        self.running = False
        print("🛑 Scheduler detenido")

scheduler_instance = FireScheduler()

if __name__ == "__main__":
    scheduler_instance.start_scheduler()
