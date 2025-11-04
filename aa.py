import psutil
import time
import os
from collections import deque

def find_fastapi_process():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if not cmdline:
                continue
            
            cmdline_str = ' '.join(cmdline)
            
            if 'python' in cmdline_str.lower() and 'main.py' in cmdline_str:
                try:
                    if proc.cwd() == current_dir:
                        return proc
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    if current_dir.lower() in cmdline_str.lower():
                        return proc
                        
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None

# ====== CONFIGURACIÓN ======
INTERVALO_SEGUNDOS = 0.5  # Cambia esto: 0.5, 1, 2, 5, etc.
# ===========================

print("🔍 Buscando proceso FastAPI...")
process = find_fastapi_process()

if not process:
    print("❌ No se encontró el proceso de FastAPI.")
    print(f"📁 Carpeta actual: {os.path.dirname(os.path.abspath(__file__))}")
    exit(1)

print(f"✅ Proceso encontrado (PID: {process.pid})")
print(f"📂 Carpeta: {process.cwd()}")
print(f"⏱️  Actualizando cada {INTERVALO_SEGUNDOS}s")
print("=" * 80)
print(f"{'CPU':<8} {'RAM (MB)':<12} {'Threads':<10} {'Conexiones':<12} {'Picos':<30}")
print("=" * 80)

# Mantener historial de picos
max_cpu = 0
max_ram = 0
max_connections = 0
picos_cpu = deque(maxlen=3)  # Solo los 3 más altos
picos_ram = deque(maxlen=3)
picos_conn = deque(maxlen=3)

while True:
    try:
        cpu = process.cpu_percent(interval=INTERVALO_SEGUNDOS)
        ram_mb = process.memory_info().rss / (1024 * 1024)
        num_threads = process.num_threads()
        
        try:
            connections = len(process.connections())
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            connections = 0
        
        # Actualizar máximos
        if cpu > max_cpu:
            max_cpu = cpu
            picos_cpu.appendleft(('CPU', cpu, time.strftime('%H:%M:%S')))
        
        if ram_mb > max_ram:
            max_ram = ram_mb
            picos_ram.appendleft(('RAM', ram_mb, time.strftime('%H:%M:%S')))
        
        if connections > max_connections:
            max_connections = connections
            picos_conn.appendleft(('CONN', connections, time.strftime('%H:%M:%S')))
        
        # Mostrar valores actuales
        picos_str = f"🔥CPU:{max_cpu:.1f}% 💾RAM:{max_ram:.0f}MB 🔌CONN:{max_connections}"
        
        print(f"{cpu:5.1f}%   {ram_mb:8.1f}      {num_threads:3d}        {connections:3d}          {picos_str}", end='\r')
        
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        print(f"\n\n❌ Proceso terminado: {e}")
        break
    except KeyboardInterrupt:
        print("\n\n")
        print("=" * 80)
        print("📊 RESUMEN DE PICOS")
        print("=" * 80)
        
        print("\n🔥 Top 3 Picos de CPU:")
        for i, (label, val, timestamp) in enumerate(picos_cpu, 1):
            print(f"   {i}. {val:5.1f}% a las {timestamp}")
        
        print("\n💾 Top 3 Picos de RAM:")
        for i, (label, val, timestamp) in enumerate(picos_ram, 1):
            print(f"   {i}. {val:6.1f} MB a las {timestamp}")
        
        print("\n🔌 Top 3 Picos de Conexiones:")
        for i, (label, val, timestamp) in enumerate(picos_conn, 1):
            print(f"   {i}. {val:3.0f} conexiones a las {timestamp}")
        
        print("\n⏹️  Monitoreo detenido.")
        break