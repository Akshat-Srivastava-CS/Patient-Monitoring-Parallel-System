import multiprocessing as mp
import struct
import time
import random
import datetime

def sensor_fetch_stage(fetch_queue, patient_ids):
    """Simulates the Instruction Fetch cycle and Data Bus transfer."""
    print("[FETCH UNIT] Pipeline stage initialized. Commencing sensor polling...")
    for _ in range(10): 
        for pid in patient_ids:
            heart_rate = random.uniform(45.0, 140.0) 
            spo2 = random.uniform(85.0, 100.0)
            
            raw_hr = struct.pack('!f', heart_rate)
            raw_spo2 = struct.pack('!f', spo2)
            
            fetch_queue.put({'patient': pid, 'raw_hr': raw_hr, 'raw_spo2': raw_spo2, 'timestamp': datetime.datetime.now()})
        time.sleep(0.5) 
    
    for _ in patient_ids:
        fetch_queue.put(None)
    print("[FETCH UNIT] Polling complete. Stage terminated.")

def decode_alu_stage(fetch_queue, execute_queue):
    """Simulates the ALU performing logical comparisons on IEEE 754 data."""
    print("[ALU UNIT] Datapath online. Awaiting data...")
    while True:
        data = fetch_queue.get()
        if data is None: 
            execute_queue.put(None)
            break
            
        hr_val = struct.unpack('!f', data['raw_hr'])[0]
        spo2_val = struct.unpack('!f', data['raw_spo2'])[0]
        
        anomaly = False
        alert_reasons = []
        
        if hr_val > 120.0 or hr_val < 50.0:
            anomaly = True
            alert_reasons.append(f"Abnormal HR: {hr_val:.1f} bpm")
        if spo2_val < 92.0:
            anomaly = True
            alert_reasons.append(f"Hypoxia Detected - SpO2: {spo2_val:.1f}%")
            
        execute_queue.put({'patient': data['patient'], 'hr': hr_val, 'spo2': spo2_val, 'anomaly': anomaly, 'reasons': alert_reasons, 'timestamp': data['timestamp']})
    print("[ALU UNIT] ALU processing complete. Stage terminated.")

def execute_alert_stage(execute_queue):
    """Simulates the Control Unit triggering hardware alerts."""
    print("[EXECUTE UNIT] Alert dispatcher online...")
    while True:
        result = execute_queue.get()
        if result is None:
            break
            
        if result['anomaly']:
            reasons_str = " | ".join(result['reasons'])
            print(f"!!! CRITICAL ALERT !!! Patient: {result['patient']} | Time: {result['timestamp'].strftime('%H:%M:%S')} | {reasons_str}")
        else:
            print(f"[LOG] {result['patient']} - Vitals Stable. HR: {result['hr']:.1f}, SpO2: {result['spo2']:.1f}%")
    print("[EXECUTE UNIT] Alert dispatcher terminated.")

if __name__ == '__main__':
    patients = ['PT-001', 'PT-002', 'PT-003']
    fetch_to_alu_queue = mp.Queue()
    alu_to_exec_queue = mp.Queue()
    
    fetch_process = mp.Process(target=sensor_fetch_stage, args=(fetch_to_alu_queue, patients))
    alu_process = mp.Process(target=decode_alu_stage, args=(fetch_to_alu_queue, alu_to_exec_queue))
    exec_process = mp.Process(target=execute_alert_stage, args=(alu_to_exec_queue,))
    
    fetch_process.start()
    alu_process.start()
    exec_process.start()
    
    fetch_process.join()
    alu_process.join()
    exec_process.join()
    print("\n[SYSTEM] Pipeline successfully halted.")