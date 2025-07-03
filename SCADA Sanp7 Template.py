import snap7
from snap7 import util
import pyodbc
import time
from datetime import datetime

# PLC Configuration
plc_1200 = snap7.client.Client()

def plc_connect():
    try:
        if not plc_1200.get_connected():
            plc_1200.connect('192.168.XX.XX', 0, 1)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - PLC connected successfully")
            return True
        return True
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - PLC connection failed: {str(e)}")
        return False

# SQL Server Configuration
def get_db_connection():
    server_ip = '192.168.XX.XX'
    port = 'XXXX'
    database = 'XXXX'
    username = 'XXX' 
    password = 'XXX'
    
    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server_ip},{port};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

def save_sensor_data_to_sql(SQL_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO PID (
            actual,PID_output,is_active
            ) VALUES (?, ?, ?)
        """, 
            SQL_data['actual'], SQL_data['PID_output'], 
            SQL_data['is_active']
        )
        
        conn.commit()
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Data saved successfully")
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Database error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def read_plc():
    try:
        # 
        realdata = plc_1200.db_read(XX,XX,XX )
        booldata = plc_1200.db_read(XX, XX, XX)
        
        SQL_data = {
            'actual': util.get_real(realdata, X),
            'PID_output':util.get_real(realdata, X),
            'is_active':util.get_bool(booldata,X,X)
            
        }
        
        save_sensor_data_to_sql(SQL_data)
        
        log_msg = " | ".join([f"{k}={v:.2f}" for k, v in SQL_data.items()])
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {log_msg}")
        
        return SQL_data
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Read failed: {str(e)}")
        return None

def main_loop():
    while True:
        try:
            if plc_connect():
                read_plc()
            time.sleep(1)
        except Exception as e:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    print("Starting PLC monitoring...")
    main_loop()