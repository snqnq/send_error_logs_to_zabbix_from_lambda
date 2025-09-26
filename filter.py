import json
import socket
import struct
import time
import zlib
import base64

class ZabbixSender:
    def __init__(self, server='127.0.0.1', port=10051):
        self.server = server
        self.port = port

    def send(self, packet):
        s = socket.socket()
        try:
            s.connect((self.server, self.port))
            s.send(packet)
            status = s.recv(1024).decode('utf-8')
            return status
            print(status)
        
        except Exception as e:
            print(e)
            return None
        finally:
            s.close()

class ZabbixPacket:
    def __init__(self):
        self.data = []

    def add(self, host, key, value, clock=None):
        if clock is None:
            clock = int(time.time())
        self.data.append({'host': host, 'key': key, 'value': value, 'clock': clock})

    def encode(self):
        packet = json.dumps({'request': 'sender data', 'data': self.data}).encode('utf-8')
        header = struct.pack('<4sBQ', b'ZBXD', 1, len(packet))
        print("Header:", header)
        print("Data Packet:", packet)
        return header + packet

def lambda_handler(event, context):
    try:
        
        print("Received Event:", event)
      
        data = event['awslogs']['data']
        decoded_data = base64.b64decode(data)
        uncompressed_data = zlib.decompress(decoded_data, 16+zlib.MAX_WBITS)
        
        log_events = json.loads(uncompressed_data)
        
        for log_event in log_events.get('logEvents', []):
            if 'message' in log_event:
                log_message = log_event['message']
                print("Log Content:", log_message)
        
                if 'ERROR' in log_message:
                    zabbix_packet = ZabbixPacket()
                    zabbix_packet.add(host='<Zabbix Host Name>', key='<Host Key>', value=log_message)
                    zabbix_sender = ZabbixSender(server='<Zabbix Server IP>', port=10051)
                    status = zabbix_sender.send(zabbix_packet.encode())
                    print("Error message sent to Zabbix")
        
                elif '<Other Use Case>' in log_message:
                    zabbix_packet = ZabbixPacket()
                    zabbix_packet.add(host='<Zabbix Host Name>', key='<Host key>', value=log_message)
                    zabbix_sender = ZabbixSender(server='<Zabbix server IP>', port=10051)
                    status = zabbix_sender.send(zabbix_packet.encode())
                    print("Daily close report message sent to Zabbix")
        
                else:
                    print("No error or report message to send to Zabbix")

        
        else:
            print("No error or report message to send to Zabbix")

 


    except Exception as e:
        print(f"An error occurred: {str(e)}")
    

