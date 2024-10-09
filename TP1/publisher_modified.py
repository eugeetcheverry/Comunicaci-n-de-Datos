
import paho.mqtt.client as mqtt
import random
import time

# Configuración
broker_address = "mqtt-dashboard.com"
topic = ""     # <<<<<<<<<<====== Completar con el nombre del grupo
min_size = 50  # Tamaño mínimo del fragmento
max_size = 70  # Tamaño máximo del fragmento
file_to_publish = 'input.txt'
skip_fragment = 2  # Simulamos la pérdida del fragmento 2

def on_connect(client, userdata, flags, rc):
    client_socket = client._socket().socket  
    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # Desactiva Nagle

def publish_file(client, filename, min_size, max_size):
    with open(filename, 'r') as file:
        content = file.read()

    total_length = len(content)
    index = 0
    fragment_number = 0
    while index < len(content):
        fragment_size = random.randint(min_size, max_size)
        fragment = content[index:index+fragment_size]
        
        is_last = 1 if index + fragment_size >= len(content) else 0
        
        if fragment_number != skip_fragment:  # Omitimos el fragmento 2 para simular la pérdida
            payload = f'{fragment_number}|{fragment_size}|{total_length}|{is_last}|{fragment}'
            client.publish(topic, payload, qos=2, retain=False)
            print(f"Fragmento publicado {fragment_number} (size: {fragment_size})")
        else:
            print(f"Fragmento {fragment_number} omitido para simular pérdida")
        
        index += fragment_size
        fragment_number += 1
        time.sleep(1)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.connect(broker_address, 1883, 60)
publish_file(client, file_to_publish, min_size, max_size)
client.disconnect()
