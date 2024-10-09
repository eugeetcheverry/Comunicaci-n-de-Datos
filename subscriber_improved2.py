import paho.mqtt.client as mqtt
import zlib  # Asegúrate de tener esta importación al inicio del archivo

# Configuración
broker = "mqtt-dashboard.com"
topic = ""  # <<<<<<<<<<====== Completar con el nombre del grupo
output_file = 'output.txt'
received_fragments = {}
last_fragment = False
total_data_length = None

def calculate_checksum(data):
    return zlib.crc32(data.encode())

def on_subscribe(self, mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_message(client, userdata, msg):
    global last_fragment, total_data_length
    
    payload = msg.payload.decode('utf-8')
    fragment_info, fragment = payload.rsplit('|', 1)
    
    fragment_info_parts = fragment_info.split('|')
    if len(fragment_info_parts) < 5:
        print("Error: Payload incompleto, se ignorará el fragmento.")
        return  # Ignorar el fragmento si no tiene suficientes partes
    
    fragment_number, fragment_size, total_length, is_last = map(int, fragment_info_parts[:4])
    received_checksum = int(fragment_info_parts[4])
    
    calculated_checksum = calculate_checksum(fragment)
    if received_checksum == calculated_checksum:
        received_fragments[fragment_number] = (fragment_size, fragment)
        print(f"Fragmento {fragment_number} recibido correctamente con checksum válido.")
    else:
        print(f"Error: Checksum inválido para el fragmento {fragment_number}.")
    
    if total_data_length is None:
        total_data_length = total_length

    if is_last == 1:
        last_fragment = True

    # Reensamblar si es el último fragmento
    if last_fragment:
        if check_data_integrity():
            reassemble_file(output_file)
        else:
            print("Error: Fragmentos incompletos, el archivo está truncado.")
            reassemble_file(output_file, truncated=True)
        quit()

def check_data_integrity():
    received_length = sum(size for size, _ in received_fragments.values())
    return received_length == total_data_length

def reassemble_file(filename, truncated=False):
    with open(filename, 'w') as file:
        for fragment_number in sorted(received_fragments):
            fragment_size, fragment = received_fragments[fragment_number]
            file.write(fragment[:fragment_size])
    if truncated:
        print(f"Archivo guardado como {filename} con contenido truncado.")
    else:
        print(f"Archivo reconstruido completamente como {filename}.")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message
client.on_subscribe = on_subscribe

client.connect(broker, 1883, 60)
client.subscribe(topic, qos=2)

client.loop_forever()

