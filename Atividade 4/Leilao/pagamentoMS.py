import pika
import json
import threading
import requests

# app.py
from flask import Flask, request, jsonify
app = Flask(__name__)

def runFlaskApp():
    app.run(port=5003, debug=True, use_reloader=False)

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
EXCHANGE_TYPE = 'direct'

ROUTING_KEY_LEILAO_VENCEDOR = 'leilao_vencedor'
ROUTING_KEY_LINK_PAGAMENTO = 'link_pagamento'
ROUTING_KEY_STATUS_PAGAMENTO = 'status_pagamento'

SISTEMA_PAGAMENTO_URL = 'http://127.0.0.1:5004/novo_pagamento'

@app.post("/status_pagamento")
def process_status_pagamento():
    json_data = request.get_json()

    if "lei_id" not in json_data or "cli_id" not in json_data or "status" not in json_data:
        return {"error": "Campos faltando na requisicao"}, 400

    # Publica o status do pagamento
    body = {
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'status': json_data['status']
    }
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY_STATUS_PAGAMENTO, body=json.dumps(body))
    print(f"Status do pagamento publicado: {body}")
    return {"message": "Status do pagamento recebido"}, 200

def process_leilao_vencedor(ch, method, properties, body):
    json_data = json.loads(body)

    if "cli_id" not in json_data or json_data['cli_id'] is None:
        print("Leilão finalizado sem lances válidos, nenhum pagamento necessário.")
        return

    body = {
        "lei_id": json_data['lei_id'],
        "cli_id": json_data['cli_id'],
        "valor": json_data['lance'],
        "moeda": "BRL"
    }
    response = requests.post(SISTEMA_PAGAMENTO_URL, json=body)

    if response.status_code == 200:
        body = json.dumps({
            'lei_id': json_data['lei_id'],
            'cli_id': json_data['cli_id'],
            'link_pagamento': response.json().get('link_pagamento', '')
        })
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY_LINK_PAGAMENTO,
            body=body
        )
    
    else:
        print(f"Erro ao solicitar pagamento: {response.status_code} - {response.text}")

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    # declara queues e associa as routing keys e callbacks
    channel.queue_declare(queue=ROUTING_KEY_LEILAO_VENCEDOR)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=ROUTING_KEY_LEILAO_VENCEDOR, routing_key=ROUTING_KEY_LEILAO_VENCEDOR)
    channel.basic_consume(queue=ROUTING_KEY_LEILAO_VENCEDOR, on_message_callback=process_leilao_vencedor, auto_ack=True)

    threading.Thread(target=runFlaskApp, daemon=True).start()

    print("Esperando mensagens de leilao ou lances...")
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, EOFError):
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()
