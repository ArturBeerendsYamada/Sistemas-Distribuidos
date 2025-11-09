# SSE logic adapted from https://github.com/MaxHalford/flask-sse-no-deps/blob/master/app.py
# explained on https://maxhalford.github.io/blog/flask-sse-no-deps/

import pika
import json
import queue
import threading
import requests

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_world():
    return 'Hello, World!'

def runFlaskApp():
    app.run(port=5000, debug=True, use_reloader=False)

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
EXCHANGE_TYPE = 'direct'

CRIAR_LEILAO_URL = 'http://127.0.0.1:5001/criar_leilao'
CONSULTAR_LEILOES_URL = 'http://127.0.0.1:5001/consultar_leiloes'
LANCE_URL = 'http://127.0.0.1:5002/lance'

ROUTING_KEY_LANCE_INVALIDADO = 'lance_invalidado'
ROUTING_KEY_LANCE_VALIDADO = 'lance_validado'
ROUTING_KEY_LEILAO_VENCEDOR = 'leilao_vencedor'
ROUTING_KEY_LINK_PAGAMENTO = 'link_pagamento'
ROUTING_KEY_STATUS_PAGAMENTO = 'status_pagamento'

QUEUE_BINDINGS = [
    (ROUTING_KEY_LANCE_INVALIDADO, ROUTING_KEY_LANCE_INVALIDADO),
    (ROUTING_KEY_LANCE_VALIDADO, ROUTING_KEY_LANCE_VALIDADO),
    (ROUTING_KEY_LEILAO_VENCEDOR, ROUTING_KEY_LEILAO_VENCEDOR),
    (ROUTING_KEY_LINK_PAGAMENTO, ROUTING_KEY_LINK_PAGAMENTO),
    (ROUTING_KEY_STATUS_PAGAMENTO, ROUTING_KEY_STATUS_PAGAMENTO)
]

announcers_per_client = {}
clients_interests = {}

class MessageAnnouncer:
    def __init__(self):
        self.listeners = []

    def listen(self):
        self.listeners.append(queue.Queue(maxsize=5))
        return self.listeners[-1]

    def announce(self, msg):
        # We go in reverse order because we might have to delete an element, which will shift the
        # indices backward
        for i in reversed(range(len(self.listeners))):
            try:
                self.listeners[i].put_nowait(msg)
            except queue.Full:
                del self.listeners[i]


def format_sse(data: str, event=None) -> str:
    """Formats a string and an event name in order to follow the event stream convention.

    >>> format_sse(data=json.dumps({'abc': 123}), event='Jackson 5')
    'event: Jackson 5\\ndata: {"abc": 123}\\n\\n'

    """
    msg = f'data: {data}\n\n'
    if event is not None:
        msg = f'event: {event}\n{msg}'
    return msg

@app.get('/ping')
def ping():
    msg = format_sse(data='pong')
    for announcer in announcers_per_client.values():
        announcer.announce(msg=msg)
    return {}, 200

@app.get('/listen')
def listen():
    client_id = int(request.args.get('cli_id'))
    if client_id not in announcers_per_client:
        announcers_per_client[client_id] = MessageAnnouncer()
    def stream():
        messages = announcers_per_client[client_id].listen()  # returns a queue.Queue
        while True:
            msg = messages.get()  # blocks until a new message arrives
            yield msg

    print(f"Cliente {client_id} conectado para notificacoes SSE {announcers_per_client}")
    return Response(stream(), mimetype='text/event-stream')

@app.delete('/unlisten')
def unlisten():
    client_id = request.args.get('cli_id')
    if client_id in announcers_per_client:
        del announcers_per_client[client_id]
    return {"message": "Interrompendo notificacoes"}, 200

@app.post('/criar_leilao')
def criar_leilao():
    json_data = request.get_json()
    response = requests.post(CRIAR_LEILAO_URL, json=json_data)
    return jsonify(response.json()), response.status_code

@app.get('/consultar_leiloes')
def consultar_leiloes():
    response = requests.get(CONSULTAR_LEILOES_URL)
    return jsonify(response.json()), response.status_code

@app.post('/lance')
def lance():
    json_data = request.get_json()

    # registra interesse do cliente no leilao
    client_id = json_data.get('cli_id')
    leilao_id = json_data.get('lei_id')
    if client_id not in clients_interests:
        clients_interests[client_id] = set()
    clients_interests[client_id].add(leilao_id)

    # envia o lance para o microservico de lances
    response = requests.post(LANCE_URL, json=json_data)
    return jsonify(response.json()), response.status_code

@app.post('/registrar_interesse')
def registrar_interesse():
    json_data = request.get_json()
    client_id = json_data.get('cli_id')
    leilao_id = json_data.get('lei_id')

    if client_id not in clients_interests:
        clients_interests[client_id] = set()

    clients_interests[client_id].add(leilao_id)
    return {"message": "Interesse registrado com sucesso"}, 201

@app.post('/cancelar_interesse')
def cancelar_interesse():
    json_data = request.get_json()
    client_id = json_data.get('cli_id')
    leilao_id = json_data.get('lei_id')

    if client_id not in clients_interests:
        return {"message": "Interesse não encontrado"}, 404

    clients_interests[client_id].remove(leilao_id)
    return {"message": "Interesse cancelado com sucesso"}, 200


def process_lance_invalidado(ch, method, properties, body):
    json_data = json.loads(body)
    body = json.dumps({
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'lance': json_data['lance'],
    })
    announcers_per_client[json_data['cli_id']].announce(msg=format_sse(data=body, event='lance_invalidado'))
    print(f"Lance invalidado anunciado via SSE: {body}")

def process_lance_validado(ch, method, properties, body):
    json_data = json.loads(body)
    body = json.dumps({
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'lance': json_data['lance'],
    })

    # anuncia para todos os clientes interessados
    for client_id, interests in clients_interests.items():
        if json_data['lei_id'] in interests:
            print(str(clients_interests) + " --- " + str(client_id) + " --- " + str(announcers_per_client))
            if client_id in announcers_per_client:
                announcers_per_client[client_id].announce(msg=format_sse(data=body, event='lance_validado'))
    print(f"Lance validado anunciado via SSE: {body}")

def process_leilao_vencedor(ch, method, properties, body):
    json_data = json.loads(body)
    body = json.dumps({
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'lance': json_data['lance'],
        'desc': json_data['desc'],
        'nome': json_data['nome']
    })

    # anuncia para todos os clientes interessados
    for client_id, interests in clients_interests.items():
        if json_data['lei_id'] in interests:
            if client_id in announcers_per_client:
                announcers_per_client[client_id].announce(msg=format_sse(data=body, event='leilao_vencedor'))

    # retira interesse dos clientes pelo leilao finalizado
    for client_id in clients_interests:
        if json_data['lei_id'] in clients_interests[client_id]:
            clients_interests[client_id].remove(json_data['lei_id'])

    print(f"Leilao vencedor anunciado via SSE: {body}")

def process_link_pagamento(ch, method, properties, body):
    json_data = json.loads(body)
    print(json_data)
    body = json.dumps({
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'link_pagamento': json_data['link_pagamento']
    })

    announcers_per_client[json_data['cli_id']].announce(msg=format_sse(data=body, event='link_pagamento'))
    print(f"Link de pagamento anunciado via SSE: {body}")

def process_status_pagamento(ch, method, properties, body):
    json_data = json.loads(body)
    body = json.dumps({
        'lei_id': json_data['lei_id'],
        'cli_id': json_data['cli_id'],
        'status': json_data['status']
    })
    announcers_per_client[json_data['cli_id']].announce(msg=format_sse(data=body, event='status_pagamento'))
    print(f"Status do pagamento anunciado via SSE: {body}")

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    # declara queues e associa as routing keys e callbacks
    for queue_name, routing_key in QUEUE_BINDINGS:
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)

    channel.basic_consume(queue='lance_invalidado', on_message_callback=process_lance_invalidado, auto_ack=True)
    channel.basic_consume(queue='lance_validado', on_message_callback=process_lance_validado, auto_ack=True)
    channel.basic_consume(queue='leilao_vencedor', on_message_callback=process_leilao_vencedor, auto_ack=True)
    channel.basic_consume(queue='link_pagamento', on_message_callback=process_link_pagamento, auto_ack=True)
    channel.basic_consume(queue='status_pagamento', on_message_callback=process_status_pagamento, auto_ack=True)

    threading.Thread(target=runFlaskApp, daemon=True).start()
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, EOFError):
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()