import pika
import json
import threading

# app.py
from flask import Flask, request, jsonify
app = Flask(__name__)

def runFlaskApp():
    app.run(port=5002, debug=True, use_reloader=False)

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
EXCHANGE_TYPE = 'direct'

ROUTING_KEY_LEILAO_INICIADO = 'leilao_iniciado'
ROUTING_KEY_LEILAO_FINALIZADO = 'leilao_finalizado'
ROUTING_KEY_LANCE_INVALIDADO = 'lance_invalidado'
ROUTING_KEY_LANCE_VALIDADO = 'lance_validado'
ROUTING_KEY_LEILAO_VENCEDOR = 'leilao_vencedor'

# chave eh o id do leilao
ultimos_lances_validos = {}
rabbitmq_objects = {}

# Routing keys
QUEUE_BINDINGS = [
    (ROUTING_KEY_LANCE_INVALIDADO, ROUTING_KEY_LANCE_INVALIDADO),
    (ROUTING_KEY_LEILAO_INICIADO, ROUTING_KEY_LEILAO_INICIADO),
    (ROUTING_KEY_LEILAO_FINALIZADO, ROUTING_KEY_LEILAO_FINALIZADO)
]

@app.post("/lance")
def process_lance():
    json_data = request.get_json()
    
    # se leilao nao existe, ou esta finalizado, ignora
    if json_data['lei_id'] not in ultimos_lances_validos or ultimos_lances_validos[json_data['lei_id']]['status'] == 'finalizado':
        print(f"Leilão não existe ou está finalizado: {json_data['lei_id']}")
        return {"error": "Leilão não existe ou está finalizado"}, 400
    
    ch = rabbitmq_objects[json_data['lei_id']]['channel']
    print(json_data['lance'])
    # se foi lance valido
    if int(json_data['lance']) > int(ultimos_lances_validos[json_data['lei_id']]['lance']) and ultimos_lances_validos[json_data['lei_id']]['status'] == 'ativo':
        # atualiza ultimo lances validos do respectivo leilao
        ultimos_lances_validos[json_data['lei_id']]['lance'] = json_data['lance']
        ultimos_lances_validos[json_data['lei_id']]['cli_id'] = json_data['cli_id']
        # publica o lance validado
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY_LANCE_VALIDADO,
            body=json.dumps(json_data)
        )
        print(f"Lance validado: {json_data}")
        return {"message": "Lance validado"}, 200
    
    # se nao, lance ignorado por ser menor
    else:
        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY_LANCE_INVALIDADO,
            body=json.dumps(json_data)
        )
        print(f"Ignorando lance invalido")
        return {"error": "Lance inválido"}, 400

def process_leilao_iniciado(ch, method, properties, body):
    json_data = json.loads(body)
    print(f"Leilao iniciado: {json_data['lei_id']}")

    # cria canal para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    # inicializa o leilão
    ultimos_lances_validos[json_data['lei_id']] = {
        'cli_id': None,
        'lance': json_data['lance_inic'],
        'nome': json_data['nome'],
        'desc': json_data['desc'],
        'status': 'ativo'
    }
    rabbitmq_objects[json_data['lei_id']] = {
        'channel': channel,
        'connection': connection
    }
    print(ultimos_lances_validos[json_data['lei_id']])

def process_leilao_finalizado(ch, method, properties, body):
    json_data = json.loads(body)
    ultimos_lances_validos[json_data['lei_id']]['status'] = 'finalizado'

    json_data['cli_id'] = ultimos_lances_validos[json_data['lei_id']]['cli_id']
    json_data['lance'] = ultimos_lances_validos[json_data['lei_id']]['lance']
    json_data['desc'] = ultimos_lances_validos[json_data['lei_id']]['desc']
    message = json.dumps(json_data)

    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY_LEILAO_VENCEDOR,
        body=message
    )

    rabbitmq_objects[json_data['lei_id']]['channel'].close()
    rabbitmq_objects[json_data['lei_id']]['connection'].close()

    print(f"Leilão finalizado: {message}")

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

    # declara queues e associa as routing keys e callbacks
    for queue_name, routing_key in QUEUE_BINDINGS:
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)

    channel.basic_consume(queue='leilao_iniciado', on_message_callback=process_leilao_iniciado, auto_ack=True)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=process_leilao_finalizado, auto_ack=True)

    threading.Thread(target=runFlaskApp, daemon=True).start()

    print("Esperando mensagens de leilao ou lances...")
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, EOFError):
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()
