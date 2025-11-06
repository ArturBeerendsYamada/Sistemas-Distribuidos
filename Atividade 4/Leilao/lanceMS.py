import pika
import json
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
EXCHANGE_TYPE = 'direct'

ROUTING_KEY_LEILAO_INICIADO = 'leilao_iniciado'
ROUTING_KEY_LEILAO_FINALIZADO = 'leilao_finalizado'
ROUTING_KEY_LANCE_REALIZADO = 'lance_realizado'
ROUTING_KEY_LANCE_VALIDADO = 'lance_validado'
ROUTING_KEY_LEILAO_VENCEDOR = 'leilao_vencedor'

# chave eh o id do leilao
ultimos_lances_validos = {}

# Routing keys
QUEUE_BINDINGS = [
    (ROUTING_KEY_LANCE_REALIZADO, ROUTING_KEY_LANCE_REALIZADO),
    (ROUTING_KEY_LEILAO_INICIADO, ROUTING_KEY_LEILAO_INICIADO),
    (ROUTING_KEY_LEILAO_FINALIZADO, ROUTING_KEY_LEILAO_FINALIZADO)
]

def process_lance_realizado(ch, method, properties, body):
    json_data_signed = json.loads(body)
    print(f"Lance recebido: {json_data_signed['message']}")
    json_data = json.loads(json_data_signed['message'])
    signature = json_data_signed['signature']

    # verifica assinatura, se nao for valida ignora
    try:
        key = RSA.import_key(open(f"../Cliente/public_key_{json_data['cli_id']}.pem").read())
        h = SHA256.new(json_data_signed['message'].encode())
        pkcs1_15.new(key).verify(h, bytes.fromhex(signature))
    except (ValueError, TypeError):
        print("Assinatura inválida")
        return
    
    # se leilao nao existe, ou esta finalizado, ignora
    if json_data['lei_id'] not in ultimos_lances_validos or ultimos_lances_validos[json_data['lei_id']]['status'] == 'finalizado':
        print(f"Leilão não existe ou está finalizado: {json_data['lei_id']}")
        return
    
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
    
    # se nao, lance ignorado por ser menor
    else:
        print(f"Ignorando lance invalido")
        return

def process_leilao_iniciado(ch, method, properties, body):
    json_data = json.loads(body)
    print(f"Leilao iniciado: {json_data['lei_id']}")
    # inicializa o leilão
    ultimos_lances_validos[json_data['lei_id']] = {
        'lance': 0,
        'cli_id': None,
        'desc': json_data['desc'],
        'status': 'ativo'
    }
    print(ultimos_lances_validos[json_data['lei_id']])

def process_leilao_finalizado(ch, method, properties, body):
    json_data = json.loads(body)
    print(f"Leilao finalizado: {json_data['lei_id']}")
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

    channel.basic_consume(queue='lance_realizado', on_message_callback=process_lance_realizado, auto_ack=True)
    channel.basic_consume(queue='leilao_iniciado', on_message_callback=process_leilao_iniciado, auto_ack=True)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=process_leilao_finalizado, auto_ack=True)

    print("Esperando mensagens de leilao ou lances...")
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, EOFError):
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()
