import pika
import json

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
QUEUE_LANCE_VALIDADO = 'lance_validado'
QUEUE_LEILAO_VENCEDOR = 'leilao_vencedor'
ROUTING_KEY_LEILAO_PART = 'leilao_'

def on_message(ch, method, properties, body):
    print(f"Recebido de {method.routing_key}: {body.decode()}")
    json_data = json.loads(body)
    routing_key = ROUTING_KEY_LEILAO_PART + str(json_data['lei_id'])

    # se vier de lances repassa o lance
    if method.routing_key == QUEUE_LANCE_VALIDADO:
        json_data['evento'] = 'lance'
    # se nao, eh leilao finalizado 
    elif method.routing_key == QUEUE_LEILAO_VENCEDOR:
        json_data['evento'] = 'fim'

    message = json.dumps(json_data)
    ch.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=message
    )
    print(f"Mensagem publicada em {routing_key}: {message}")

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    # declara queues e associa as routing keys e callbacks
    channel.queue_declare(queue=QUEUE_LANCE_VALIDADO)
    channel.queue_declare(queue=QUEUE_LEILAO_VENCEDOR)

    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_LANCE_VALIDADO, routing_key=QUEUE_LANCE_VALIDADO)
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_LEILAO_VENCEDOR, routing_key=QUEUE_LEILAO_VENCEDOR)

    channel.basic_consume(queue=QUEUE_LANCE_VALIDADO, on_message_callback=on_message, auto_ack=True)
    channel.basic_consume(queue=QUEUE_LEILAO_VENCEDOR, on_message_callback=on_message, auto_ack=True)

    print("Esperando mensagens de lance_validado e leilao_vencedor...")
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, EOFError):
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()
