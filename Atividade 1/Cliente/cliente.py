import pika
import threading
import sys
import json
import time
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
ROUTING_KEY_LEILAO_INICIADO = 'leilao_iniciado'
ROUTING_KEY_LANCE_REALIZADO = 'lance_realizado'
ROUTING_KEY_LEILAO_PART = 'leilao_'

client_id = 0
queue_name = None

def consume():
    # abre conexao
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    # declara queue e associa a routing key 'leilao_iniciado'
    result = channel.queue_declare(queue='')
    global queue_name
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=ROUTING_KEY_LEILAO_INICIADO)

    # chamada quando receber uma mensagem
    def callback(ch, method, properties, body):
        json_data = json.loads(body)
        if method.routing_key == ROUTING_KEY_LEILAO_INICIADO:
            # se receber roteada de leilao iniciado, notifica o usuario
            print("========== Novo leilão =========")
            print(f"Leilão ID: {json_data['lei_id']}")
            print(f"Descrição: {json_data['desc']}")
            print(f"Início: {time.ctime(json_data['data_inic'])}")
            print(f"Fim: {time.ctime(json_data['data_fim'])}")
            print("================================")
        else:
            # se receber roteada de algum leilao subscrito, notifica o usuario
            print("====== Atualização leilão ======")
            print(f"Leilão ID: {json_data['lei_id']}")
            if json_data['evento'] == 'fim':
                print(f"Descrição: {json_data['desc']}")
                print(f"    Usuário ID: {json_data['cli_id']}")
                print(f"    Lance final: {json_data['lance']}")
                print("FINALIZADO!")
            elif json_data['evento'] == 'lance':
                print(f"    Novo lance: {json_data['lance']}")
            print("================================")

    # inicia escuta
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(f"Esperando leilões...")
    print(f"Se deseja participar de algum, digite o ID do leilão seguido do valor lance que deseja fazer (apenas números)")
    channel.start_consuming()

def publish(private_key):
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    while True:
        try:
            entrada = input()
            # espera dois inteiros separados por espaco
            partes = entrada.split()
            if len(partes) == 2 and partes[0].isdigit() and partes[1].isdigit():
                id_leilao = partes[0]
                lance = partes[1]

                # subscreve o usuario para receber atualizacoes do leilao indicado
                routing_key = ROUTING_KEY_LEILAO_PART + id_leilao
                channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)

                # monta a mensagem
                message = json.dumps({
                    "lei_id": int(id_leilao),
                    "cli_id": client_id,
                    "lance": int(lance)
                })

                # assina a mensagem
                key = RSA.import_key(private_key)
                h = SHA256.new(message.encode())
                signature = pkcs1_15.new(key).sign(h)
                message_signed = json.dumps({
                    "message": message,
                    "signature": signature.hex()
                })   
                    
                # publica a mensagem com a rota apropriada
                channel.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=ROUTING_KEY_LANCE_REALIZADO,
                    body=message_signed
                )
                print(f"Lance de '{lance}' enviado para leilão com ID '{id_leilao}'")
            else:
                print("Entrada invalida, por favor insira o ID do leilão seguido do valor do lance.")
        except (KeyboardInterrupt, EOFError):
            print("\nSaindo. Agradeçemos pela preferência.")
            break

    connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        print("Inicializando cliente com ID default (0)")
    else:
        client_id = int(sys.argv[1])

    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()
    with open("public_key_{}.pem".format(client_id), "wb") as f:
        f.write(public_key)
    

    threading.Thread(target=consume, daemon=True).start()
    publish(private_key)