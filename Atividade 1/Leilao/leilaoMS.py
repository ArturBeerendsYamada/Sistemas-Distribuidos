import pika
import json
import schedule
import time


RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
ROUTING_KEY_LEILAO_INICIADO = 'leilao_iniciado'
ROUTING_KEY_LEILAO_FINALIZADO = 'leilao_finalizado'

# agenda os leiloes para 5 segundos depois do tempo de execucao

# leilao 0 dura 30 segundos, tempo para cada cliente fazer lances
LEI_0_INIC = int(time.time()) + 5
LEI_0_FIM = LEI_0_INIC + 30

# leilao 1 dura 5 segundos e comeca 5 segundos depois do leilao 0
# para mostrar que leiloes podem acontecer em paralelo
# e se o usuario nao fazer um lance nele, nao recebe mensagens
LEI_1_INIC = LEI_0_INIC + 5
LEI_1_FIM = LEI_1_INIC + 5

# leilao 2 dura 10 segundos e comeca 10 segundos depois do fim do leilao 0
# para mostrar que lances realizados enquanto um leilao nao esta ativo sao descartados 
LEI_2_INIC = LEI_0_FIM + 10
LEI_2_FIM = LEI_2_INIC + 10

INICIAR = 1
FINALIZAR = 0

def leilao_agendado(evento, lei_id, channel):
    if evento == INICIAR:
        routing_key = ROUTING_KEY_LEILAO_INICIADO
    elif evento == FINALIZAR:
        routing_key = ROUTING_KEY_LEILAO_FINALIZADO
        
    message = json.dumps({
        'lei_id': leiloes[lei_id]['lei_id'],
        'desc': leiloes[lei_id]['desc'],
        'data_inic': leiloes[lei_id]['data_inic'],
        'data_fim': leiloes[lei_id]['data_fim'],
    })
    channel.basic_publish(
       exchange=EXCHANGE_NAME,
       routing_key=routing_key,
       body=message
    )
    if evento == INICIAR:
        print(f"Leilão iniciado: {lei_id}")
    elif evento == FINALIZAR:
        print(f"Leilão finalizado: {lei_id}")
    
    return schedule.CancelJob

leiloes = [
    {
        'lei_id': 0,
        'desc': "pintura",
        'data_inic': LEI_0_INIC,
        'data_fim': LEI_0_FIM,
    },
    {
        'lei_id': 1,
        'desc': "vaso antigo",
        'data_inic': LEI_1_INIC,
        'data_fim': LEI_1_FIM,
    },
    {
        'lei_id': 2,
        'desc': "queijo",
        'data_inic': LEI_2_INIC,
        'data_fim': LEI_2_FIM,
    }
]

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')
    
    for entry in leiloes:
        atual = time.time()
        espera_inic = entry['data_inic'] - atual
        espera_fim = entry['data_fim'] - atual
        schedule.every(espera_inic).seconds.do(leilao_agendado, evento=INICIAR, lei_id=entry['lei_id'], channel=channel)
        print(f"inicio de {entry['lei_id']} agendado para daqui {espera_inic}s")
        schedule.every(espera_fim).seconds.do(leilao_agendado, evento=FINALIZAR, lei_id=entry['lei_id'], channel=channel)
        print(f"fim de {entry['lei_id']} agendado para daqui {espera_fim}s")

    print("[iniciar] [ID] [DESCRICAO] [DATA_INICIO] [DATA_FIM]")

    while True:
        try:
            schedule.run_pending()   # executa jobs agendados
            time.sleep(1)            # evita busy loop
            
            # partes
            # if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            #     entrada = input()
            #     partes = entrada.split()
            # if partes[0] == 'iniciar':
            #     message = json.dumps({
            #         'lei_id': partes[1],
            #         'desc': partes[2],
            #         'data_inic': partes[3],
            #         'data_fim': partes[4]
            #     })
            #     #channel.basic_publish(
            #     #    exchange=EXCHANGE_NAME,
            #     #    routing_key=ROUTING_KEY_LEILAO_INICIADO,
            #     #    body=message
            #     #)
            #     print(f"Leilão iniciado: {partes[1]}")
            # else:
            #     message = json.dumps({
            #         'lei_id': partes[1],
            #     })
            #     #channel.basic_publish(
            #     #   exchange=EXCHANGE_NAME,
            #     #    routing_key=ROUTING_KEY_LEILAO_FINALIZADO,
            #     #   body=message
            #     #)
            #     print(f"Leilão finalizado: {partes[1]}")
        except (KeyboardInterrupt, EOFError):
            print("\nSaindo. Agradeçemos pela preferência.")
            break

    #connection.close()

if __name__ == '__main__':
    main()
