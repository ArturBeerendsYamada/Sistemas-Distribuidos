import pika
import json
import schedule
import time
import threading

# app.py
from flask import Flask, request, jsonify
app = Flask(__name__)

def runFlaskApp():
    app.run(port=5001, debug=True, use_reloader=False)

def _find_next_id():
    return max(leilao["lei_id"] for leilao in leiloes) + 1

@app.get("/consultar_leiloes")
def consultar_leiloes():
    body = []
    for leilao in leiloes:
        atual = time.time()
        if atual < leilao['data_inic']:
            status = 'agendado'
        elif leilao['data_inic'] <= atual < leilao['data_fim']:
            status = 'em andamento'
        else:
            status = 'finalizado'
        body.append({
            'lei_id': leilao['lei_id'],
            'nome': leilao['nome'],
            'desc': leilao['desc'],
            'lance_inic': leilao['lance_inic'],
            'data_inic': leilao['data_inic'],
            'data_fim': leilao['data_fim'],
            'status': status,
        })
    return jsonify(body), 200

@app.post("/criar_leilao")
def criar_leilao():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    if request.is_json:
        novo_leilao = request.get_json()
        atual = time.time()

        # valida campos obrigatorios
        if "desc" not in novo_leilao or "data_inic" not in novo_leilao or "data_fim" not in novo_leilao or "nome" not in novo_leilao or "lance_inic" not in novo_leilao:
            return {"error": "Campos faltando na requisicao"}, 400
        if novo_leilao["data_inic"] >= novo_leilao["data_fim"]:
            return {"error": "data_inic deve ser anterior a data_fim"}, 400
        if novo_leilao["data_inic"] <= time.time():
            return {"error": "data_inic deve ser posterior ao tempo atual"}, 400

        # atribui id unico
        novo_leilao["lei_id"] = _find_next_id()

        # adiciona leilao a lista e agenda inicio/fim
        leiloes.append(novo_leilao)
        espera_inic = novo_leilao['data_inic'] - atual
        espera_fim = novo_leilao['data_fim'] - atual
        schedule.every(espera_inic).seconds.do(leilao_agendado, evento=INICIAR, lei_id=novo_leilao['lei_id'], channel=channel, connection=connection)
        print(f"inicio de {novo_leilao['lei_id']} agendado para daqui {espera_inic}s")
        schedule.every(espera_fim).seconds.do(leilao_agendado, evento=FINALIZAR, lei_id=novo_leilao['lei_id'], channel=channel, connection=connection)
        print(f"fim de {novo_leilao['lei_id']} agendado para daqui {espera_fim}s")

        return novo_leilao, 201
    return {"error": "Request must be JSON"}, 415

RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'leilao_exchange'
ROUTING_KEY_LEILAO_INICIADO = 'leilao_iniciado'
ROUTING_KEY_LEILAO_FINALIZADO = 'leilao_finalizado'


# leilao 0 dura 30 segundos e comeca 5 segundos depois do inicio do script
LEI_0_INIC = int(time.time()) + 5
LEI_0_FIM = LEI_0_INIC + 5

INICIAR = 1
FINALIZAR = 0

def leilao_agendado(evento, lei_id, channel, connection):
    if evento == INICIAR:
        routing_key = ROUTING_KEY_LEILAO_INICIADO
    elif evento == FINALIZAR:
        routing_key = ROUTING_KEY_LEILAO_FINALIZADO
        
    message = json.dumps({
        'lei_id': leiloes[lei_id]['lei_id'],
        'nome': leiloes[lei_id]['nome'],
        'desc': leiloes[lei_id]['desc'],
        'lance_inic': leiloes[lei_id]['lance_inic'],
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
        channel.close()
        connection.close()
        print(f"Leilão finalizado: {lei_id}")
    
    return schedule.CancelJob

leiloes = [
    {
        'lei_id': 0,
        'nome': "pintura",
        'desc': "eh uma pintura",
        'lance_inic': 100.0,
        'data_inic': LEI_0_INIC,
        'data_fim': LEI_0_FIM,
    }
]

def main():
    # abre conexao e exchange para publicar mensagens
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    threading.Thread(target=runFlaskApp, daemon=True).start()

    for entry in leiloes:
        atual = time.time()
        espera_inic = entry['data_inic'] - atual
        espera_fim = entry['data_fim'] - atual
        schedule.every(espera_inic).seconds.do(leilao_agendado, evento=INICIAR, lei_id=entry['lei_id'], channel=channel, connection=connection)
        print(f"inicio de {entry['lei_id']} agendado para daqui {espera_inic}s")
        schedule.every(espera_fim).seconds.do(leilao_agendado, evento=FINALIZAR, lei_id=entry['lei_id'], channel=channel, connection=connection)
        print(f"fim de {entry['lei_id']} agendado para daqui {espera_fim}s")

    print("[iniciar] [ID] [DESCRICAO] [DATA_INICIO] [DATA_FIM]")

    while True:
        try:
            schedule.run_pending()   # executa jobs agendados
            time.sleep(1)            # evita busy loop
            
        except (KeyboardInterrupt, EOFError):
            print("\nSaindo. Agradeçemos pela preferência.")
            break

    #connection.close()

if __name__ == '__main__':
    main()
