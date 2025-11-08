import requests

# app.py
from flask import Flask, request, jsonify
app = Flask(__name__)

def runFlaskApp():
    app.run(port=5004, debug=True, use_reloader=False)

PAGAMENTO_MS_URL = 'http://127.0.0.1:5003/status_pagamento'

pagamentos = {}

@app.post("/novo_pagamento")
def process_novo_pagamento():
    json_data = request.get_json()

    if "lei_id" not in json_data or "cli_id" not in json_data or "valor" not in json_data or "moeda" not in json_data:
        return {"error": "Campos faltando na requisicao"}, 400

    pagamentos[(json_data['lei_id'], json_data['cli_id'])] = {
        'valor': json_data['valor'],
        'moeda': json_data['moeda'],
        'status': 'pendente'
    }
    print(pagamentos[(json_data['lei_id'], json_data['cli_id'])])

    link_pagamento = f"http://127.0.0.1:5004/pagar?lei_id={json_data['lei_id']}&cli_id={json_data['cli_id']}"
    body = {
        "link_pagamento": link_pagamento
    }

    return jsonify(body), 200

@app.post("/pagar")
def process_pagar():
    json_data = request.get_json()

    try:
        lei_id = int(request.args.get('lei_id'))
        cli_id = int(request.args.get('cli_id'))
    except (ValueError, TypeError):
        return {"error": "Parâmetros inválidos"}, 400

    status = 'aprovado'
    print(f"Processando pagamento para lei_id: {lei_id}, cli_id: {cli_id}")
    print(pagamentos)

    if "valor" not in json_data or "moeda" not in json_data:
        return {"error": "Campos faltando na requisicao"}, 400

    if (lei_id, cli_id) not in pagamentos:
        return {"error": "Pagamento não encontrado"}, 404
    
    if json_data["moeda"] != pagamentos[(lei_id, cli_id)]['moeda']:
        status = 'recusado por moeda incompativel'

    if pagamentos[(lei_id, cli_id)]['valor'] != int(json_data["valor"]):
        status = 'recusado por valor incompativel'

    body = {
        "lei_id": lei_id,
        "cli_id": cli_id,
        "status": status
    }
    print(body)
    response = requests.post(PAGAMENTO_MS_URL, json=body)
    pagamentos[(lei_id, cli_id)]['status'] = status

    if status == 'aprovado' and response.status_code == 200:
        return {"status": "Pagamento aprovado com sucesso"}, 200
    else:
        if response.status_code == 200:
            return {"status": f"Pagamento {status}"}, 200
        else:
            return {"error": "Erro ao notificar status do pagamento"}, 500


def main():
    try:
        runFlaskApp()
    except (KeyboardInterrupt, EOFError):
        pass

if __name__ == '__main__':
    main()
