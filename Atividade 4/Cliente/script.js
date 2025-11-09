const eventSource = new EventSource('http://127.0.0.1:5000/listen?cli_id=1');

eventSource.onmessage = function(event) {
    try {
        const data = JSON.parse(event.data);
        console.log('Received data:', data);
    } catch (error) {
        console.log('Received data:', event.data);
    }
};

eventSource.addEventListener('lance_validado', function(event) {
    const data = JSON.parse(event.data);
    console.log('Lance Validado:', data);
});

eventSource.addEventListener('lance_invalidado', function(event) {
    const data = JSON.parse(event.data);
    console.log('Lance Invalidado:', data);
});

eventSource.addEventListener('leilao_vencedor', function(event) {
    const data = JSON.parse(event.data);
    console.log('Leilao Vencedor:', data);
});

eventSource.addEventListener('link_pagamento', function(event) {
    const data = JSON.parse(event.data);
    console.log('Link de Pagamento:', data);
});

eventSource.addEventListener('status_pagamento', function(event) {
    const data = JSON.parse(event.data);
    console.log('Status de Pagamento:', data);
});

eventSource.onerror = function(error) {
    console.error('Error:', error);
    eventSource.close();
};

eventSource.onopen = function() {
    console.log('Connection established');
};


const BASE_URL = 'http://127.0.0.1:5000'; // ajuste se seu backend roda em outra porta
const CLI_ID = Math.floor(Date.now() / 1000)*1000 + Math.floor(Math.random() * 1000); // id do cliente
console.log('Client ID:', CLI_ID);

function unixSecondsFromDatetimeLocal(value) {
    // value é no formato "YYYY-MM-DDTHH:MM" ou com segundos
    return Math.floor(new Date(value).getTime() / 1000);
}

async function criarLeilao() {
    try {
        const nome = document.getElementById('nome').value.trim();
        const desc = document.getElementById('descricao').value.trim();
        dataInicio = document.getElementById('dataInicio').value;
        dataFim = document.getElementById('dataFim').value;
        const lanceInicial = parseFloat(document.getElementById('lanceInicial').value);

        if (!nome || !desc || isNaN(lanceInicial)) {
            alert('Preencha todos os campos corretamente.');
            return;
        }
        // se data inicio ou data fim nao forem preenchidos, coloca inicio para daqui 5 segundos e fim para 10 segundos
        if (!dataInicio || !dataFim) {
            const now = Math.floor(Date.now() / 1000);
            dataInicio = new Date((now + 5) * 1000);
            dataFim = new Date((now + 10) * 1000);
        }

        const payload = {
            nome: nome,
            desc: desc,
            lance_inic: lanceInicial,
            data_inic: unixSecondsFromDatetimeLocal(dataInicio),
            data_fim: unixSecondsFromDatetimeLocal(dataFim)
        };

        const res = await fetch(`${BASE_URL}/criar_leilao`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!res.ok) {
            const err = await res.json().catch(()=>({error: 'Erro desconhecido'}));
            alert('Erro ao criar leilão: ' + (err.error || res.statusText));
            return;
        }

        const novo = await res.json();
        alert('Leilão criado com sucesso (ID: ' + novo.lei_id + ')');
        consultarLeiloes();
        // limpa formulário
        document.getElementById('leilaoForm').reset();
    } catch (e) {
        console.error(e);
        alert('Erro ao criar leilão.');
    }
}

async function efetuarLance() {
    try {
        const lei_id = parseInt(document.getElementById('leilaoId').value);
        const valor = parseFloat(document.getElementById('valorLance').value);

        if (isNaN(lei_id) || isNaN(valor)) {
            alert('Preencha ID do leilão e valor corretamente.');
            return;
        }

        const payload = { lei_id: lei_id, valor: valor, cli_id: CLI_ID };

        console.log('Efetuando lance:', payload);

        // const res = await fetch(`${BASE_URL}/efetuar_lance`, {
        //     method: 'POST',
        //     headers: { 'Content-Type': 'application/json' },
        //     body: JSON.stringify(payload)
        // });

        // if (!res.ok) {
        //     const err = await res.json().catch(()=>({error: 'Erro desconhecido'}));
        //     alert('Erro ao efetuar lance: ' + (err.error || res.statusText));
        //     return;
        // }

        // const data = await res.json();
        // alert('Lance enviado. Resposta: ' + (data.message || JSON.stringify(data)));
        // document.getElementById('lanceForm').reset();
    } catch (e) {
        console.error(e);
        alert('Erro ao efetuar lance.');
    }
}

async function consultarLeiloes() {
    try {
        const res = await fetch(`${BASE_URL}/consultar_leiloes`);
        if (!res.ok) {
            alert('Erro ao consultar leilões.');
            return;
        }
        const lista = await res.json();
        const container = document.getElementById('resultadoConsultaLeiloes');
        container.innerHTML = '';
        if (!Array.isArray(lista) || lista.length === 0) {
            container.textContent = 'Nenhum leilão encontrado.';
            return;
        }
        lista.forEach(l => {
            const el = document.createElement('div');
            el.className = 'leilao-item';
            el.innerHTML = `
                <strong>ID:</strong> ${l.lei_id} — <strong>${l.nome}</strong><br/>
                <em>${l.desc}</em><br/>
                Lance inicial: R$ ${Number(l.lance_inic).toFixed(2)} — Status: ${l.status}<br/>
                Início: ${new Date(l.data_inic * 1000).toLocaleString()} — Fim: ${new Date(l.data_fim * 1000).toLocaleString()}
            `;
            container.appendChild(el);
        });
    } catch (e) {
        console.error(e);
        alert('Erro ao consultar leilões.');
    }
}

async function registrarInteresse() {
    try {
        const lei_id = parseInt(document.getElementById('interesseLeilaoId').value);
        if (isNaN(lei_id)) { alert('ID inválido'); return; }
        const payload = { lei_id: lei_id, cli_id: CLI_ID };
        const res = await fetch(`${BASE_URL}/registrar_interesse`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) {
            const err = await res.json().catch(()=>({error: 'Erro desconhecido'}));
            alert('Erro ao registrar interesse: ' + (err.error || res.statusText));
            return;
        }
        alert('Interesse registrado.');
    } catch (e) {
        console.error(e);
        alert('Erro ao registrar interesse.');
    }
}

async function cancelarInteresse() {
    try {
        const lei_id = parseInt(document.getElementById('interesseLeilaoId').value);
        if (isNaN(lei_id)) { alert('ID inválido'); return; }
        const payload = { lei_id: lei_id, cli_id: CLI_ID };
        const res = await fetch(`${BASE_URL}/cancelar_interesse`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) {
            const err = await res.json().catch(()=>({error: 'Erro desconhecido'}));
            alert('Erro ao cancelar interesse: ' + (err.error || res.statusText));
            return;
        }
        alert('Interesse cancelado.');
    } catch (e) {
        console.error(e);
        alert('Erro ao cancelar interesse.');
    }
}