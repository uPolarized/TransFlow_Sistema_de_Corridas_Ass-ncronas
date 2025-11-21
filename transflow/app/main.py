
import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from producer import publish_corrida_event
from database.mongo_client import get_mongo_collection
from database.redis_client import get_redis_client
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="TransFlow - API")

db = get_mongo_collection()
redis = get_redis_client()

class Passageiro(BaseModel):
    nome: str
    telefone: str

class Motorista(BaseModel):
    nome: str
    nota: float

class Corrida(BaseModel):
    id_corrida: str
    passageiro: Passageiro
    motorista: Motorista
    origem: str
    destino: str
    valor_corrida: float
    forma_pagamento: str

@app.post('/corridas', status_code=201)
async def criar_corrida(corrida: Corrida):
    
    payload = corrida.dict()
    await publish_corrida_event(payload)
    return {'status': 'published', 'id_corrida': corrida.id_corrida}

@app.get('/corridas')
async def listar_corridas():
    docs = list(db.find({}))
    for d in docs:
        d['_id'] = str(d['_id'])
    return docs

@app.get('/corridas/{forma_pagamento}')
async def filtrar_corridas(forma_pagamento: str):
    docs = list(db.find({'forma_pagamento': forma_pagamento}))
    for d in docs:
        d['_id'] = str(d['_id'])
    return docs

@app.get('/saldo/{motorista}')
async def obter_saldo(motorista: str):
    key = f"saldo:{motorista.lower()}"
    saldo = redis.get(key)
    if saldo is None:
        raise HTTPException(status_code=404, detail="Motorista não encontrado no Redis")
    try:
        saldo_val = float(saldo)
    except:
        saldo_val = 0.0
    return {'motorista': motorista, 'saldo': saldo_val}
