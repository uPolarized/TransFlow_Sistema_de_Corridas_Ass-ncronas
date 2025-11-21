
# Minimal model placeholder for Pydantic usage (if needed)
from pydantic import BaseModel

class CorridaModel(BaseModel):
    id_corrida: str
    passageiro: dict
    motorista: dict
    origem: str
    destino: str
    valor_corrida: float
    forma_pagamento: str
