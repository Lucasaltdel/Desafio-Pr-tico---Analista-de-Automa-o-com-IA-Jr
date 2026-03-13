from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

@app.get("/")
def home():
    return {
        "message": "API de solicitações operacionais ativa",
        "docs": "http://127.0.0.1:8000/docs"
    }

class Contato(BaseModel):
    email: str | None = None
    telefone: str | None = None


class Solicitacao(BaseModel):
    id: str
    cliente: str
    tipo: str
    queue: str
    priority_final: str
    descricao: str
    contato: Contato
    created_at: str


@app.post("/solicitacoes")
def receber_solicitacao(data: Solicitacao):

    return {
        "status": "recebido",
        "id": data.id,
        "queue": data.queue
    }