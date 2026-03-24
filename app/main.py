from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import Base, engine
from app import models
from app.routes import draws, stats, stats_v2

app = FastAPI(title="Lottery API Clean")

# CORS TEMPORALMENTE ABIERTO PARA LOVABLE
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(draws.router)
app.include_router(stats.router)
app.include_router(stats_v2.router)


@app.get("/")
def root():
    return {"status": "ok", "project": "lottery_api_clean"}


@app.get("/health")
def health():
    return {"ok": True}


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)