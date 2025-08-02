from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import water_data

app = FastAPI(title="PrecipAPI", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(water_data.router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "Welcome to PrecipAPI!"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
