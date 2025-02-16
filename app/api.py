from fastapi import FastAPI
from app.db import init_db, close_db
from app.transactions import router as transactions_router

app = FastAPI()


@app.on_event("startup")
def startup():
    """Initialize the database connection pool on app startup."""
    init_db()


@app.on_event("shutdown")
def shutdown():
    """Close database connections on app shutdown."""
    close_db()


# Include API routes
app.include_router(transactions_router)
