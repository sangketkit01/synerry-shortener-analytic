import time
from collections import defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from src.config.settings import settings
from src.api.routes import router
from src.scheduler.cron import start_scheduler, shutdown_scheduler

# Simple in-memory rate limiter / WAF middleware for Analytics service
class SecurityWAFMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_counts = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
        # Extract primary IP if multiple behind proxy
        if "," in client_ip:
            client_ip = client_ip.split(",")[0].strip()

        current_time = time.time()
        # Clean expired timestamps
        self.request_counts[client_ip] = [
            timestamp for timestamp in self.request_counts[client_ip]
            if current_time - timestamp < self.window_seconds
        ]

        if len(self.request_counts[client_ip]) >= self.max_requests:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"success": False, "message": "Rate limit exceeded on Analytics Service. Please slow down."}
            )

        self.request_counts[client_ip].append(current_time)

        response = await call_next(request)

        # Attach Security Headers (WAF protection)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        return response

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Star Schema tables and start scheduler
    try:
        from src.config.database import engine, Base
        import src.models # Ensure models are registered
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"[Database Init Warning] Could not connect to DB yet: {e}")

    start_scheduler()
    yield
    # Shutdown: Stop APScheduler
    shutdown_scheduler()

app = FastAPI(
    title="Short URL Analytics Service",
    description="OLAP Star Schema, WAF-Protected ETL Data Pipeline Service",
    version="1.0.0",
    lifespan=lifespan
)

# Apply Security WAF & Rate Limiting Middleware
app.add_middleware(SecurityWAFMiddleware, max_requests=100, window_seconds=60)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=settings.PORT, reload=True)
