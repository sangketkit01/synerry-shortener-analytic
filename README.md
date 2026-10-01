# Installation & Setup Guide

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ (Port 5432)

### Setup & Run Development

```bash
# 1. Create and activate virtual environment
python -m venv venv

# Windows:
.\venv\Scripts\activate
# Linux/macOS:
# source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Setup environment variables
cp .env.example .env

# 4. Start analytics server
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

Service will run at `http://localhost:8000` (Swagger UI at `/docs`)
