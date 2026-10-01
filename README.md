# Analytics Engine & Data Pipeline (`synerry-shortener-analytic`)

บริการคลังข้อมูลสถิติและการประมวลผล Data Pipeline (ETL) พัฒนาด้วย Python 3.12, FastAPI, SQLAlchemy และ Pandas สำหรับระบบ Synerry Short URL

---

## 1. ข้อมูลการทดสอบออนไลน์ (Live Demo & Credentials)

* **URL ทดสอบระบบออนไลน์**: [https://synerry-shortener.eastasia.cloudapp.azure.com](https://synerry-shortener.eastasia.cloudapp.azure.com)
* **Administrator**: `admin@synerry.com` / `Admin@123456`
* **Standard User**: `demo@synerry.com` / `Demo@123456`

---

## 2. ลิงก์ Repositories ที่เกี่ยวข้อง (Microservices)

* **Frontend**: [https://github.com/sangketkit01/synerry-shortener-frontend](https://github.com/sangketkit01/synerry-shortener-frontend)
* **Backend API**: [https://github.com/sangketkit01/synerry-shortener-backend](https://github.com/sangketkit01/synerry-shortener-backend)
* **Analytics Engine**: [https://github.com/sangketkit01/synerry-shortener-analytic](https://github.com/sangketkit01/synerry-shortener-analytic)

---

## 3. วิธีการติดตั้งและเริ่มใช้งาน (Installation & Setup)

### ข้อกำหนดของระบบ (Prerequisites)
* Python v3.10 ขึ้นไป และ pip
* PostgreSQL v14 ขึ้นไป (พอร์ต 5432)

### ขั้นตอนการรัน

```bash
# 1. สร้างและเปิดใช้งาน Virtual Environment
# บน Windows:
python -m venv venv
.\venv\Scripts\activate

# บน Linux / macOS:
# python3 -m venv venv
# source venv/bin/activate

# 2. ติดตั้ง Dependencies
pip install -r requirements.txt

# 3. ตั้งค่าไฟล์ Environment Variables
cp .env.example .env

# 4. เริ่มรันเซิร์ฟเวอร์ในโหมดพัฒนา
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

API Documentation (Swagger UI) พร้อมใช้งานที่: `http://localhost:8000/docs`
