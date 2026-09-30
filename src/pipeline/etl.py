import logging
from datetime import datetime
from urllib.parse import urlparse
import httpx
from user_agents import parse as parse_ua
from src.config.settings import settings
from src.config.database import SessionLocal
from sqlalchemy.exc import IntegrityError
from src.models.star_schema import (
    DimDevice,
    DimBrowser,
    DimPlatform,
    DimReferrer,
    DimCountry,
    DimUrl,
    FactClick,
)

logger = logging.getLogger("ETLPipeline")

def mask_ip(ip: str | None) -> str:
    if not ip or ip == "unknown":
        return "unknown"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.***.***"
    if ":" in ip: # IPv6
        return ip.split(":")[0] + ":****:****"
    return "masked"

def extract_referrer_domain(ref: str | None) -> str:
    if not ref or ref.strip() == "" or ref.lower() == "direct":
        return "Direct"
    try:
        parsed = urlparse(ref)
        hostname = parsed.hostname or ref
        return hostname.lower().replace("www.", "")
    except Exception:
        return "Other"

async def run_etl_pipeline() -> dict:
    """
    Core ETL Pipeline:
    1. Extract: Fetch raw clicks from Express Core Backend
    2. Transform: Parse user-agents, referrers, and mask IPs
    3. Load: Upsert into Star Schema (dim_* and fact_clicks)
    4. Acknowledge: Mark clicks as processed on Core Backend
    """
    logger.info("ETL Pipeline started at %s", datetime.utcnow().isoformat())

    backend_url = f"{settings.BACKEND_INTERNAL_URL}/api/v1/internal/clicks/raw"
    mark_url = f"{settings.BACKEND_INTERNAL_URL}/api/v1/internal/clicks/mark-processed"
    headers = {"x-internal-key": settings.INTERNAL_PIPELINE_KEY}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(backend_url, headers=headers)
            if resp.status_code != 200:
                logger.error("Failed to fetch raw clicks: %s", resp.text)
                return {"status": "error", "message": f"Backend returned {resp.status_code}"}

            data = resp.json()
            raw_clicks = data.get("clicks", [])

        if not raw_clicks:
            logger.info("ETL: No new raw clicks to process.")
            return {
                "status": "success",
                "processed_events": 0,
                "message": "No new events to process",
                "timestamp": datetime.utcnow().isoformat()
            }

        db = SessionLocal()
        processed_ids = []

        try:
            # Dimension Caches to minimize queries
            device_cache = {d.device_type: d.id for d in db.query(DimDevice).all()}
            browser_cache = {b.name: b.id for b in db.query(DimBrowser).all()}
            platform_cache = {p.name: p.id for p in db.query(DimPlatform).all()}
            referrer_cache = {r.source_domain: r.id for r in db.query(DimReferrer).all()}
            country_cache = {c.country_code: c.id for c in db.query(DimCountry).all()}

            def get_or_create_device(device_name: str) -> int:
                if device_name not in device_cache:
                    dim = db.query(DimDevice).filter(DimDevice.device_type == device_name).first()
                    if not dim:
                        try:
                            dim = DimDevice(device_type=device_name)
                            db.add(dim)
                            db.flush()
                        except IntegrityError:
                            db.rollback()
                            dim = db.query(DimDevice).filter(DimDevice.device_type == device_name).first()
                    if dim:
                        device_cache[device_name] = dim.id
                return device_cache.get(device_name, 1)

            def get_or_create_browser(browser_name: str) -> int:
                if browser_name not in browser_cache:
                    dim = db.query(DimBrowser).filter(DimBrowser.name == browser_name).first()
                    if not dim:
                        try:
                            dim = DimBrowser(name=browser_name)
                            db.add(dim)
                            db.flush()
                        except IntegrityError:
                            db.rollback()
                            dim = db.query(DimBrowser).filter(DimBrowser.name == browser_name).first()
                    if dim:
                        browser_cache[browser_name] = dim.id
                return browser_cache.get(browser_name, 1)

            def get_or_create_platform(platform_name: str) -> int:
                if platform_name not in platform_cache:
                    dim = db.query(DimPlatform).filter(DimPlatform.name == platform_name).first()
                    if not dim:
                        try:
                            dim = DimPlatform(name=platform_name)
                            db.add(dim)
                            db.flush()
                        except IntegrityError:
                            db.rollback()
                            dim = db.query(DimPlatform).filter(DimPlatform.name == platform_name).first()
                    if dim:
                        platform_cache[platform_name] = dim.id
                return platform_cache.get(platform_name, 1)

            def get_or_create_referrer(ref_domain: str) -> int:
                if ref_domain not in referrer_cache:
                    dim = db.query(DimReferrer).filter(DimReferrer.source_domain == ref_domain).first()
                    if not dim:
                        try:
                            dim = DimReferrer(source_domain=ref_domain)
                            db.add(dim)
                            db.flush()
                        except IntegrityError:
                            db.rollback()
                            dim = db.query(DimReferrer).filter(DimReferrer.source_domain == ref_domain).first()
                    if dim:
                        referrer_cache[ref_domain] = dim.id
                return referrer_cache.get(ref_domain, 1)

            def get_or_create_country(code: str, name: str) -> int:
                if code not in country_cache:
                    dim = db.query(DimCountry).filter(DimCountry.country_code == code).first()
                    if not dim:
                        try:
                            dim = DimCountry(country_code=code, country_name=name)
                            db.add(dim)
                            db.flush()
                        except IntegrityError:
                            db.rollback()
                            dim = db.query(DimCountry).filter(DimCountry.country_code == code).first()
                    if dim:
                        country_cache[code] = dim.id
                return country_cache.get(code, 1)

            for click in raw_clicks:
                url_data = click.get("url")
                if not url_data:
                    continue

                url_id = url_data.get("id")
                # Ensure DimUrl exists
                dim_url = db.query(DimUrl).filter(DimUrl.id == url_id).first()
                if not dim_url:
                    try:
                        dim_url = DimUrl(
                            id=url_id,
                            original_url=url_data.get("originalUrl"),
                            short_code=url_data.get("shortCode"),
                            custom_alias=url_data.get("customAlias"),
                            user_id=url_data.get("userId"),
                        )
                        db.add(dim_url)
                        db.flush()
                    except IntegrityError:
                        db.rollback()
                        dim_url = db.query(DimUrl).filter(DimUrl.id == url_id).first()

                # User Agent parsing
                ua_str = click.get("userAgent") or ""
                ua = parse_ua(ua_str)

                if ua.is_bot:
                    device_type = "Bot"
                elif ua.is_mobile:
                    device_type = "Mobile"
                elif ua.is_tablet:
                    device_type = "Tablet"
                elif ua.is_pc:
                    device_type = "Desktop"
                else:
                    device_type = "Other"

                browser_name = ua.browser.family or "Other"
                platform_name = ua.os.family or "Other"

                # Referrer parsing
                referrer_domain = extract_referrer_domain(click.get("referrer"))

                # Country
                country_code = click.get("country") or "TH"
                country_name = "Thailand" if country_code == "TH" else "Global"

                # Get dimension IDs
                device_id = get_or_create_device(device_type)
                browser_id = get_or_create_browser(browser_name)
                platform_id = get_or_create_platform(platform_name)
                ref_id = get_or_create_referrer(referrer_domain)
                country_id = get_or_create_country(country_code, country_name)

                # Parse clicked_at
                clicked_at_str = click.get("clickedAt")
                if clicked_at_str:
                    try:
                        clicked_at = datetime.fromisoformat(clicked_at_str.replace("Z", "+00:00"))
                    except Exception:
                        clicked_at = datetime.utcnow()
                else:
                    clicked_at = datetime.utcnow()

                masked_ip_str = mask_ip(click.get("ipAddress"))
                # Check for duplicate event in FactClick to maintain idempotency
                existing_fact = db.query(FactClick).filter(
                    FactClick.url_id == url_id,
                    FactClick.clicked_at == clicked_at,
                    FactClick.ip_masked == masked_ip_str,
                ).first()
                if existing_fact:
                    processed_ids.append(click.get("id"))
                    continue

                # Create FactClick record
                fact = FactClick(
                    url_id=url_id,
                    device_id=device_id,
                    browser_id=browser_id,
                    platform_id=platform_id,
                    referrer_id=ref_id,
                    country_id=country_id,
                    clicked_at=clicked_at,
                    click_count=1,
                    ip_masked=masked_ip_str,
                )
                db.add(fact)
                processed_ids.append(click.get("id"))

            db.commit()

            # Mark processed on core backend
            if processed_ids:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.post(
                        mark_url,
                        headers=headers,
                        json={"ids": processed_ids}
                    )

            logger.info("ETL Pipeline completed. Processed %d events.", len(processed_ids))
            return {
                "status": "success",
                "processed_events": len(processed_ids),
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            db.rollback()
            logger.error("ETL DB transaction failed: %s", e)
            raise e
        finally:
            db.close()

    except Exception as e:
        logger.error("ETL Pipeline fatal error: %s", e)
        return {"status": "error", "message": str(e)}
