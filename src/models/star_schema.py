from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from src.config.database import Base

class DimDevice(Base):
    __tablename__ = "dim_devices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    device_type = Column(String(50), unique=True, nullable=False, index=True) # Desktop, Mobile, Tablet, Bot

    fact_clicks = relationship("FactClick", back_populates="device")


class DimBrowser(Base):
    __tablename__ = "dim_browsers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True) # Chrome, Safari, Edge, Firefox, LINE

    fact_clicks = relationship("FactClick", back_populates="browser")


class DimPlatform(Base):
    __tablename__ = "dim_platforms"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True) # iOS, Android, Windows, macOS, Linux

    fact_clicks = relationship("FactClick", back_populates="platform")


class DimReferrer(Base):
    __tablename__ = "dim_referrers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_domain = Column(String(255), unique=True, nullable=False, index=True) # Direct, google.com, facebook.com, etc.

    fact_clicks = relationship("FactClick", back_populates="referrer")


class DimCountry(Base):
    __tablename__ = "dim_countries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    country_code = Column(String(10), unique=True, nullable=False, index=True) # TH, US, JP, Unknown
    country_name = Column(String(100), nullable=False)

    fact_clicks = relationship("FactClick", back_populates="country")


class DimUrl(Base):
    __tablename__ = "dim_urls"

    id = Column(String(64), primary_key=True) # Matches UUID from Core DB
    original_url = Column(String(2048), nullable=False)
    short_code = Column(String(32), unique=True, nullable=False, index=True)
    custom_alias = Column(String(100), nullable=True, index=True)
    user_id = Column(String(64), nullable=True, index=True)

    fact_clicks = relationship("FactClick", back_populates="url")


class FactClick(Base):
    __tablename__ = "fact_clicks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    url_id = Column(String(64), ForeignKey("dim_urls.id"), nullable=False, index=True)
    device_id = Column(Integer, ForeignKey("dim_devices.id"), nullable=False, index=True)
    browser_id = Column(Integer, ForeignKey("dim_browsers.id"), nullable=False, index=True)
    platform_id = Column(Integer, ForeignKey("dim_platforms.id"), nullable=False, index=True)
    referrer_id = Column(Integer, ForeignKey("dim_referrers.id"), nullable=False, index=True)
    country_id = Column(Integer, ForeignKey("dim_countries.id"), nullable=True, index=True)
    
    clicked_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    click_count = Column(Integer, default=1, nullable=False)
    ip_masked = Column(String(64), nullable=True) # e.g. 192.168.***.***

    # Relationships
    url = relationship("DimUrl", back_populates="fact_clicks")
    device = relationship("DimDevice", back_populates="fact_clicks")
    browser = relationship("DimBrowser", back_populates="fact_clicks")
    platform = relationship("DimPlatform", back_populates="fact_clicks")
    referrer = relationship("DimReferrer", back_populates="fact_clicks")
    country = relationship("DimCountry", back_populates="fact_clicks")

    __table_args__ = (
        Index("idx_fact_clicks_lookup", "url_id", "clicked_at"),
    )
