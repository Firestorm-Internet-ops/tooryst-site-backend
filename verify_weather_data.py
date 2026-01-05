"""Verify weather data freshness for all attractions."""
from app.infrastructure.persistence.db import SessionLocal
from app.infrastructure.persistence import models
from sqlalchemy import func
from datetime import datetime, timedelta

def verify_weather_data():
    session = SessionLocal()
    try:
        results = session.query(
            models.Attraction.id,
            models.Attraction.name,
            func.max(models.WeatherForecast.date_local).label('latest_date')
        ).outerjoin(
            models.WeatherForecast,
            models.Attraction.id == models.WeatherForecast.attraction_id
        ).group_by(
            models.Attraction.id,
            models.Attraction.name
        ).all()

        today = datetime.now().date()
        stale_threshold = today + timedelta(days=2)

        no_data = []
        stale_data = []
        fresh_data = []

        for attraction_id, name, latest_date in results:
            if latest_date is None:
                no_data.append((attraction_id, name))
            elif latest_date <= stale_threshold:
                stale_data.append((attraction_id, name, latest_date))
            else:
                fresh_data.append((attraction_id, name, latest_date))

        print(f"\n{'='*80}")
        print(f"Weather Data Freshness Report - {today}")
        print(f"{'='*80}\n")

        print(f"✗ No Weather Data: {len(no_data)} attractions")
        print(f"⚠ Stale Data (≤{stale_threshold}): {len(stale_data)} attractions")
        print(f"✓ Fresh Data (>{stale_threshold}): {len(fresh_data)} attractions")
        print(f"\nTotal Attractions: {len(results)}")

        if no_data:
            print(f"\n{'='*80}")
            print("Attractions WITHOUT weather data (first 10):")
            print(f"{'='*80}")
            for aid, name in no_data[:10]:
                print(f"  - ID {aid}: {name}")
            if len(no_data) > 10:
                print(f"  ... and {len(no_data) - 10} more")

        if stale_data:
            print(f"\n{'='*80}")
            print("Attractions with STALE weather data (first 10):")
            print(f"{'='*80}")
            for aid, name, date in stale_data[:10]:
                days_old = (today - date).days if date else None
                print(f"  - ID {aid}: {name} (latest: {date}, {days_old} days old)")
            if len(stale_data) > 10:
                print(f"  ... and {len(stale_data) - 10} more")

        if fresh_data:
            print(f"\n{'='*80}")
            print(f"Sample of FRESH weather data (first 5):")
            print(f"{'='*80}")
            for aid, name, date in fresh_data[:5]:
                days_ahead = (date - today).days if date else None
                print(f"  - ID {aid}: {name} (latest: {date}, {days_ahead} days ahead)")

        print(f"\n{'='*80}\n")

        return no_data, stale_data, fresh_data
    finally:
        session.close()

if __name__ == "__main__":
    verify_weather_data()
