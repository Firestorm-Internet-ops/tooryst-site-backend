#!/usr/bin/env python3
"""
Data Completeness Report for Attractions

This script generates a report showing how complete the data is for each attraction
across all data sections (hero images, best time, weather, etc.).

Run:
    python scripts/data_completeness_report.py
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.infrastructure.persistence.db import get_db_session
from app.infrastructure.persistence.models import (
    Attraction, City, HeroImage, BestTimeData, WeatherForecast,
    Review, Tip, MapSnapshot, NearbyAttraction, AttractionMetadata,
    AudienceProfile, SocialVideo
)


def get_data_counts() -> Dict[str, Dict[str, int]]:
    """Get count of data records for each attraction and data type."""
    with get_db_session() as session:
        # Get all attractions with city info
        attractions = session.query(
            Attraction.id,
            Attraction.slug,
            Attraction.name,
            City.name.label('city_name'),
            City.country
        ).join(City).all()

        data_counts = {}

        for attr in attractions:
            attr_key = f"{attr.slug}"

            # Count records for each data type
            counts = {
                'hero_images': session.query(HeroImage).filter_by(attraction_id=attr.id).count(),
                'best_time': session.query(BestTimeData).filter_by(attraction_id=attr.id).count(),
                'weather': session.query(WeatherForecast).filter_by(attraction_id=attr.id).count(),
                'reviews': session.query(Review).filter_by(attraction_id=attr.id).count(),
                'tips': session.query(Tip).filter_by(attraction_id=attr.id).count(),
                'map': session.query(MapSnapshot).filter_by(attraction_id=attr.id).count(),
                'nearby': session.query(NearbyAttraction).filter_by(attraction_id=attr.id).count(),
                'metadata': session.query(AttractionMetadata).filter_by(attraction_id=attr.id).count(),
                'audience': session.query(AudienceProfile).filter_by(attraction_id=attr.id).count(),
                'social_videos': session.query(SocialVideo).filter_by(attraction_id=attr.id).count(),
            }

            data_counts[attr_key] = {
                'name': attr.name,
                'city': attr.city_name,
                'country': attr.country,
                **counts
            }

        return data_counts


def calculate_completeness(counts: Dict[str, int]) -> Tuple[int, float]:
    """Calculate completeness score (0-10) and percentage."""
    data_types = ['hero_images', 'best_time', 'weather', 'reviews', 'tips',
                  'map', 'nearby', 'metadata', 'audience', 'social_videos']

    complete_sections = 0
    total_sections = len(data_types)

    for data_type in data_types:
        if counts.get(data_type, 0) > 0:
            complete_sections += 1

    completeness_score = complete_sections
    completeness_percentage = (complete_sections / total_sections) * 100

    return completeness_score, completeness_percentage


def generate_report():
    """Generate and display the data completeness report."""
    print("🔍 Storyboard Data Completeness Report")
    print("=" * 80)

    data_counts = get_data_counts()

    if not data_counts:
        print("No attractions found in database.")
        return

    # Prepare data for display
    report_data = []
    for slug, data in data_counts.items():
        score, percentage = calculate_completeness(data)
        report_data.append({
            'slug': slug,
            'name': data['name'],
            'city': data['city'],
            'country': data['country'],
            'hero': data['hero_images'],
            'best_time': data['best_time'],
            'weather': data['weather'],
            'reviews': data['reviews'],
            'tips': data['tips'],
            'map': data['map'],
            'nearby': data['nearby'],
            'metadata': data['metadata'],
            'audience': data['audience'],
            'videos': data['social_videos'],
            'score': score,
            'percentage': percentage
        })

    # Sort by completeness score (ascending - show least complete first)
    report_data.sort(key=lambda x: x['score'])

    # Display summary
    total_attractions = len(report_data)
    avg_score = sum(item['score'] for item in report_data) / total_attractions
    avg_percentage = sum(item['percentage'] for item in report_data) / total_attractions

    print(f"📊 Summary:")
    print(f"   Total Attractions: {total_attractions}")
    print(".1f")
    print(".1f")
    print()

    # Display table header
    print("<10")
    print("-" * 120)

    # Display each attraction
    for item in report_data:
        status = "✅" if item['score'] == 10 else "⚠️" if item['score'] >= 7 else "❌"
        print("<10")

    print()
    print("📋 Legend:")
    print("   ✅ Complete (10/10) - All data sections populated")
    print("   ⚠️  Partial (7-9/10) - Most data available")
    print("   ❌ Incomplete (<7/10) - Missing significant data")
    print()
    print("🔧 Data Sections:")
    print("   hero: Hero images | best_time: Best time data | weather: Weather forecast")
    print("   reviews: Visitor reviews | tips: Safety/insider tips | map: Map & directions")
    print("   nearby: Nearby attractions | metadata: Contact & visitor info")
    print("   audience: Audience profiles | videos: Social media videos")


if __name__ == "__main__":
    try:
        generate_report()
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        sys.exit(1)