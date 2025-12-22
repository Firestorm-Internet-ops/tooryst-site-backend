#!/usr/bin/env python3
"""Trigger pipeline for specific attractions."""
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Import Celery app
from app.celery_app import celery_app
from app.tasks.parallel_pipeline_tasks import orchestrate_pipeline

# List of the 18 new attractions
attraction_slugs = [
    "catamaran-cruise-venice",
    "fun-spot-america-orlando",
    "madame-tussauds-dubai",
    "madame-tussauds-las-vega",
    "madame-tussauds-london",
    "medieval-torture-museum-los-angeles",
    "museum-of-contemporary-art-chicago",
    "museum-of-illusions-dubai",
    "museum-of-illusions-istanbul",
    "museum-of-illusions-madrid",
    "museum-of-illusions-miami",
    "museum-of-illusions-milan",
    "museum-of-illusions-rome",
    "national-archaeological-museum-florence",
    "natural-history-museum-las-vegas",
    "neue-nationalgalerie-las-vegas",
    "segway-tour-krakow",
    "xtreme-parasail-in-honolulu-nashville",
]

print("="*80)
print("TRIGGERING PIPELINE FOR 18 ATTRACTIONS")
print("="*80)
print(f"Attractions: {', '.join(attraction_slugs)}")
print("="*80)

# Trigger the pipeline
result = orchestrate_pipeline.delay(attraction_slugs)

print(f"✓ Pipeline triggered!")
print(f"Task ID: {result.id}")
print(f"Status: {result.status}")
print("")
print("Monitor progress:")
print(f"  • Pipeline logs: tail -f backend/logs/pipeline_run_*.log")
print(f"  • Task status: celery -A app.celery_app inspect active")
print("="*80)
