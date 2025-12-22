#!/usr/bin/env python3
"""Analyze tips generation JSON output."""
import json
import sys
from pathlib import Path
from collections import defaultdict


def analyze_tips_file(filepath):
    """Analyze a tips generation JSON file."""
    
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    print(f"\n{'='*70}")
    print(f"Tips Generation Analysis")
    print(f"{'='*70}\n")
    
    print(f"Generated at: {data['generated_at']}")
    print(f"Total attractions: {data['total_attractions']}\n")
    
    # Summary stats
    summary = data['summary']
    print(f"Summary:")
    print(f"  ✓ Success: {summary['success']}")
    print(f"  ✗ Failed: {summary['failed']}")
    print(f"  ⊘ No data: {summary['no_data']}")
    print(f"  Success rate: {(summary['success'] / data['total_attractions'] * 100):.1f}%\n")
    
    # Source breakdown
    print(f"Tips Sources:")
    for source, count in sorted(summary['sources'].items(), key=lambda x: x[1], reverse=True):
        pct = (count / summary['success'] * 100) if summary['success'] > 0 else 0
        print(f"  {source}: {count} ({pct:.1f}%)")
    print()
    
    # Tips statistics
    tips_by_attraction = data['tips_by_attraction']
    total_tips = 0
    tips_by_type = defaultdict(int)
    tips_by_scope = defaultdict(int)
    attractions_with_tips = 0
    
    for attraction_id, attraction_data in tips_by_attraction.items():
        tips = attraction_data.get('tips', [])
        if tips:
            attractions_with_tips += 1
            total_tips += len(tips)
            
            for tip in tips:
                tips_by_type[tip.get('tip_type', 'UNKNOWN')] += 1
                tips_by_scope[tip.get('scope', 'unknown')] += 1
    
    print(f"Tips Statistics:")
    print(f"  Total tips generated: {total_tips}")
    print(f"  Attractions with tips: {attractions_with_tips}")
    print(f"  Average tips per attraction: {(total_tips / attractions_with_tips if attractions_with_tips > 0 else 0):.1f}\n")
    
    print(f"Tips by Type:")
    for tip_type, count in sorted(tips_by_type.items()):
        pct = (count / total_tips * 100) if total_tips > 0 else 0
        print(f"  {tip_type}: {count} ({pct:.1f}%)")
    print()
    
    print(f"Tips by Scope:")
    for scope, count in sorted(tips_by_scope.items()):
        pct = (count / total_tips * 100) if total_tips > 0 else 0
        print(f"  {scope}: {count} ({pct:.1f}%)")
    print()
    
    # Attractions without tips
    attractions_without_tips = [
        (aid, adata['attraction_name'], adata.get('error', 'No data'))
        for aid, adata in tips_by_attraction.items()
        if not adata.get('tips')
    ]
    
    if attractions_without_tips:
        print(f"Attractions without tips ({len(attractions_without_tips)}):")
        for aid, name, reason in attractions_without_tips[:10]:  # Show first 10
            print(f"  - {name} (ID: {aid}): {reason}")
        if len(attractions_without_tips) > 10:
            print(f"  ... and {len(attractions_without_tips) - 10} more")
    print()
    
    # Sample tips
    print(f"Sample Tips:")
    sample_count = 0
    for attraction_id, attraction_data in tips_by_attraction.items():
        tips = attraction_data.get('tips', [])
        if tips and sample_count < 3:
            print(f"\n  {attraction_data['attraction_name']} ({attraction_data['city_name']}):")
            print(f"    Source: {attraction_data['source']}")
            for tip in tips[:2]:  # Show first 2 tips
                print(f"    - [{tip['tip_type']}] {tip['text'][:80]}...")
            sample_count += 1
    
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Find the latest tips JSON file
        logs_dir = Path(__file__).parent.parent / "logs"
        tips_files = sorted(logs_dir.glob("tips_generation_*.json"), reverse=True)
        
        if not tips_files:
            print("Error: No tips generation JSON files found in backend/logs/")
            sys.exit(1)
        
        filepath = tips_files[0]
        print(f"Analyzing latest file: {filepath.name}")
    else:
        filepath = Path(sys.argv[1])
    
    if not filepath.exists():
        print(f"Error: File not found: {filepath}")
        sys.exit(1)
    
    analyze_tips_file(filepath)
