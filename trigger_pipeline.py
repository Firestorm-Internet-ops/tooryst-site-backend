#!/usr/bin/env python3
"""Manually trigger pipeline via API."""
import sys
import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def trigger_pipeline_api():
    """Trigger pipeline via API endpoint."""
    
    # Get configuration
    api_url = os.getenv("API_URL", "http://localhost:8000")
    admin_key = os.getenv("ADMIN_API_KEY")
    
    if not admin_key:
        print("❌ ADMIN_API_KEY not found in .env")
        return False
    
    print("="*80)
    print("MANUAL PIPELINE TRIGGER (via API)")
    print("="*80)
    print(f"🌐 API URL: {api_url}")
    print(f"🔑 Using admin key: {admin_key[:10]}...")
    print(f"⏱️  Triggering pipeline...")
    print("="*80)
    
    try:
        # Call the pipeline start API
        response = requests.post(
            f"{api_url}/api/v1/pipeline/start",
            headers={"X-Admin-Key": admin_key},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print("")
            print("="*80)
            print("✅ SUCCESS")
            print("="*80)
            print(f"Status: {result.get('status')}")
            print(f"Message: {result.get('message')}")
            print(f"Task ID: {result.get('task_id')}")
            print("")
            print("📋 Monitor progress:")
            print(f"  • Import logs:  tail -f backend/logs/import_*.log")
            print(f"  • Pipeline logs: tail -f backend/logs/pipeline_run_*.log")
            print(f"  • Task status:  curl -H 'X-Admin-Key: {admin_key}' {api_url}/api/v1/pipeline/status/{result.get('task_id')}")
            print("="*80)
            return True
        else:
            print("")
            print("="*80)
            print("❌ ERROR")
            print("="*80)
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
            print("="*80)
            return False
            
    except requests.exceptions.ConnectionError:
        print("")
        print("="*80)
        print("❌ CONNECTION ERROR")
        print("="*80)
        print(f"Could not connect to {api_url}")
        print("Make sure the backend is running: ./start_all.sh")
        print("="*80)
        return False
    except Exception as e:
        print("")
        print("="*80)
        print("❌ ERROR")
        print("="*80)
        print(f"Error: {str(e)}")
        print("="*80)
        return False

if __name__ == "__main__":
    success = trigger_pipeline_api()
    sys.exit(0 if success else 1)
