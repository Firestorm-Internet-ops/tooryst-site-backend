"""Script to reset YouTube API quota flag in Redis."""
import os
import sys
import pathlib

# Ensure project root is on sys.path for imports
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from app.core.quota_manager import quota_manager

# Load environment variables
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))


def main():
    """Reset YouTube quota flag."""
    print("=" * 80)
    print("RESET YOUTUBE API QUOTA FLAG")
    print("=" * 80)
    print()
    
    # Check current status
    status = quota_manager.get_quota_status("youtube")
    print("Current YouTube quota status:")
    print(f"  Quota exceeded: {status.get('quota_exceeded', False)}")
    if status.get('resets_in_hours', 0) > 0:
        print(f"  Resets in: {status.get('resets_in_hours', 0)} hours")
    print()
    
    if not status.get('quota_exceeded', False):
        print("✓ YouTube quota is NOT marked as exceeded. No action needed.")
        return
    
    # Reset the quota flag
    print("Resetting YouTube quota flag...")
    quota_manager.reset_quota("youtube")
    
    # Verify reset
    new_status = quota_manager.get_quota_status("youtube")
    print()
    print("New YouTube quota status:")
    print(f"  Quota exceeded: {new_status.get('quota_exceeded', False)}")
    
    if not new_status.get('quota_exceeded', False):
        print()
        print("✓ Successfully reset YouTube quota flag!")
        print("  YouTube API calls will now be allowed.")
    else:
        print()
        print("⚠ Warning: Quota flag still shows as exceeded.")
        print("  This might be a Redis connection issue.")


if __name__ == "__main__":
    main()
