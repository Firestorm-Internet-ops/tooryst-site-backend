#!/usr/bin/env python3
"""Generate a secure admin API key."""
import secrets
import string

def generate_admin_key(length=32):
    """Generate a secure random API key.
    
    Args:
        length: Length of the key (default 32)
        
    Returns:
        Secure random string
    """
    alphabet = string.ascii_letters + string.digits
    key = ''.join(secrets.choice(alphabet) for _ in range(length))
    return key


if __name__ == "__main__":
    key = generate_admin_key()
    print("\n" + "="*60)
    print("Generated Admin API Key:")
    print("="*60)
    print(f"\n{key}\n")
    print("="*60)
    print("\nAdd this to your .env file:")
    print(f"ADMIN_API_KEY={key}")
    print("="*60 + "\n")
