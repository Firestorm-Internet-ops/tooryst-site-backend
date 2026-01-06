#!/usr/bin/env python3
"""Debug script to understand JSON handling in SQLAlchemy."""

import sys
import os
from pathlib import Path

# Add backend to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_json_handling():
    """Debug JSON handling in SQLAlchemy."""
    print("🔍 Debugging JSON handling...")
    
    from app.infrastructure.persistence.db import SessionLocal
    from app.infrastructure.persistence import models
    from datetime import datetime
    
    session = SessionLocal()
    try:
        # Create a simple test
        test_data = {
            'contact_info': {
                'phone': {
                    'value': '+31 20 589 1400',
                    'url': 'tel:+31205891400'
                }
            },
            'opening_hours': [
                {
                    'day': 'Monday',
                    'open_time': '10:00',
                    'close_time': '22:00',
                    'is_closed': False
                }
            ],
            'highlights': ['Test highlight 1', 'Test highlight 2']
        }
        
        print(f"Original data types:")
        print(f"  contact_info: {type(test_data['contact_info'])}")
        print(f"  opening_hours: {type(test_data['opening_hours'])}")
        print(f"  highlights: {type(test_data['highlights'])}")
        
        # Create a new metadata object
        new_metadata = models.AttractionMetadata(
            attraction_id=9999,  # Use a non-existent ID for testing
            contact_info=test_data['contact_info'],
            opening_hours=test_data['opening_hours'],
            highlights=test_data['highlights'],
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        print(f"After creating model object:")
        print(f"  contact_info: {type(new_metadata.contact_info)}")
        print(f"  opening_hours: {type(new_metadata.opening_hours)}")
        print(f"  highlights: {type(new_metadata.highlights)}")
        
        # Check what SQLAlchemy will actually send to the database
        from sqlalchemy import inspect
        inspector = inspect(new_metadata)
        
        # Get the attributes
        contact_info_attr = inspector.attrs.contact_info
        opening_hours_attr = inspector.attrs.opening_hours
        highlights_attr = inspector.attrs.highlights
        
        print(f"SQLAlchemy attribute types:")
        print(f"  contact_info: {type(contact_info_attr)}")
        print(f"  opening_hours: {type(opening_hours_attr)}")
        print(f"  highlights: {type(highlights_attr)}")
        
        # Check the property values
        print(f"SQLAlchemy property types:")
        print(f"  contact_info property: {contact_info_attr.class_attribute.property}")
        print(f"  opening_hours property: {opening_hours_attr.class_attribute.property}")
        print(f"  highlights property: {highlights_attr.class_attribute.property}")
        
        # Check the column types
        print(f"SQLAlchemy column types:")
        print(f"  contact_info column: {contact_info_attr.class_attribute.property.columns[0].type}")
        print(f"  opening_hours column: {opening_hours_attr.class_attribute.property.columns[0].type}")
        print(f"  highlights column: {highlights_attr.class_attribute.property.columns[0].type}")
        
        # Try to add to session and see what happens
        session.add(new_metadata)
        
        # Check the session state
        print(f"After adding to session:")
        print(f"  contact_info: {type(new_metadata.contact_info)}")
        print(f"  opening_hours: {type(new_metadata.opening_hours)}")
        print(f"  highlights: {type(new_metadata.highlights)}")
        
        # Check what's in the session's identity map
        state = inspect(new_metadata)
        print(f"Session state:")
        print(f"  contact_info: {state.attrs.contact_info}")
        print(f"  opening_hours: {state.attrs.opening_hours}")
        print(f"  highlights: {state.attrs.highlights}")
        
        # Try to flush and see what SQL is generated
        try:
            session.flush()
            print("✅ Flush successful")
        except Exception as e:
            print(f"❌ Flush failed: {e}")
            session.rollback()
        
    except Exception as e:
        print(f"❌ Error during debug: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

def main():
    """Main debug function."""
    print("🚀 Starting JSON debug...")
    print("=" * 60)
    
    debug_json_handling()
    
    print("=" * 60)
    print("🎉 Debug complete")

if __name__ == "__main__":
    main()