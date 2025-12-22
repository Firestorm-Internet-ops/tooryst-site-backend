#!/usr/bin/env python
"""Verify that the checkpoint system is fully installed and working."""
import sys
from pathlib import Path

def check_files():
    """Check if all required files exist."""
    print("\n📁 Checking files...")
    files = [
        'app/core/checkpoint_manager.py',
        'app/tasks/pipeline_resume.py',
        'sql/add_pipeline_checkpoints.sql',
        'tests/test_checkpoint_system.py',
    ]
    
    for file in files:
        path = Path(file)
        if path.exists():
            print(f"  ✅ {file}")
        else:
            print(f"  ❌ {file} - NOT FOUND")
            return False
    return True

def check_database():
    """Check if database table exists."""
    print("\n🗄️  Checking database...")
    try:
        from app.infrastructure.persistence.db import SessionLocal
        from sqlalchemy import text
        
        session = SessionLocal()
        try:
            result = session.execute(text("DESCRIBE pipeline_checkpoints;")).fetchall()
            if result:
                print(f"  ✅ pipeline_checkpoints table exists ({len(result)} columns)")
                return True
            else:
                print("  ❌ pipeline_checkpoints table not found")
                return False
        finally:
            session.close()
    except Exception as e:
        print(f"  ❌ Database error: {e}")
        return False

def check_imports():
    """Check if all modules can be imported."""
    print("\n📦 Checking imports...")
    try:
        from app.core.checkpoint_manager import checkpoint_manager
        print("  ✅ checkpoint_manager imported")
        
        from app.tasks.pipeline_resume import resume_pipeline, get_pipeline_status
        print("  ✅ resume_pipeline imported")
        print("  ✅ get_pipeline_status imported")
        
        from app.tasks.parallel_pipeline_tasks import (
            should_skip_stage, record_stage_completion
        )
        print("  ✅ should_skip_stage imported")
        print("  ✅ record_stage_completion imported")
        
        return True
    except Exception as e:
        print(f"  ❌ Import error: {e}")
        return False

def check_functionality():
    """Check if checkpoint system works."""
    print("\n⚙️  Checking functionality...")
    try:
        from app.core.checkpoint_manager import checkpoint_manager
        
        # Create a test checkpoint
        checkpoint_manager.create_checkpoint(
            pipeline_run_id=9999,
            attraction_id=9999,
            stage_name='test',
            status='completed'
        )
        print("  ✅ Checkpoint creation works")
        
        # Retrieve it
        checkpoint = checkpoint_manager.get_checkpoint(9999, 9999, 'test')
        if checkpoint:
            print("  ✅ Checkpoint retrieval works")
        else:
            print("  ❌ Checkpoint retrieval failed")
            return False
        
        # Check if completed
        is_done = checkpoint_manager.is_stage_completed(9999, 9999, 'test')
        if is_done:
            print("  ✅ Stage completion check works")
        else:
            print("  ❌ Stage completion check failed")
            return False
        
        return True
    except Exception as e:
        print(f"  ❌ Functionality error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all checks."""
    print("=" * 60)
    print("🔍 CHECKPOINT SYSTEM VERIFICATION")
    print("=" * 60)
    
    checks = [
        ("Files", check_files),
        ("Database", check_database),
        ("Imports", check_imports),
        ("Functionality", check_functionality),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} check failed: {e}")
            results.append((name, False))
    
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:20} {status}")
        if not result:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n✅ ALL CHECKS PASSED - SYSTEM IS READY!")
        print("\nTo resume a pipeline:")
        print("  from app.tasks.pipeline_resume import resume_pipeline")
        print("  resume_pipeline.delay(pipeline_run_id=29)")
        return 0
    else:
        print("\n❌ SOME CHECKS FAILED - PLEASE FIX ISSUES ABOVE")
        return 1

if __name__ == '__main__':
    sys.exit(main())
