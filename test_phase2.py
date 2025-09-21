#!/usr/bin/env python3
"""
FikFap Scraper Phase 2 - Test Runner
Run all validation tests and demonstrate Phase 2 capabilities
"""
import sys
import os
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

def run_unit_tests():
    """Run unit tests"""
    print("🧪 Running Phase 2 Unit Tests...")
    print("=" * 50)

    try:
        # Import and run tests
        from tests.test_data_validation import run_tests
        success = run_tests()

        if success:
            print("\n✅ All unit tests passed!")
            return True
        else:
            print("\n❌ Some unit tests failed!")
            return False

    except ImportError as e:
        print(f"❌ Could not import test modules: {e}")
        print("Make sure all dependencies are installed: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

async def run_integration_demo():
    """Run integration demonstration"""
    print("\n🚀 Running Phase 2 Integration Demo...")
    print("=" * 50)

    try:
        # Import and run main demo
        from main import test_data_extraction
        await test_data_extraction()
        print("\n✅ Integration demo completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Integration demo failed: {e}")
        return False

def main():
    """Main test runner"""
    print("FikFap Scraper Phase 2 - Complete Test Suite")
    print("=" * 60)

    # Check if we can import required modules
    try:
        import aiohttp
        import pydantic
        import m3u8
        print("✅ Required dependencies available")
    except ImportError as e:
        print(f"❌ Missing dependencies: {e}")
        print("Please install: pip install -r requirements.txt")
        return 1

    # Run unit tests
    unit_tests_passed = run_unit_tests()

    # Run integration demo
    try:
        integration_passed = asyncio.run(run_integration_demo())
    except Exception as e:
        print(f"❌ Integration demo error: {e}")
        integration_passed = False

    # Summary
    print("\n" + "=" * 60)
    print("📋 Phase 2 Test Results Summary:")
    print(f"   Unit Tests: {'✅ PASSED' if unit_tests_passed else '❌ FAILED'}")
    print(f"   Integration Demo: {'✅ PASSED' if integration_passed else '❌ FAILED'}")

    if unit_tests_passed and integration_passed:
        print("\n🎉 Phase 2 - Data Extraction & Processing COMPLETE!")
        print("✨ All components working correctly")
        print("📋 Ready for Phase 3: Download Implementation")
        return 0
    else:
        print("\n⚠️  Some tests failed - check the output above")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
