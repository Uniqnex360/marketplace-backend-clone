"""
Test database connection script
Run this first to ensure your MongoDB Atlas connection works
"""

from mongoengine import connect, disconnect
import sys

def test_connection():
    """Test MongoDB Atlas connection"""
    
    CONNECTION_STRING = "mongodb+srv://techteam:WcblsEme1Q1Vv7Rt@cluster0.5hrxigl.mongodb.net/ecommerce_db?retryWrites=true&w=majority&appName=Cluster0"
    
    print("="*60)
    print("MongoDB Atlas Connection Test")
    print("="*60)
    print()
    
    try:
        print("Attempting to connect to MongoDB Atlas...")
        connect(host=CONNECTION_STRING)
        print("✓ Successfully connected to MongoDB Atlas!")
        print()
        
        # Try to import models to verify project structure
        try:
            from ecommerce_tool.omnisight.models import Product, Marketplace, Brand
            print("✓ Successfully imported models")
            print()
            
            # Try to query marketplace
            marketplace_count = Marketplace.objects.count()
            print(f"✓ Database query successful")
            print(f"  Found {marketplace_count} marketplace(s) in database")
            print()
            
            # Try to query products
            product_count = Product.objects.count()
            print(f"  Found {product_count} product(s) in database")
            print()
            
        except ImportError as e:
            print("⚠ Warning: Could not import models")
            print(f"  Error: {str(e)}")
            print("  This is okay if you're testing connection only")
            print()
        
        disconnect()
        print("="*60)
        print("✓ Connection test PASSED - You're ready to import!")
        print("="*60)
        return True
        
    except Exception as e:
        print(f"✗ Connection test FAILED")
        print(f"  Error: {str(e)}")
        print()
        print("Troubleshooting:")
        print("  1. Check your internet connection")
        print("  2. Verify MongoDB Atlas IP whitelist")
        print("  3. Confirm username and password are correct")
        print("  4. Ensure database 'ecommerce_db' exists")
        print()
        print("="*60)
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)