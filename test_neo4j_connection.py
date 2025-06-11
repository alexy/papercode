#!/usr/bin/env python3

import sys
from neo4j import GraphDatabase
from models import PapersWithCodeGraph

def test_neo4j_connection(uri="bolt://localhost:7687", username="neo4j", password="password"):
    """Test Neo4j connection with different credentials"""
    print(f"Testing Neo4j connection:")
    print(f"  URI: {uri}")
    print(f"  Username: {username}")
    print(f"  Password: {'*' * len(password)}")
    
    try:
        # Test direct connection
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            if record and record['test'] == 1:
                print("✅ Basic Neo4j connection successful")
            else:
                print("❌ Basic Neo4j connection failed - no result")
                return False
        
        driver.close()
        
        # Test with our wrapper
        print("Testing PapersWithCodeGraph wrapper...")
        graph = PapersWithCodeGraph(uri, username, password)
        
        stats = graph.get_graph_stats()
        print(f"✅ Graph stats retrieved: {stats}")
        
        graph.close()
        return True
        
    except Exception as e:
        print(f"❌ Neo4j connection failed: {e}")
        return False

def suggest_fixes():
    """Suggest common fixes for Neo4j connection issues"""
    print("\n🔧 Common Neo4j Setup Issues:")
    print("1. **Wrong Password**: Default Neo4j password is often changed during setup")
    print("   - Try: neo4j, password, admin, or check your Neo4j setup")
    print("   - Reset via Neo4j Browser: http://localhost:7474")
    
    print("\n2. **Neo4j Not Running**: Make sure Neo4j is started")
    print("   - Neo4j Desktop: Start database")
    print("   - Docker: docker run -p 7474:7474 -p 7687:7687 neo4j:latest")
    print("   - Service: sudo systemctl start neo4j")
    
    print("\n3. **Wrong URI**: Check if Neo4j is on different host/port")
    print("   - Local: bolt://localhost:7687")
    print("   - Cloud: bolt://your-cloud-instance:7687")
    
    print("\n4. **Test Connection**:")
    print("   python test_neo4j_connection.py bolt://localhost:7687 neo4j YOUR_PASSWORD")

def main():
    """Main function with command line arguments"""
    if len(sys.argv) == 4:
        uri, username, password = sys.argv[1:4]
    elif len(sys.argv) == 3:
        uri, username, password = sys.argv[1], sys.argv[2], "password"
    else:
        uri, username, password = "bolt://localhost:7687", "neo4j", "password"
    
    success = test_neo4j_connection(uri, username, password)
    
    if not success:
        suggest_fixes()
        
        # Try common alternative passwords
        print("\n🔍 Trying common passwords...")
        common_passwords = ["neo4j", "admin", "test", "dev", ""]
        
        for pwd in common_passwords:
            if pwd != password:
                print(f"Trying password: '{pwd}'")
                if test_neo4j_connection(uri, username, pwd):
                    print(f"✅ Success with password: '{pwd}'")
                    print(f"Use: --neo4j-password {pwd}")
                    break
        else:
            print("❌ None of the common passwords worked")

if __name__ == "__main__":
    main()