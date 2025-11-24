import pytest
import requests
import os

# Detectar puerto según ambiente
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')
PORT = 8002 if ENVIRONMENT == 'dev' else (8001 if ENVIRONMENT == 'qa' else 8000)
BASE_URL = f"http://localhost:{PORT}"

def test_security_headers():
    """Validar headers de seguridad según OWASP"""
    response = requests.get(f"{BASE_URL}/health")
    headers = response.headers
    
    required_headers = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': None,
        'Strict-Transport-Security': None
    }
    
    results = []
    passed = 0
    
    for header, expected in required_headers.items():
        if header not in headers:
            results.append(f"❌ Missing: {header}")
        elif expected and headers[header] != expected:
            results.append(f"⚠️ {header}: {headers[header]} (expected: {expected})")
        else:
            results.append(f"✅ {header}: OK")
            passed += 1
    
    print("\n📊 OWASP Security Headers Report:")
    for r in results:
        print(r)
    
    assert passed >= 3, f"Only {passed}/5 security headers present"

def test_cors_configured():
    """Validar CORS"""
    response = requests.options(f"{BASE_URL}/sql", headers={
        'Origin': 'https://pvsproyectof.pages.dev'
    })
    
    assert 'Access-Control-Allow-Origin' in response.headers
    print(f"✅ CORS: {response.headers.get('Access-Control-Allow-Origin')}")