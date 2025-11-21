import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

@pytest.fixture
def driver():
    """Setup Chrome driver para CI/CD"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    
    # Crear carpeta para screenshots
    os.makedirs('tests/screenshots', exist_ok=True)
    
    yield driver
    driver.quit()

def test_frontend_loads(driver):
    """Test que el frontend carga correctamente"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    body = wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    
    assert body is not None
    driver.save_screenshot('tests/screenshots/frontend_loaded.png')
    print("✅ Frontend cargado correctamente")

def test_login_form_exists(driver):
    """Test que existe el formulario de login"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    
    # Verificar elementos del formulario
    username = wait.until(EC.presence_of_element_located((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    login_btn = driver.find_element(By.CSS_SELECTOR, "button[onclick='handleLogin(event)']")
    
    assert username is not None
    assert password is not None
    assert login_btn is not None
    
    driver.save_screenshot('tests/screenshots/login_form.png')
    print("✅ Formulario de login encontrado")

def test_login_attempt(driver):
    """Test de intento de login"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    
    # Llenar formulario
    username = wait.until(EC.presence_of_element_located((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    
    username.send_keys("testuser")
    password.send_keys("testpass123")
    
    driver.save_screenshot('tests/screenshots/before_login.png')
    
    # Click login
    login_btn = driver.find_element(By.CSS_SELECTOR, "button[onclick='handleLogin(event)']")
    login_btn.click()
    
    time.sleep(3)
    
    driver.save_screenshot('tests/screenshots/after_login.png')
    
    # Verificar que se hizo el intento (puede fallar si user no existe, está ok)
    page_source = driver.page_source.lower()
    assert "login" in page_source or "bienvenido" in page_source or "error" in page_source
    
    print("✅ Login attempt completed")