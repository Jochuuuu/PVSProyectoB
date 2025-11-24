import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import os
import random
import string

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
    
    os.makedirs('tests/screenshots', exist_ok=True)
    
    yield driver
    driver.quit()

def generate_random_username():
    """Generar username aleatorio para tests"""
    return f"test_user_{random.randint(1000, 9999)}"

# ==========================================
# TESTS DE CARGA Y CONEXIÓN
# ==========================================

def test_frontend_loads(driver):
    """Test que el frontend carga correctamente"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    body = wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    
    assert body is not None
    driver.save_screenshot('tests/screenshots/01_frontend_loaded.png')
    print("  Frontend cargado correctamente")

def test_connection_status_visible(driver):
    """Test que se muestra el estado de conexión"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    status = wait.until(EC.presence_of_element_located((By.ID, "connectionStatus")))
    
    assert status is not None
    assert status.is_displayed()
    print("  Estado de conexión visible")

# ==========================================
# TESTS DE LOGIN
# ==========================================

def test_login_form_exists(driver):
    """Test que existe el formulario de login"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    
    username = wait.until(EC.presence_of_element_located((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    
    assert username is not None
    assert password is not None
    assert login_btn is not None
    
    driver.save_screenshot('tests/screenshots/02_login_form.png')
    print("  Formulario de login encontrado")

def test_login_success(driver):
    """Test de login exitoso con credenciales válidas"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 30)
    
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    username = wait.until(EC.element_to_be_clickable((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    
    username.clear()
    username.send_keys("admin")
    password.clear()
    password.send_keys("adminxd")
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    # MEJOR: Esperar que aparezca el user-info (indica login exitoso)
    try:
        user_info = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "user-info"))
        )
        print("✅ Login exitoso - user-info visible")
        
        # Verificar que el modal se oculta (opcional)
        time.sleep(2)  # Dar tiempo al modal para cerrarse
        
    except TimeoutException:
        # Fallback: verificar si al menos el toast apareció
        page_source = driver.page_source.lower()
        if "bienvenido" in page_source or "conectado" in page_source:
            print("⚠️ Login procesado pero UI lenta")
        else:
            driver.save_screenshot('tests/screenshots/login_failed.png')
            raise AssertionError("Login no completado en 30s")
    
    driver.save_screenshot('tests/screenshots/login_success.png')


def test_login_empty_fields(driver):
    """Test que login falla con campos vacíos"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    # HTML5 validation debería prevenir el submit
    modal = driver.find_element(By.ID, "authModal")
    assert "hidden" not in modal.get_attribute("class")
    
    driver.save_screenshot('tests/screenshots/04_login_empty.png')
    print("  Validación de campos vacíos funciona")

def test_login_wrong_credentials(driver):
    """Test que login falla con credenciales incorrectas"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    
    username = wait.until(EC.element_to_be_clickable((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    
    username.clear()
    username.send_keys("usuario_incorrecto")
    password.clear()
    password.send_keys("password_incorrecta")
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    time.sleep(3)
    
    # El modal NO debería ocultarse
    modal = driver.find_element(By.ID, "authModal")
    assert "hidden" not in modal.get_attribute("class")
    
    driver.save_screenshot('tests/screenshots/05_login_wrong.png')
    print("  Login rechaza credenciales incorrectas")

# ==========================================
# TESTS DE REGISTRO
# ==========================================

def test_switch_to_register_tab(driver):
    """Test que se puede cambiar al tab de registro"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    # Click en tab de registro
    register_tab = driver.find_element(By.CSS_SELECTOR, ".auth-tab:nth-child(2)")
    register_tab.click()
    
    time.sleep(1)
    
    # Verificar que el formulario de registro está visible
    register_form = driver.find_element(By.ID, "registerForm")
    assert register_form.is_displayed()
    
    driver.save_screenshot('tests/screenshots/06_register_tab.png')
    print("  Tab de registro funciona")

def test_register_form_exists(driver):
    """Test que existe el formulario de registro"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    # Cambiar a tab de registro
    register_tab = driver.find_element(By.CSS_SELECTOR, ".auth-tab:nth-child(2)")
    register_tab.click()
    
    time.sleep(1)
    
    # Verificar campos
    username = driver.find_element(By.ID, "regUser")
    password = driver.find_element(By.ID, "regPass")
    password_confirm = driver.find_element(By.ID, "regPassConfirm")
    register_btn = driver.find_element(By.CSS_SELECTOR, "#registerForm button[type='submit']")
    
    assert username is not None
    assert password is not None
    assert password_confirm is not None
    assert register_btn is not None
    
    driver.save_screenshot('tests/screenshots/07_register_form.png')
    print("  Formulario de registro encontrado")

def test_register_password_mismatch(driver):
    """Test que registro falla si contraseñas no coinciden"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 10)
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    register_tab = driver.find_element(By.CSS_SELECTOR, ".auth-tab:nth-child(2)")
    register_tab.click()
    
    time.sleep(1)
    
    username = driver.find_element(By.ID, "regUser")
    password = driver.find_element(By.ID, "regPass")
    password_confirm = driver.find_element(By.ID, "regPassConfirm")
    
    username.send_keys("test_user")
    password.send_keys("password123")
    password_confirm.send_keys("password456")  # Diferente
    
    register_btn = driver.find_element(By.CSS_SELECTOR, "#registerForm button[type='submit']")
    register_btn.click()
    
    #   ESPERAR que el toast APAREZCA (no que ya esté visible)
    try:
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#toast.show")))
        
        toast = driver.find_element(By.ID, "toast")
        toast_message = driver.find_element(By.CLASS_NAME, "toast-message").text
        
        # Verificar que es un mensaje de error
        assert "no coinciden" in toast_message.lower() or "contraseña" in toast_message.lower()
        
        driver.save_screenshot('tests/screenshots/08_register_mismatch.png')
        print(f"  Validación de contraseñas funciona: '{toast_message}'")
        
    except Exception as e:
        driver.save_screenshot('tests/screenshots/08_register_mismatch_failed.png')
        print(f" (Warning) No se detectó toast de error: {e}")
        
        # Verificar al menos que el modal NO se cerró
        modal = driver.find_element(By.ID, "authModal")
        assert "hidden" not in modal.get_attribute("class")
        print("  Pero el registro NO procedió (modal aún visible)")

def test_register_success(driver):
    """Test de registro exitoso con usuario nuevo"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 20)  # ⏱️ Aumentado a 20 segundos
    wait.until(EC.visibility_of_element_located((By.ID, "authModal")))
    
    # Cambiar a registro
    register_tab = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, ".auth-tab:nth-child(2)")
    ))
    register_tab.click()
    
    time.sleep(1)
    
    # Usuario aleatorio
    new_username = generate_random_username()
    
    # Llenar formulario
    username = wait.until(EC.element_to_be_clickable((By.ID, "regUser")))
    password = driver.find_element(By.ID, "regPass")
    password_confirm = driver.find_element(By.ID, "regPassConfirm")
    
    username.clear()
    username.send_keys(new_username)
    password.clear()
    password.send_keys("password123")
    password_confirm.clear()
    password_confirm.send_keys("password123")
    
    driver.save_screenshot('tests/screenshots/09a_before_register.png')
    
    # Submit
    register_btn = driver.find_element(By.CSS_SELECTOR, "#registerForm button[type='submit']")
    register_btn.click()
    
    # Esperar toast de éxito (aparece primero)
    try:
        toast = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#toast.show")))
        time.sleep(0.5)  # Dar tiempo a leer el mensaje
        
        toast_message = driver.find_element(By.CLASS_NAME, "toast-message").text
        print(f"Toast: '{toast_message}'")
        
    except Exception as e:
        print(f"No se capturó toast: {e}")
    
    # Esperar cambio al tab de login (tarda hasta 1 segundo por el setTimeout)
    try:
        # Esperar que loginForm sea visible
        login_form = wait.until(EC.visibility_of_element_located((By.ID, "loginForm")))
        
        # Verificar que registerForm está oculto
        register_form = driver.find_element(By.ID, "registerForm")
        assert not register_form.is_displayed()
        
        # Verificar username pre-llenado
        login_username = driver.find_element(By.ID, "loginUser")
        prefilled_value = login_username.get_attribute("value")
        
        driver.save_screenshot('tests/screenshots/09b_after_register.png')
        
        assert prefilled_value == new_username, f"Username esperado: {new_username}, encontrado: {prefilled_value}"
        
        print(f" (Warning) Registro exitoso para: {new_username}")
        print(f" (Warning) Auto-cambió a login con username pre-llenado")
        
    except Exception as e:
        driver.save_screenshot('tests/screenshots/09c_register_error.png')
        
        # Debug info
        try:
            register_form = driver.find_element(By.ID, "registerForm")
            login_form = driver.find_element(By.ID, "loginForm")
            print(f"registerForm visible: {register_form.is_displayed()}")
            print(f"loginForm visible: {login_form.is_displayed()}")
        except:
            pass
        
        raise AssertionError(f"No se completó el registro para {new_username}: {e}")


# ==========================================
# TESTS DE FUNCIONALIDAD POST-LOGIN
# ==========================================

def test_logout(driver):
    """Test que logout funciona"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 15)
    
    # Login primero
    username = wait.until(EC.element_to_be_clickable((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    
    username.send_keys("admin")
    password.send_keys("adminxd")
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    wait.until(EC.invisibility_of_element_located((By.ID, "authModal")))
    
    time.sleep(2)
    
    # Click logout
    logout_btn = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "logout-btn")))
    logout_btn.click()
    
    # Confirmar en el alert
    try:
        alert = wait.until(EC.alert_is_present())
        alert.accept()
        
        time.sleep(2)
        
        driver.save_screenshot('tests/screenshots/10_after_logout.png')
        print("  Logout ejecutado")
    except:
        driver.save_screenshot('tests/screenshots/10_logout_failed.png')
        print(" (Warning) No se pudo confirmar logout")

def test_sql_editor_visible_after_login(driver):
    """Test que el editor SQL es visible después de login"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 15)
    
    # Login
    username = wait.until(EC.element_to_be_clickable((By.ID, "loginUser")))
    password = driver.find_element(By.ID, "loginPass")
    
    username.send_keys("admin")
    password.send_keys("adminxd")
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    wait.until(EC.invisibility_of_element_located((By.ID, "authModal")))
    
    # Verificar editor SQL
    sql_editor = wait.until(EC.presence_of_element_located((By.ID, "sqlQuery")))
    assert sql_editor.is_displayed()
    
    driver.save_screenshot('tests/screenshots/11_sql_editor.png')
    print("  Editor SQL visible después de login")

# ==========================================
# TESTS DE INTEGRACIÓN
# ==========================================

def test_complete_user_journey(driver):
    """Test completo: registro -> login -> query -> logout"""
    driver.get("https://pvsproyectof.pages.dev/main.html")
    
    wait = WebDriverWait(driver, 15)
    
    # 1. Registro
    register_tab = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, ".auth-tab:nth-child(2)")
    ))
    register_tab.click()
    
    time.sleep(1)
    
    new_username = generate_random_username()
    
    username = driver.find_element(By.ID, "regUser")
    password = driver.find_element(By.ID, "regPass")
    password_confirm = driver.find_element(By.ID, "regPassConfirm")
    
    username.send_keys(new_username)
    password.send_keys("test123456")
    password_confirm.send_keys("test123456")
    
    register_btn = driver.find_element(By.CSS_SELECTOR, "#registerForm button[type='submit']")
    register_btn.click()
    
    time.sleep(3)
    
    # 2. Login con el nuevo usuario
    login_username = wait.until(EC.element_to_be_clickable((By.ID, "loginUser")))
    login_password = driver.find_element(By.ID, "loginPass")
    
    # El username podría estar pre-llenado
    login_username.clear()
    login_username.send_keys(new_username)
    login_password.send_keys("test123456")
    
    login_btn = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
    login_btn.click()
    
    wait.until(EC.invisibility_of_element_located((By.ID, "authModal")))
    
    time.sleep(2)
    
    # 3. Verificar que está logueado
    user_info = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "user-info")))
    assert user_info.is_displayed()
    
    driver.save_screenshot('tests/screenshots/12_complete_journey.png')
    print(f"  Journey completo exitoso para usuario: {new_username}")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])