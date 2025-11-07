# tests/test_login_selenium.py

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def test_login_bd2():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # IMPORTANTE: usar Service, ya no pasar el path directo como primer arg
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # 1) Abrir tu frontend BD2
        # Si sirves este index.html desde FastAPI en "/", usa esta URL:
        driver.get("http://localhost:8000/")

        wait = WebDriverWait(driver, 10)

        # 2) Esperar a que el modal de auth esté visible
        wait.until(EC.visibility_of_element_located((By.ID, "authModal")))

        # 3) Llenar usuario y contraseña según tu HTML
        #    Tus inputs son:
        #    <input type="text" id="loginUser" ...>
        #    <input type="password" id="loginPass" ...>
        user_input = wait.until(EC.visibility_of_element_located((By.ID, "loginUser")))
        pass_input = driver.find_element(By.ID, "loginPass")

        user_input.clear()
        user_input.send_keys("admin")
        pass_input.clear()
        pass_input.send_keys("123456")

        # 4) Click en el botón "Iniciar Sesión"
        #    Botón:
        #    <button type="submit" class="btn btn-primary btn-block">Iniciar Sesión</button>
        login_button = driver.find_element(By.CSS_SELECTOR, "#loginForm button[type='submit']")
        login_button.click()

        # 5) Esperar a que el modal de login desaparezca o cambie el contenido
        wait.until(EC.invisibility_of_element_located((By.ID, "authModal")))

        # 6) Afirmar que ya estamos dentro de la app
        #    Por ejemplo, que se ve el header "SQL Query" o el texto del editor.
        page = driver.page_source
        assert "SQL Query" in page or "Escribe tu consulta SQL aquí" in page

    finally:
        driver.quit()
