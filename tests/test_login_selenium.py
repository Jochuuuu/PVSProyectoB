import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

def test_login_bd2():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    try:
        # Si tu frontend BD2 tiene login local, cambia esta URL
        driver.get("http://localhost:8000/login")

        # Cambia los selectores si tu HTML es distinto
        user_input = driver.find_element(By.NAME, "username")
        pass_input = driver.find_element(By.NAME, "password")
        login_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")

        user_input.send_keys("admin")
        pass_input.send_keys("123456")
        login_button.click()

        driver.implicitly_wait(5)
        assert "Bienvenido" in driver.page_source or "Inicio" in driver.page_source
    finally:
        driver.quit()
