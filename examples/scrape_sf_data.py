from webhandler.driver import Driver

with Driver() as driver:
    driver.get("https://www.example.com")
    print(driver.page_source)