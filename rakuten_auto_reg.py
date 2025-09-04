import shutil
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import Select
import traceback
import requests
import time
import random
import logging
import threading
from queue import Queue
import re
import os
import pyautogui
import colorama
import uuid
from datetime import datetime
import signal
import sys
import atexit
import chromedriver_autoinstaller
import psutil
import shutil
import platform
import subprocess

colorama.init()

drivers = []
successful_accounts = []
failed_accounts = []

def kill_child_processes(pid, sig=15):
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                child.send_signal(sig)
            except Exception:
                pass
        gone, alive = psutil.wait_procs(children, timeout=3)
        for p in alive:
            try:
                p.kill()
            except Exception:
                pass
    except Exception as e:
        logging.warning(f"Không thể kill process con cho PID {pid}: {e}")

def cleanup_drivers():
    """Dọn dẹp tất cả các WebDriver instances."""
    logging.warning("Đang dọn dẹp drivers...")
    global drivers
    for driver in drivers[:]:
        try:
            driver.close()
            driver.quit()
            if hasattr(driver, 'service') and driver.service.process:
                kill_child_processes(driver.service.process.pid)
        except Exception as e:
            logging.warning(f"Lỗi khi đóng driver: {e}")
        finally:
            if driver in drivers:
                drivers.remove(driver)

def signal_handler(sig, frame):
    """Xử lý SIGINT (Ctrl+C) và SIGTERM (đóng terminal)."""
    logging.info("Nhận tín hiệu dừng. Đang dọn dẹp...")
    cleanup_drivers()
    clean_all_user_data()
    logging.info("Dọn dẹp hoàn tất. Thoát...")
    sys.exit(0)

# Setup logging
COLOR_RESET = '\033[0m'
COLOR_INFO = '\033[32m'    # Green
COLOR_WARNING = '\033[33m' # Yellow
COLOR_ERROR = '\033[31m'   # Red

class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = ''
        if record.levelno == logging.INFO:
            color = COLOR_INFO
        elif record.levelno == logging.WARNING:
            color = COLOR_WARNING
        elif record.levelno == logging.ERROR:
            color = COLOR_ERROR
        msg = super().format(record)
        return f"{color}{msg}{COLOR_RESET}"
    
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        return dt.strftime("%H:%M")

# Create stream handler with color
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))

# File handler without color
file_handler = logging.FileHandler('rakuten_automation.log', encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

# Global variables
file_lock = threading.Lock()
show_browser = True

def load_input_files():
    """Tải tài khoản từ accounts.txt và proxy từ proxy.txt"""
    try:
        # Load accounts (email|password|name)
        accounts = []
        with open('accounts.txt', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 4:
                        accounts.append({
                            'email': parts[0].strip(),
                            'password': parts[1].strip(),
                            'name': parts[2].strip(),
                            'name_japanese': parts[3].strip()
                        })
        
        # Load proxies (optional)
        proxies = []
        try:
            with open('proxy.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Handle different proxy formats
                        if '@' in line:
                            # Format: host:port@username-password
                            try:
                                host_port, credentials = line.split('@', 1)
                                proxies.append({
                                    'host_port': host_port,
                                    'credentials': credentials,
                                    'full': line
                                })
                            except:
                                proxies.append({'host_port': line, 'credentials': None, 'full': line})
                        else:
                            # Standard format: host:port or host:port:user:pass
                            proxies.append({'host_port': line, 'credentials': None, 'full': line})
        except FileNotFoundError:
            logging.warning("proxy.txt không tìm thấy. Chạy mà không dùng proxy.")
        
        if not accounts:
            logging.error("Không tìm thấy tài khoản hợp lệ trong accounts.txt")
            raise ValueError("Không có tài khoản để xử lý")
        
        logging.info(f"Đã tải {len(accounts)} tài khoản và {len(proxies)} proxy")
        return accounts, proxies
    
    except Exception as e:
        logging.error(f"Lỗi khi tải file đầu vào: {repr(e)}")
        raise

def wait_for_document_loaded(driver, timeout=10):
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                state = driver.execute_script("return document.readyState")
                if state == "complete":
                    return True
            except Exception:
                pass  # Driver might not be ready yet
            time.sleep(1)
        return False
    
def init_driver(proxy=None, email=None, row=0, col=0, size=(1366, 768)):
    """Khởi tạo Chrome driver với cài đặt không bị phát hiện"""
    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-infobars')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-insecure-localhost')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-gpu')
    
    # Random user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"
    ]
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # User data directory
    user_data_dir = os.path.join(os.getcwd(), "user-data")
    if email:
        user_data_dir = os.path.join(user_data_dir, f"user-data-{email.replace('@', '_').replace('.', '_')}")
    if not os.path.exists(user_data_dir):
        os.makedirs(user_data_dir)
    options.add_argument(f'--user-data-dir={user_data_dir}')
    if (proxy and show_browser):
        options.add_argument(f'--proxy-server={proxy["host_port"]}')
    
    # Headless mode
    options.headless = (not show_browser)
    
    try:
        version_main = chromedriver_autoinstaller.get_chrome_version()
        version_main = int(version_main.split('.')[0])
    except:
        version_main = None
    
    driver = uc.Chrome(
        options=options,
        version_main=version_main,
        use_subprocess=True,
        headless=(not show_browser)
    )
    
    # Set window position and size
    if show_browser:
        width, height = size
        x = col * width
        y = row * height
        driver.set_window_rect(x=x, y=y, width=width, height=height)
    else:
        driver.set_window_size(1366, 768)
    
    # Anti-detection scripts
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.navigator.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """
    })
    
    # Proxy settings (after driver is created)
    if proxy and show_browser:
        try:
            driver.get('https://bing.com')
            time.sleep(5)
            if wait_for_document_loaded(driver, timeout=10):
                credentials = proxy.get('credentials')
                if credentials:
                    user, password = credentials.split(':')
                    pyautogui.typewrite(user)
                    pyautogui.press('tab')
                    pyautogui.typewrite(password)
                    time.sleep(1)
                    pyautogui.press('enter')
                    time.sleep(random.randint(2, 3))
        except Exception as e:
            logging.warning(f"Lỗi khi cài đặt proxy: {e}")
    
    return driver

def safe_click(driver, element):
    """Click an toàn với fallback về JavaScript"""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.3)
        ActionChains(driver).move_to_element(element).click().perform()
    except Exception as e:
        try:
            driver.execute_script("arguments[0].click();", element)
        except Exception as e2:
            logging.warning(f"Cả hai phương pháp click đều thất bại: {repr(e)}, {repr(e2)}")
            raise

def generate_random_birthdate():
    """Tạo ngày sinh ngẫu nhiên cho độ tuổi 18-60"""
    current_year = datetime.now().year
    birth_year = random.randint(current_year - 60, current_year - 18)
    birth_month = random.randint(1, 12)
    
    # Handle February and leap years
    if birth_month == 2:
        max_day = 29 if birth_year % 4 == 0 and (birth_year % 100 != 0 or birth_year % 400 == 0) else 28
    elif birth_month in [4, 6, 9, 11]:
        max_day = 30
    else:
        max_day = 31
    
    birth_day = random.randint(1, max_day)
    
    return birth_year, birth_month, birth_day

def register_rakuten_account(driver, email, password, name, name_japanese):
    """Đăng ký tài khoản Rakuten mới"""
    try:
        # Navigate to registration URL
        registration_url = "https://grp02.id.rakuten.co.jp/rms/nid/registfwdi?nonce=c7b29b109f7b0ca086676823e1667038&response_type=code&service_id=r08&x_request_id=bbf8f492-27e8-42a1-860f-f0a6b20c1e6c&client_id=c3703898-c1bd-4387-a80d-31886d35c898&locale=en-US&state=33ed431c9f1fe02dbfd78d35fcc002ab&arcLocaleEnable=true&client_id=c3703898-c1bd-4387-a80d-31886d35c898&nonce=3ae757685ed95bee17cb36892e1a79ab&redirect_uri=https%3A%2F%2Fvacation-stay.jp%2Fopenid%2Foidc_callback&response_type=code&scope=openid%20profile%20email%20phone&state=c9a0bbbf6fba02b4ea6d37a6d1fd012b"
        driver.get(registration_url)
        wait = WebDriverWait(driver, 30)
        
        logging.info(f"Bắt đầu đăng ký cho {email}")
        
        # Wait for page to load
        time.sleep(5)

        # Click <a href="javascript:setLangJa();">日本語</a>
        try:
            lang_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "日本語")))
            safe_click(driver, lang_link)
            time.sleep(10)
        except:
            pass
        
        # Try to find and fill email field with multiple selectors
        email_selectors = [
            "input[name='email']", "input[name='email2']"
        ]
        
        email_filled = False
        for selector in email_selectors:
            try:
                if selector.startswith("input["):
                    email_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                else:
                    email_input = wait.until(EC.presence_of_element_located((By.ID, selector)))
                email_input.clear()
                email_input.send_keys(email)
                time.sleep(random.uniform(0.5, 1.5))
                email_filled = True
            except:
                continue
        
        if not email_filled:
            raise Exception("Không thể tìm thấy trường nhập email")
        
        # Fill password field
        password_selectors = ["p_id", "input[type='password']"]
        password_filled = False
        
        for selector in password_selectors:
            try:
                if selector.startswith("input["):
                    password_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                else:
                    password_input = driver.find_element(By.ID, selector)
                password_input.clear()
                password_input.send_keys(password)
                time.sleep(random.uniform(0.5, 1.5))
                password_filled = True
            except:
                continue
        
        if not password_filled:
            raise Exception("Không thể tìm thấy trường nhập mật khẩu")
        
        # Fill name fields
        name_parts = name.split(' ', 1)
        first_name = name_parts[0] if len(name_parts) > 0 else name
        last_name = name_parts[1] if len(name_parts) > 1 else ""
        
        # Fill name in Japanese
        name_japanese_parts = name_japanese.split(' ', 1)
        first_name_jp = name_japanese_parts[0] if len(name_japanese_parts) > 0 else name_japanese
        last_name_jp = name_japanese_parts[1] if len(name_japanese_parts) > 1 else ""
        
        # Define field mappings
        field_mappings = [
            (first_name, ["input[name='lname']"]),
            (last_name, ["input[name='fname']"]),
            (first_name_jp, ["input[name='lname_kana']"]),
            (last_name_jp, ["input[name='fname_kana']"])
        ]
        
        # Fill all name fields
        for value, selectors in field_mappings:
            if not value:  # Skip empty values
                continue
            
            field_filled = False
            for selector in selectors:
                try:
                    field_input = driver.find_element(By.CSS_SELECTOR, selector)
                    field_input.clear()
                    field_input.send_keys(value)
                    time.sleep(random.uniform(0.5, 1.5))
                    field_filled = True
                    break
                except:
                    continue
            
            if not field_filled:
                logging.warning(f"Could not fill field with selectors: {selectors}")
        
        def submit_form(custom_selector=None):
            # Submit form - try multiple selectors
            submit_selectors =  [
                "input[type='submit']", "button[type='submit']"
            ]

            if custom_selector:
                submit_selectors = custom_selector
            
            submitted = False
            for selector in submit_selectors:
                try:
                    if selector.startswith("//"):
                        submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    else:
                        submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    safe_click(driver, submit_button)
                    submitted = True
                    break
                except:
                    continue
            
            if not submitted:
                raise Exception("Không thể tìm thấy nút gửi")
        submit_form()
        # Wait for redirect or success message
        time.sleep(10)

        submit_form(["form[name='Regist3Form'] input[type='submit']"])

        time.sleep(10)

        submit_form(["form[name='DynaForm'] input[type='submit']"])

        time.sleep(10)
        
        # Check for success indicators
        current_url = driver.current_url
        success_indicators = ["login.account.rakuten.com"]
        
        if any(indicator in current_url.lower() for indicator in success_indicators):
            logging.info(f"✅ Đăng ký thành công tài khoản: {email}")
            return True, "Đăng ký thành công"
        return True, "Đăng ký thành công"
    except Exception as e:
        logging.error(f"❌ Lỗi trong quá trình đăng ký cho {email}: {repr(e)}")
        return False, repr(e)

def process_account(driver, account, account_index):
    """Xử lý đăng ký một tài khoản"""
    email, password, name, name_japanese = account['email'], account['password'], account['name'], account['name_japanese'] 
    try:
        logging.info(f"Đang xử lý tài khoản {account_index + 1}: {email}")
        success, message = register_rakuten_account(driver, email, password, name, name_japanese)
        with file_lock:
            if success:
                successful_accounts.append(account)
                # Lưu tài khoản thành công
                with open('successful_accounts.txt', 'a', encoding='utf-8') as f:
                    f.write(f"{email}|{password}|{name}\n")
            else:
                failed_accounts.append({'account': account, 'error': message})
                # Lưu tài khoản thất bại
                with open('failed_accounts.txt', 'a', encoding='utf-8') as f:
                    f.write(f"{email}|{password}|{name}|{message}\n")
        logging.info(f"Hoàn tất xử lý tài khoản: {email}")
    except Exception as e:
        logging.error(f"Lỗi xử lý tài khoản {email}: {repr(e)}")
        with file_lock:
            failed_accounts.append({'account': account, 'error': repr(e)})
            with open('failed_accounts.txt', 'a', encoding='utf-8') as f:
                f.write(f"{email}|{password}|{name}|{repr(e)}\n")

def clean_all_user_data(retries=5, delay=1):
    """Dọn dẹp tất cả thư mục dữ liệu người dùng"""
    logging.info("Đang dọn dẹp dữ liệu người dùng...")
    user_data_dir = os.path.join(os.getcwd(), "user-data")
    if os.path.exists(user_data_dir):
        for _ in range(retries):
            try:
                shutil.rmtree(user_data_dir)
                logging.info("Đã dọn dẹp dữ liệu người dùng thành công.")
                break
            except PermissionError:
                logging.warning(f"Đang dọn dẹp dữ liệu. Thử lại sau {delay}s...")
                time.sleep(delay)
            except Exception as e:
                # logging.error(f"Lỗi không mong muốn khi dọn dẹp dữ liệu người dùng: {repr(e)}")
                time.sleep(delay)
        else:
            logging.error(f"Không thể dọn dẹp dữ liệu người dùng sau {retries} lần thử.")

def main():
    """Hàm chính"""
    global show_browser
    
    try:
        # Load input files
        accounts, proxies = load_input_files()
        
        # Clean previous user data
        clean_all_user_data()
        
        # Get number of threads
        try:
            num_threads = int(input("Nhập số luồng để chạy: "))
            if num_threads <= 0:
                logging.warning("Số luồng phải là số dương. Đặt mặc định là 1.")
                num_threads = 1
            if num_threads > len(accounts):
                logging.warning(f"Số luồng ({num_threads}) vượt quá số tài khoản ({len(accounts)}). Đặt thành {len(accounts)}.")
                num_threads = len(accounts)
        except ValueError:
            logging.warning("Đầu vào số luồng không hợp lệ. Đặt mặc định là 1.")
            num_threads = 1
        
        # Nhập lựa chọn hiển thị trình duyệt
        show = input("Bạn có muốn hiển thị cửa sổ trình duyệt không? (y/n): ").strip().lower()
        show_browser = show in ['y', 'yes']
        
        # Setup account queue
        account_queue = Queue()
        screen_width, screen_height = pyautogui.size()
        col = 4  # Number of columns for browser windows
        
        # Add accounts to queue
        for idx, account in enumerate(accounts):
            account_queue.put((account, idx))
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        driver_init_lock = threading.Lock()
        
        def worker():
            """Hàm worker thread"""
            driver = None
            while not account_queue.empty():
                try:
                    account, account_index = account_queue.get()
                    
                    with driver_init_lock:
                        proxy = proxies[account_index % len(proxies)] if len(proxies) > 0 else None
                        row = account_index // col
                        col_index = account_index % col
                        size = (screen_width // col, 400)
                        driver = init_driver(
                            proxy=proxy, 
                            email=account['email'], 
                            row=row, 
                            col=col_index, 
                            size=size
                        )
                        drivers.append(driver)
                    
                    process_account(driver, account, account_index)
                    
                except Exception as e:
                    logging.error(f"Lỗi trong worker thread: {repr(e)}")
                
                finally:
                    if driver:
                        try:
                            driver.close()
                            driver.quit()
                            if hasattr(driver, "service") and hasattr(driver.service, "process") and driver.service.process:
                                kill_child_processes(driver.service.process.pid)
                        except Exception as e:
                            logging.warning(f"Lỗi khi dọn dẹp driver: {e}")
                        finally:
                            if driver in drivers:
                                drivers.remove(driver)
                    
                    account_queue.task_done()
        
        # Start worker threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, name=f"Luồng-{i+1}")
            t.start()
            threads.append(t)
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        # Báo cáo cuối cùng và dọn dẹp
        logging.info("Đã xử lý xong tất cả tài khoản.")
        logging.info(f"✅ Đăng ký thành công: {len(successful_accounts)}")
        logging.info(f"❌ Đăng ký thất bại: {len(failed_accounts)}")
        
        clean_all_user_data()
        logging.info("Chương trình hoàn tất. Thoát sau 5 giây...")
        time.sleep(5)
        
    except Exception as e:
        logging.error(f"Lỗi trong hàm main: {repr(e)}")
    finally:
        cleanup_drivers()

if __name__ == "__main__":
    try:
        signal.signal(signal.SIGTERM, cleanup_drivers)
        signal.signal(signal.SIGINT, cleanup_drivers)
        atexit.register(cleanup_drivers)
        main()
    except KeyboardInterrupt:
        logging.info("Nhận KeyboardInterrupt. Đang dọn dẹp...")
        cleanup_drivers()
        clean_all_user_data()
        logging.info("Dọn dẹp hoàn tất. Thoát...")
        sys.exit(0)
