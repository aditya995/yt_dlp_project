import subprocess
import sys
import os
import time
import logging
from packaging import version
import psutil

# === Base Paths ===
BASE_DIR = os.path.dirname(__file__)
TRAY_SCRIPT = "server_tray.py"
PID_FILE = os.path.join(BASE_DIR, "server_tray.pid")
COMMANDS_FILE = os.path.join(BASE_DIR, "server_tray_commands.txt")
LOG_FILE = os.path.join(BASE_DIR, "server_updater.log")

# === Logging Setup ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def is_yt_dlp_installed():
    try:
        import yt_dlp
        return True
    except ImportError:
        return False

def get_installed_version():
    try:
        from importlib.metadata import version as get_version
        return get_version("yt-dlp")
    except Exception as e:
        logging.error(f"Error getting yt-dlp version from pip: {e}")
        return None

def get_latest_version():
    """Check PyPI for latest yt-dlp version."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "index", "versions", "yt-dlp"],
            capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW
        )
        for line in result.stdout.splitlines():
            if "Available versions:" in line:
                return line.split("Available versions:")[1].split(",")[0].strip()
    except Exception as e:
        logging.error(f"Error fetching latest yt-dlp version: {e}")
    return None

def update_yt_dlp():
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade", "yt-dlp"],
            capture_output=True,
            text=True,
            timeout=60,
            creationflags=subprocess.CREATE_NO_WINDOW,
            startupinfo=subprocess.STARTUPINFO()
        )
        if result.returncode == 0:
            logging.info("yt-dlp updated successfully.")
            return True
        else:
            logging.error("yt-dlp update failed:")
            logging.error(result.stderr)
    except subprocess.TimeoutExpired:
        logging.error("yt-dlp update timed out.")
    except Exception as e:
        logging.error(f"Unexpected error during yt-dlp update: {e}")
    return False

def is_process_running(pid):
    try:
        p = psutil.Process(pid)
        return p.is_running()
    except Exception:
        return False

def is_tray_running():
    if not os.path.exists(PID_FILE):
        return False
    try:
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
        return is_process_running(pid)
    except Exception:
        return False

def launch_tray_if_not_running():
    if not is_tray_running():
        subprocess.Popen(
            [sys.executable, os.path.join(BASE_DIR, TRAY_SCRIPT)],
            creationflags=subprocess.CREATE_NO_WINDOW,
            startupinfo=subprocess.STARTUPINFO()
        )
        logging.info("Tray launched.")
    else:
        logging.info("Tray already running.")

def send_command(command):
    try:
        with open(COMMANDS_FILE, "a") as f:
            f.write(command + "\n")
        logging.info(f"Sent command: {command}")
    except Exception as e:
        logging.error(f"Failed to write command '{command}': {e}")

def main_loop():
    if not is_yt_dlp_installed():
        logging.info("yt-dlp not found. Installing...")
        update_yt_dlp()

    launch_tray_if_not_running()

    while True:
        try:
            installed = get_installed_version()
            latest = get_latest_version()
            # logging.info(f"Current yt-dlp: {installed} | Latest: {latest}")

            if installed and latest:
                if version.parse(installed) < version.parse(latest):
                    logging.info(f"Updating yt-dlp from {installed} to {latest}")

                    # Step 1: Stop server before update
                    send_command("stop-server")
                    time.sleep(2)

                    # Step 2: Update yt-dlp
                    if update_yt_dlp():
                        time.sleep(1)
                        send_command("start-server")
                        logging.info("Server restarted after yt-dlp update.")
                    else:
                        logging.warning("Update failed. Restarting previous server version.")
                        send_command("start-server")
                #else:
                    #logging.info("No update needed.")
            else:
                logging.warning("Could not determine yt-dlp versions.")

            time.sleep(15)

        except KeyboardInterrupt:
            logging.info("Updater interrupted by user. Shutting down.")
            send_command("stop-server")
            send_command("exit")
            break
        except Exception as e:
            logging.error(f"Error during update check: {e}")
            time.sleep(15)

if __name__ == "__main__":
    main_loop()
