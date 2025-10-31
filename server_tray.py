import threading
from time import sleep
from pystray import Icon, Menu, MenuItem
from PIL import Image, ImageDraw
import subprocess
import sys
import os
import atexit
import logging


# --- Paths ---
BASE_DIR = os.path.dirname(__file__)
PID_FILE = os.path.join(BASE_DIR, "server_tray.pid")
COMMANDS_FILE = os.path.join(BASE_DIR, "server_tray_commands.txt")

# Logging setup
LOG_FILE = os.path.join(BASE_DIR, "server_tray.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# --- Tray Icon Drawing ---
def create_icon(color):
    img = Image.new("RGB", (64, 64), color)
    draw = ImageDraw.Draw(img)
    draw.ellipse((16, 16, 48, 48), fill="white")
    return img

icon_running = create_icon("green")
icon_stopped = create_icon("red")

# --- Server Process ---
server_process = None
icon = None  # Defined later in run_tray()

def start_server(_=None):
    global server_process
    if server_process and server_process.poll() is None:
        return  # Already running
    server_process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if icon:
        icon.icon = icon_running
    logging.info("Server started.")

def stop_server(_=None):
    global server_process
    if server_process and server_process.poll() is None:
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
        logging.info("Server stopped.")
    else:
        logging.info("Server was not running.")
    server_process = None
    if icon:
        icon.icon = icon_stopped

def restart_server(_=None):
    stop_server()
    sleep(1)
    start_server()

def exit_app(_=None):
    stop_server()
    remove_pid_file()
    icon.stop()

# --- PID File Handling ---
def write_pid_file():
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

def remove_pid_file():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

atexit.register(remove_pid_file)

# --- Command Listener Thread ---
def command_listener():
    while True:
        if os.path.exists(COMMANDS_FILE):
            try:
                with open(COMMANDS_FILE, "r") as f:
                    lines = f.readlines()

                # Process each command
                for line in lines:
                    cmd = line.strip()
                    if cmd == "stop-server":
                        stop_server()
                    elif cmd == "start-server":
                        start_server()
                    elif cmd == "restart-server":
                        restart_server()
                    elif cmd == "exit":
                        exit_app()
                        return

                # Truncate the file after processing
                open(COMMANDS_FILE, "w").close()  # Clears the file

            except Exception as e:
                logging.info(f"Error reading commands: {e}")
        sleep(1)


# --- Server Monitor Thread ---
def monitor_status():
    while True:
        if server_process and server_process.poll() is None:
            icon.icon = icon_running
        else:
            icon.icon = icon_stopped
        sleep(2)

# --- Tray UI ---
def run_tray():
    global icon
    menu = Menu(
        MenuItem("Start Server", start_server),
        MenuItem("Stop Server", stop_server),
        MenuItem("Restart Server", restart_server),
        MenuItem("Exit", exit_app),
    )
    icon = Icon("yt-dlp Server", icon_stopped, menu=menu)

    write_pid_file()

    # Start server on launch
    start_server()

    # Monitor server status
    threading.Thread(target=monitor_status, daemon=True).start()

    # Start listening for commands from app.pyw
    threading.Thread(target=command_listener, daemon=True).start()

    icon.run()

# --- CLI Entrypoint ---
if __name__ == "__main__":
    # Only run tray normally (ignore --stop-server etc, those are now deprecated)
    run_tray()
