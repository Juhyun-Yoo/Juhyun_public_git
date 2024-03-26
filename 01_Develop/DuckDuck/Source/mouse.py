import pystray, os, subprocess
import psutil
from pystray import MenuItem as item
from PIL import Image
import threading

caffeine = 'caffeine64.exe'

def kill_process(process_name):
    for process in psutil.process_iter(['pid', 'name']):
        if process.info['name'] == process_name:
            pid = process.info['pid']
            try:
                p = psutil.Process(pid)
                p.terminate()  # 또는 p.kill()을 사용할 수 있습니다.
            except psutil.AccessDenied as e:
                None
            break
    else:
        None
        
class IconManager:
    def __init__(self):
        self.stop_requested = False
        self.active_label = '활성화'
        self.deactive_label = '비 활성화'
        self.icon = None

    def run_caffeine(self):
        subprocess.call(caffeine + ' -noicon', shell=True)
        
    def set_tooltip(self, text):
        if self.icon:
            self.icon.update_menu()
            self.icon.title = text

    def on_activate(self, icon, item):
        self.stop_requested = False
        self.icon = icon
        threading.Thread(target=self.run_caffeine, daemon=True).start()
        self.set_tooltip('덕덕이 커피 끓이는 중')
        
    def not_activate(self, icon, item):
        kill_process(caffeine)
        self.set_tooltip('덕덕이 노는 중')
        self.stop_requested = True

    def on_exit(self, icon, item):
        kill_process(caffeine)
        self.stop_requested = True
        icon.stop()
# Get the current directory
current_directory = os.getcwd()
# Set the path of duck.ico to the current directory
image_path = current_directory+"\duck.ico"

# 시스템 트레이 아이콘 생성
image = Image.open(image_path) 
icon_manager = IconManager()
menu = (item(icon_manager.active_label, icon_manager.on_activate),item(icon_manager.deactive_label, icon_manager.not_activate), item('종료', icon_manager.on_exit))

icon = pystray.Icon("icon", image, "덕덕이", menu)

# 시스템 트레이 아이콘 표시
icon.run()
