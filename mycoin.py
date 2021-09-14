import os
import time
import subprocess
from utility.setting import system_path

subprocess.Popen(f'python {system_path}/collector/collector.py')
time.sleep(10)

os.system(f'python {system_path}/trader/window.py')
