from os.path import dirname, abspath
import sys
root_dir = dirname(dirname(abspath(__file__)))
print(root_dir)
sys.path.append(root_dir)
