import os
import sys

# Import parent path
cur_dir = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.abspath(cur_dir))

from slipo.exceptions import SlipoException # pylint: disable=import-error
from slipoframes.context import SlipoContext # pylint: disable=import-error
