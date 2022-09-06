import fnmatch
import os

from .LrcFile import LrcFile
from .LrcFileCollection import LrcFileCollection


class LrcDir(LrcFileCollection):
    """
    LrcDir represents a directory that contains files suitable for
    LrcFile objects
    """
    _dir_name: str

    def __init__(self, dir_name: str):
        super().__init__()
        self._dir_name = dir_name
        self._read_dir_data(dir_name)

    def _read_dir_data(self, dir_name: str):
        for _, _, files in os.walk(dir_name, onerror=self._handle_walk_error):
            for fname in files:
                if fnmatch.fnmatch(fname, "*.lrc"):
                    fname_full = os.path.join(dir_name, fname)
                    self.append_data_file(LrcFile(fname_full))

    def _handle_walk_error(self, error: OSError):
        raise Exception(
            f"Error while reading data directory '{self._dir_name}', error was:\n{error}"
        )

    @property
    def dir_name(self) -> str:
        return self._dir_name
