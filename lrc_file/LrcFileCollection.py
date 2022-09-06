from typing import Optional

from .LrcFile import LrcFile


class LrcFileCollection:
    _data_files: list[LrcFile]

    def __init__(self):
        self._data_files = []

    def append_data_file(self, data_file: LrcFile):
        self._data_files.append(data_file)

    def get_data_files(self, machines: Optional[set[str]] = None):
        if machines is not None:
            return list(filter(lambda x: x.machines == machines, self._data_files))
        else:
            return self._data_files

    @property
    def machines(self) -> list[set[str]]:
        return list(map(lambda x: x.machines, self._data_files))
