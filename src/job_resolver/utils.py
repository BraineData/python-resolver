import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Location:
    base_name: str
    inner_path: Path

    def as_path(self) -> Path:
        return Path(os.environ[self.base_name]) / self.inner_path

    def __truediv__(self, divisor):
        return Location(self.base_name, self.inner_path / divisor)

    @classmethod
    def from_path(cls, base_name: str, path: Path) -> "Location":
        return Location(base_name, path.relative_to(os.environ[base_name]))

    @property
    def name(self):
        return self.inner_path.name
