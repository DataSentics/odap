from abc import ABC, abstractmethod
from typing import Dict, Optional


class ExportInterface(ABC):
    @abstractmethod
    def export(self, config: Optional[Dict] = None):
        pass
