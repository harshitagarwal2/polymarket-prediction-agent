from __future__ import annotations

from abc import ABC, abstractmethod


class Indexer(ABC):
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
