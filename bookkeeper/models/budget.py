"""
Модель бюджета.
"""

from dataclasses import dataclass
from enum import Enum


class Period(Enum):
    """
    Enum для задания кванта планирования бюджета
    """
    DAY = 0
    WEEK = 1
    MONTH = 2


@dataclass(slots=True)
class Budget:
    """
    Модель бюджета
    """
    amount: int
    category_id: int
    period: Period
    pk: int = 0
