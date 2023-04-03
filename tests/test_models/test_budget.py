from datetime import datetime

import pytest

from bookkeeper.repository.memory_repository import MemoryRepository
from bookkeeper.models.budget import Period, Budget

@pytest.fixture
def repo():
    return MemoryRepository()

def test_create_with_full_args_list():
    b = Budget(amount=100, category_id=1, period=Period.DAY, pk=2)
    assert b.amount      == 100
    assert b.category_id == 1
    assert b.period      == Period.DAY
    assert b.pk          == 2

def test_create_brief():
    b = Budget(100, 1, Period.DAY, 2)
    assert b.amount      == 100
    assert b.category_id == 1
    assert b.period      == Period.DAY
    assert b.pk          == 2


def test_can_add_to_repo(repo):
    b = Budget(100, 1, Period.DAY)
    pk = repo.add(b)
    assert b.pk == pk