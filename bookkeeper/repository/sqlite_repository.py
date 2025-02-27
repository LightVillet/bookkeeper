"""
Модуль, описывающий репозиторий для работы с SQLite
"""

import sqlite3
from inspect import get_annotations
from typing import Any, get_args
from types import UnionType
from datetime import datetime, date

from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQLiteRepository(AbstractRepository[T]):
    """
    Основной репозиторий для работы с SQLite.
    """

    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')
        self.cls = cls
        names = ', '.join(self.fields.keys())
        placeholders = ', '.join("?" * len(self.fields))
        fields_update = ", ".join([f"{field}=?" for field in self.fields.keys()])
        self.queries = {
            'foreign_keys': 'PRAGMA foreign_keys = ON',
            'add': f'INSERT INTO {self.table_name} ({names}) VALUES ({placeholders})',
            'get': f'SELECT pk, {names} FROM {self.table_name} WHERE pk = ?',
            'get_all': f'SELECT pk, {names}  FROM {self.table_name}',
            'update': f'UPDATE {self.table_name} SET {fields_update} WHERE pk = ?',
            'delete': f'DELETE FROM {self.table_name} WHERE pk = ?',
        }
        types_for_table = [f"{name} {self._resolve_type(type)}"
                           for name, type in self.fields.items()]
        create_table = f'CREATE TABLE IF NOT EXISTS {self.table_name} (' \
            + f'{", ".join(types_for_table)}, pk INTEGER PRIMARY KEY )'
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(self.queries['foreign_keys'])
            cur.execute(create_table)

    def add(self, obj: T) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'Trying to add object {obj} with filled `pk` attribute')

        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(self.queries['foreign_keys'])
            cur.execute(self.queries['add'], values)

            if cur.lastrowid is not None:
                obj.pk = cur.lastrowid

        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            row = cur.execute(self.queries['get'], [pk]).fetchone()

        if row is None:
            return None

        return self._generate_object(row)

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            base = self.queries['get_all']

            if where is not None:
                conditions = " AND ".join([f"{field} = ?" for field in where.keys()])
                rows = cur.execute(
                    base + f' WHERE {conditions}',
                    list(where.values())
                ).fetchall()
            else:
                rows = cur.execute(base).fetchall()

        return [self._generate_object(row) for row in rows]

    def update(self, obj: T) -> None:
        if getattr(obj, 'pk', None) is None:
            raise ValueError('Trying to update object without `pk` attribute')

        values = [getattr(obj, x) for x in self.fields]
        values.append(obj.pk)

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(self.queries['foreign_keys'])
            cur.execute(self.queries['update'], values)
            if cur.rowcount == 0:
                raise ValueError('Trying to update object with unknown primary key')

    def delete(self, pk: int) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(self.queries['foreign_keys'])
            cur.execute(self.queries['delete'], [pk])
            if cur.rowcount == 0:
                raise ValueError('Trying to delete object with unknown primary key')

    def _generate_object(self, values: list[Any]) -> T:
        """
        Вспомогательный метод для генерации объектов класса T
        из значений, хранящихся в базе даных.
        """
        class_arguments = {}

        for field_name, field_value in zip(self.fields.keys(), values[1:]):
            field_type = self.fields[field_name]
            if field_type == datetime:
                field_value = datetime.fromisoformat(field_value)
            if field_type == date:
                field_value = date.fromisoformat(field_value)

            class_arguments[field_name] = field_value

        obj = self.cls(**class_arguments)
        obj.pk = values[0]
        return obj  # type: ignore

    @staticmethod
    def _resolve_type(obj_type: type) -> str:
        """
            Вспомогательный метод для соответствия типов данных
            питоновских и SQLite
        """
        if issubclass(UnionType, obj_type):
            obj_type = get_args(obj_type)  # type: ignore
        if issubclass(str, obj_type):
            return 'TEXT'
        if issubclass(int, obj_type):
            return 'INTEGER'
        if issubclass(float, obj_type):
            return 'REAL'
        if issubclass(datetime, obj_type):
            return 'TIMESTAMP'
        return 'TEXT'
