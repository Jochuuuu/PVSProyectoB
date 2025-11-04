# tests/test_avl_index.py
import os, struct, json, tempfile
import pytest
from tabla import TableStorageManager
from sql import SQLTableManager
from estructuras.point_class import Point

def make_dirs(base):
    os.makedirs(os.path.join(base, "tablas"), exist_ok=True)
    os.makedirs(os.path.join(base, "indices"), exist_ok=True)

def write_meta(base, table="Productos", attrs=None):
    if attrs is None:
        attrs = [
            {"name": "id", "data_type": "INT", "is_key": True},                # PK
            {"name": "name", "data_type": "VARCHAR[20]", "index": "avl"},      # AVL por string
            {"name": "price", "data_type": "DECIMAL", "index": "avl"},         # AVL por double
            {"name": "pos", "data_type": "POINT", "index": "avl"},             # AVL por POINT
        ]
    meta = {"table_name": table, "attributes": attrs, "primary_key": "id"}
    with open(os.path.join(base, "tablas", f"{table}_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta

def storage(base, meta):
    return TableStorageManager(meta["table_name"], meta, base_dir=os.path.join(base, "tablas"))

def test_insert_and_search_strings(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    meta = write_meta(tmp_path)
    monkeypatch.chdir(tmp_path)

    t = storage(tmp_path, meta)
    # Inserciones en orden que fuerce rebalanceos (L, R, LR, RL internos)
    rows = [
        {"id": 1, "name": "mango",   "price": 10.0, "pos": Point(0,0)},
        {"id": 2, "name": "banana",  "price":  5.0, "pos": Point(1,1)},
        {"id": 3, "name": "papaya",  "price": 12.0, "pos": Point(2,2)},
        {"id": 4, "name": "apple",   "price":  7.0, "pos": Point(3,3)},
        {"id": 5, "name": "coconut", "price":  9.0, "pos": Point(4,4)},
    ]
    for r in rows:
        assert t.insert(r) > 0

    avl_name = t.indices["name"]    # usa AVLFile.insert_record y rebalancea
    avl_price = t.indices["price"]

    # Búsqueda exacta por string
    r = avl_name.search("banana")
    assert r and r[0] == 2   # devuelve números de registro con ese valor

    # Duplicados permitidos si la columna NO es clave (name no es key)
    t.insert({"id": 6, "name": "banana", "price": 8.5, "pos": Point(5,5)})
    rr = avl_name.search("banana")
    assert sorted(rr) == [2, 6]     # cubre rama de duplicados en search

def test_range_search_numeric_and_string(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    meta = write_meta(tmp_path)
    monkeypatch.chdir(tmp_path)
    t = storage(tmp_path, meta)

    for i, (n, p) in enumerate([("a",3.0), ("b",7.0), ("c",11.0), ("d",13.5)], 1):
        assert t.insert({"id": i, "name": n, "price": p, "pos": Point(i,i)}) == i

    avl_price = t.indices["price"]
    # rango [5,12] debe incluir precios 7.0 y 11.0
    got = sorted(avl_price.range_search(5.0, 12.0))
    assert got == [2, 3]

    avl_name = t.indices["name"]
    # rango lexicográfico ['b','d'] → b, c, d
    got = sorted(avl_name.range_search("b", "d"))
    assert got == [2, 3, 4]

def test_point_index_search_and_range(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    meta = write_meta(tmp_path)
    monkeypatch.chdir(tmp_path)
    t = storage(tmp_path, meta)

    t.insert({"id": 1, "name": "p1", "price": 1.0, "pos": Point(0,0)})
    t.insert({"id": 2, "name": "p2", "price": 2.0, "pos": Point(5,5)})
    t.insert({"id": 3, "name": "p3", "price": 3.0, "pos": Point(10,10)})

    avl_pos = t.indices["pos"]
    # búsqueda exacta por Point (usa get_attribute_from_record_num para POINT)
    eq = avl_pos.search(Point(5,5))
    assert eq == [2]

    # rango rectangular con Points (min,max)
    got = sorted(avl_pos.range_search(Point(0,0), Point(6,6)))
    assert got == [1, 2]

def test_delete_record_updates_avl(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    meta = write_meta(tmp_path)
    monkeypatch.chdir(tmp_path)
    t = storage(tmp_path, meta)

    for i, n in enumerate(["x","y","z"], 1):
        t.insert({"id": i, "name": n, "price": float(i), "pos": Point(i,i)})

    avl_name = t.indices["name"]
    assert avl_name.search("y") == [2]

    # Borrado lógico en tabla + eliminación en índice AVL (delete_record)
    deleted = t.delete_records([2])
    assert deleted == 1
    assert avl_name.search("y") == []   # ya no aparece
