import os, struct, json
import pytest
from pathlib import Path

# Importa desde tu paquete/proyecto
from estructuras.hash import ExtendibleHashFile, Bucket, FB, D
from estructuras.point_class import Point

# --------- Utilidades ----------
RECORD_FORMAT = "<i50sdii"  # id, name(50s), price(double), a(int), b(int)
REC_SIZE = struct.calcsize(RECORD_FORMAT)
HDR_FORMAT = "<i"
HDR_SIZE = struct.calcsize(HDR_FORMAT)

def make_dirs(tmp):
    (tmp / "tablas").mkdir(exist_ok=True)
    (tmp / "indices").mkdir(exist_ok=True)

def write_meta(tmp, table="Productos"):
    meta = {
        "table_name": table,
        "attributes": [
            {"name":"id","data_type":"INT","is_key":True,"index":"hash"},
            {"name":"name","data_type":"VARCHAR[50]","is_key":False,"index":"hash"},
            {"name":"price","data_type":"DECIMAL","is_key":False,"index":"hash"},
            {"name":"a","data_type":"INT","is_key":False,"index":"hash"},
            {"name":"b","data_type":"INT","is_key":False,"index":"hash"}
        ],
        "primary_key":"id"
    }
    with open(tmp / "tablas" / f"{table}_meta.json","w",encoding="utf-8") as f:
        json.dump(meta,f,indent=2)

def pack_record(id_, name, price, a, b):
    name_bytes = name.encode("utf-8")[:50]
    name_bytes += b"\x00"*(50-len(name_bytes))
    return struct.pack(RECORD_FORMAT, id_, name_bytes, float(price), int(a), int(b))

def write_table(tmp, rows, table="Productos"):
    bin_path = tmp / "tablas" / f"{table}.bin"
    with open(bin_path,"wb") as f:
        f.write(struct.pack(HDR_FORMAT, -1))  # header free-list
        for row in rows:
            f.write(pack_record(*row))
    return bin_path

# --------- Tests ----------
def test_bucket_pack_unpack_and_is_full(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    # Bucket básico
    b = Bucket([1,2,3], next=7)
    assert not b.is_full()
    data = b.to_bytes()
    b2 = Bucket.from_bytes(data)
    assert b2.records == [1,2,3] and b2.next == 7

    # Llenar al máximo
    full = Bucket(list(range(FB)), -1)
    assert full.is_full()

def test_load_table_metadata_ok_and_missing(tmp_path, monkeypatch):
    make_dirs(tmp_path)
    write_meta(tmp_path, "Productos")
    # Chdir al tmp para que "tablas/" e "indices/" sean relativos
    monkeypatch.chdir(tmp_path)
    h = ExtendibleHashFile(index_attr=2, table_name="Productos", is_key=True)
    assert h.table_metadata is not None  # ok

    # Tabla sin meta → debe retornar None (y no explotar)
    h2 = ExtendibleHashFile(index_attr=2, table_name="NoMeta", is_key=False)
    assert h2.table_metadata is None  # ruta de advertencia

def test_hash_bin_types(tmp_path, monkeypatch):
    make_dirs(tmp_path); write_meta(tmp_path)
    monkeypatch.chdir(tmp_path)
    h = ExtendibleHashFile(index_attr=2, table_name="Productos", is_key=False)

    # string
    assert isinstance(h.hash_bin("AA"), str)
    # int (mover index_attr a 1 para probar entero)
    h.index_attr = 1
    assert isinstance(h.hash_bin(123), str)
    # double (mover index_attr a 3)
    h.index_attr = 3
    assert isinstance(h.hash_bin(12.34), str)
    # point (simulamos meta como POINT y valor Point)
    h._get_attribute_type = lambda idx: "POINT"  # forzamos rama POINT
    assert isinstance(h.hash_bin(Point(3,4)), str)

def test_insert_search_and_overflow_split_and_duplicates(tmp_path, monkeypatch):
    make_dirs(tmp_path); write_meta(tmp_path)
    # 6 registros con el MISMO nombre para llenar bucket base y provocar overflow/split
    rows = [(i, "COLLIDE", 1.0, 0, 0) for i in range(1, 7)]
    write_table(tmp_path, rows)
    monkeypatch.chdir(tmp_path)
    h = ExtendibleHashFile(index_attr=2, table_name="Productos", is_key=False)

    # 1) inserción normal
    assert h.insert_record(1) is True

    # 2) duplicado por clave (is_key=True)
    assert h.insert_record(1) is False  # ya está

    # 3) llenar bucket hasta FB y forzar overflow/split con más inserciones
    ok = True
    for rid in range(2, 7):
        ok = h.insert_record(rid) and ok
    assert ok is True  # cubre rama bucket lleno → split/overflow

    # 4) search encuentra al menos uno
    found = h.search("COLLIDE")
    assert 1 in found

def test_range_search_returns_error(tmp_path, monkeypatch):
    make_dirs(tmp_path); write_meta(tmp_path)
    write_table(tmp_path, [(1,"A",1.0,0,0)])
    monkeypatch.chdir(tmp_path)
    h = ExtendibleHashFile(index_attr=2, table_name="Productos", is_key=False)
    resp = h.range_search("A","Z")
    assert resp["error"] and "hash no soporta búsquedas por rango" in resp["message"]

def test_delete_paths_base_and_overflow_and_not_found(tmp_path, monkeypatch):
    make_dirs(tmp_path); write_meta(tmp_path)
    # Hacemos colisión para crear overflow y luego probar delete en base y en overflow
    rows = [(1,"X",1.0,0,0),
            (2,"X",1.0,0,0),
            (3,"X",1.0,0,0),
            (4,"X",1.0,0,0),
            (5,"X",1.0,0,0),
            (6,"X",1.0,0,0)]  # 6 => overflow
    write_table(tmp_path, rows)
    monkeypatch.chdir(tmp_path)
    h = ExtendibleHashFile(index_attr=2, table_name="Productos", is_key=False)
    for rid in range(1,7):
        assert h.insert_record(rid)

    # eliminar en bucket base
    assert h.delete_record(1) == 1
    # eliminar en overflow (alguno de los restantes)
    assert h.delete_record(6) == 6
    # no encontrado
    assert h.delete_record(99) is None
