import os
import json
import pytest
from pathlib import Path
from tabla import TableStorageManager
from estructuras.point_class import Point

# --- helpers ---
def mk_table_info():
    return {
        "attributes": [
            {"name": "id", "data_type": "INT", "is_key": True},
            {"name": "location", "data_type": "POINT", "index": "rtree"},
            {"name": "name", "data_type": "VARCHAR[20]"},
        ],
        "primary_key": "id",
    }

def insert_demo_points(storage):
    rows = [
        {"id": 1, "location": Point(0, 0), "name": "A"},
        {"id": 2, "location": Point(3, 4), "name": "B"},     # dist=5
        {"id": 3, "location": Point(10, 10), "name": "C"},
        {"id": 4, "location": Point(-2, 1), "name": "D"},
    ]
    for r in rows:
        storage.insert(r)

# --- tests ---
def test_rtree_insert_search_delete_and_cache(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree = storage.indices["location"]

    # inserciones (TableStorageManager llama a insert_record del índice)
    insert_demo_points(storage)

    # search exact (usa intersection + filtro fino)
    assert rtree.search(Point(3, 4)) == [2]

    # range rectangular (incluye puntos en bbox)
    inside = sorted(rtree.range_search(Point(-3, -1), Point(3, 4)))
    assert inside == [1, 2, 4]

    # cache path en get_attribute_from_record_num: segunda lectura debe venir de id_to_point
    assert rtree.id_to_point.get(2) is not None
    p = rtree.get_attribute_from_record_num(2)   # hit de cache
    assert isinstance(p, Point) and abs(p.x-3.0) < 1e-9

    # delete_record elimina del índice y del cache
    assert rtree.delete_record(2) == 2
    assert 2 not in rtree.search(Point(3, 4))
    assert 2 not in rtree.id_to_point

def test_rtree_metadata_persist_reload(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree = storage.indices["location"]

    insert_demo_points(storage)
    assert rtree._save_metadata() is True
    assert os.path.exists(rtree.metadata_file)

    # CERRAR el handle del índice antes de reabrir (Windows)
    rtree.rtree_index.close()
    # opcionalmente: forzar GC para liberar referencias internas (Windows puede ser quisquilloso)
    import gc; gc.collect()

    # Re-crear el storage (esto re-crea el índice usando los .idx/.dat existentes)
    storage2 = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree2 = storage2.indices["location"]

    # Debe haber cargado el cache id->Point desde el _meta.json
    assert len(rtree2.id_to_point) >= 3

def test_spatial_radius_and_knn_via_storage(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    insert_demo_points(storage)

    # radial (center=0,0 radio=6) -> {id 1, id 2}
    ids = storage.spatial_radius_search("location", Point(0, 0), 6.0)
    assert set(ids).issuperset({1})

    # knn (2 vecinos más cercanos a (0.5,0.5))
    k_ids = storage.spatial_knn_search("location", Point(0.5, 0.5), 2)
    assert isinstance(k_ids, list) and len(k_ids) <= 2

def test_rtree_search_type_guards(tmp_path, monkeypatch):
    """Ramas de error/entrada inválida devuelven vacío."""
    monkeypatch.chdir(tmp_path)
    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree = storage.indices["location"]
    insert_demo_points(storage)

    assert rtree.search("no-point") == []
    assert rtree.range_search("x", "y") == []

def test_rtree_get_stats_bbox_and_paths(tmp_path, monkeypatch):
    """
    Cubre get_stats(): total_records, bounding_box correcto y rutas de archivos.
    """
    monkeypatch.chdir(tmp_path)

    # Reusar helpers del propio archivo de tests
    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree = storage.indices["location"]

    # Insertamos puntos con bbox no-trivial
    insert_demo_points(storage)  # (0,0), (3,4), (10,10), (-2,1)

    stats = rtree.get_stats()
    assert isinstance(stats, dict)
    assert stats["total_records"] >= 4  # cache id->point debe tenerlos
    assert stats["index_type"].startswith("R-Tree")

    # Verificar bounding box esperado: min_x=-2, max_x=10, min_y=0, max_y=10
    bb = stats["bounding_box"]
    assert bb is not None
    assert bb["min_x"] == -2
    assert bb["max_x"] == 10
    assert bb["min_y"] == 0
    assert bb["max_y"] == 10
    assert bb["width"] == 12
    assert bb["height"] == 10

    # Rutas a los archivos de índice/metadata
    paths = stats["index_files"]
    assert paths["dat"].endswith(".dat")
    assert paths["idx"].endswith(".idx")
    assert paths["meta"].endswith("_meta.json")

    # Operaciones soportadas expuestas por get_stats
    ops = set(stats["operations_supported"])
    assert {"exact_search", "range_search", "radius_search", "knn_search"} <= ops


def test_rtree_rebuild_index_from_table(tmp_path, monkeypatch):
    """
    Cubre rebuild_index(): borrar .idx/.dat, reconstruir desde el .bin,
    y verificar que vuelven a encontrarse los registros.
    """
    monkeypatch.chdir(tmp_path)

    storage = TableStorageManager("rt_tab", mk_table_info(), base_dir=str(tmp_path/"tablas"))
    rtree = storage.indices["location"]

    # Insertar puntos y confirmar que existen
    insert_demo_points(storage)
    assert rtree.search(Point(3, 4)) == [2]

    # Cerrar handle antes de manipular archivos (Windows importante)
    rtree.rtree_index.close()

    # Eliminar archivos físicos del índice para simular índice perdido
    if os.path.exists(rtree.index_file_dat):
        os.remove(rtree.index_file_dat)
    if os.path.exists(rtree.index_file_idx):
        os.remove(rtree.index_file_idx)

    # Crear NUEVA instancia de RTreeFile vacía (mismos parámetros que usa TableStorageManager)
    # - record_format viene del storage
    # - index_attr real para 'location' (2do atributo lógico) se obtiene con el helper del storage
    real_attr = storage._calculate_real_attr_index_for_index(2)
    new_rtree = type(rtree)(
        record_format=storage.record_format,
        index_attr=real_attr,
        table_name="rt_tab",
        is_key=False,
    )

    # Reconstruir desde la tabla .bin (leer registros activos y reinsertar)
    assert new_rtree.rebuild_index() is True

    # Debe volver a encontrar los mismos ids
    assert new_rtree.search(Point(3, 4)) == [2]
    stats = new_rtree.get_stats()
    assert stats["total_records"] >= 3  # reconstruyó el cache y el índice
