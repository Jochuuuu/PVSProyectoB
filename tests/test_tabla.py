import pytest
import os
import shutil
import tempfile
import struct
from tabla import TableStorageManager
from estructuras.point_class import Point


class TestTableStorageManagerAdditional:

    @pytest.fixture
    def temp_dir(self):
        """Fixture que crea un directorio temporal para las pruebas"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def rtree_table_info(self):
        """Fixture con tabla que tiene índice R-Tree para POINT"""
        return {
            "attributes": [
                {"name": "id", "data_type": "INT", "is_key": True},
                {"name": "location", "data_type": "POINT", "index": "rtree"},
                {"name": "name", "data_type": "VARCHAR[30]"},
            ],
            "primary_key": "id",
        }

    @pytest.fixture
    def mixed_types_table_info(self):
        """Fixture con tabla que tiene múltiples tipos de datos"""
        return {
            "attributes": [
                {"name": "id", "data_type": "INT", "is_key": True},
                {"name": "flag", "data_type": "BOOL"},
                {"name": "created_date", "data_type": "DATE"},
                {"name": "location", "data_type": "POINT"},
                {"name": "description", "data_type": "VARCHAR[100]"},
            ],
            "primary_key": "id",
        }

    def test_initialization_with_indices(self, temp_dir, rtree_table_info):
        """Test inicialización con índices incluyendo R-Tree"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Verificar que el índice R-Tree se creó
        assert "location" in storage.indices
        assert hasattr(storage.indices["location"], "range_search_radius")
        assert hasattr(storage.indices["location"], "range_search_knn_simple")

    def test_is_rtree_spatial_index(self, temp_dir, rtree_table_info):
        """Test verificación de índice R-Tree espacial"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Verificar que detecta correctamente el R-Tree espacial
        assert storage._is_rtree_spatial_index("location") == True
        assert storage._is_rtree_spatial_index("id") == False
        assert storage._is_rtree_spatial_index("nonexistent") == False

    def test_spatial_radius_search(self, temp_dir, rtree_table_info):
        """Test búsqueda radial usando R-Tree"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Insertar algunos puntos
        records = [
            {"id": 1, "location": Point(0, 0), "name": "Origin"},
            {"id": 2, "location": Point(3, 4), "name": "Point1"},
            {"id": 3, "location": Point(10, 10), "name": "Point2"},
        ]

        for record in records:
            storage.insert(record)

        # Búsqueda radial desde el origen con radio 6
        center = Point(0, 0)
        results = storage.spatial_radius_search("location", center, 6.0)

        # Debería encontrar al menos el punto origen y el punto (3,4) que está a distancia 5
        assert isinstance(results, list)

    def test_spatial_knn_search(self, temp_dir, rtree_table_info):
        """Test búsqueda K vecinos más cercanos"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Insertar algunos puntos
        records = [
            {"id": 1, "location": Point(0, 0), "name": "Origin"},
            {"id": 2, "location": Point(1, 1), "name": "Close"},
            {"id": 3, "location": Point(10, 10), "name": "Far"},
        ]

        for record in records:
            storage.insert(record)

        # Buscar los 2 vecinos más cercanos al punto (0.5, 0.5)
        center = Point(0.5, 0.5)
        results = storage.spatial_knn_search("location", center, 2)

        assert isinstance(results, list)
        assert len(results) <= 2

    def test_spatial_search_invalid_attribute(self, temp_dir, rtree_table_info):
        """Test búsqueda espacial con atributo inválido"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        with pytest.raises(ValueError, match="no tiene índice R-Tree espacial"):
            storage.spatial_radius_search("id", Point(0, 0), 5.0)

        with pytest.raises(ValueError, match="no tiene índice R-Tree espacial"):
            storage.spatial_knn_search("name", Point(0, 0), 3)

    def test_select_with_spatial_conditions(self, temp_dir, rtree_table_info):
        """Test select con condiciones espaciales"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Insertar datos
        records = [
            {"id": 1, "location": Point(0, 0), "name": "Origin"},
            {"id": 2, "location": Point(3, 4), "name": "Point1"},
        ]

        for record in records:
            storage.insert(record)

        # Test búsqueda radial
        result = storage.select(lista_espaciales=[("RADIUS", "location", Point(0, 0), 6.0)])

        assert result["error"] == False
        assert isinstance(result["numeros_registro"], list)

        # Test búsqueda KNN
        result = storage.select(lista_espaciales=[("KNN", "location", Point(0, 0), 1)])

        assert result["error"] == False
        assert isinstance(result["numeros_registro"], list)

    def test_select_with_unsupported_spatial_type(self, temp_dir, rtree_table_info):
        """Test select con tipo espacial no soportado"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        result = storage.select(lista_espaciales=[("INVALID", "location", Point(0, 0), 5.0)])

        assert result["error"] == True
        assert "errores" in result

    def test_mixed_data_types_handling(self, temp_dir, mixed_types_table_info):
        """Test manejo de múltiples tipos de datos"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        record = {
            "id": 1,
            "flag": True,
            "created_date": 1609459200,  # timestamp
            "location": Point(1.5, 2.5),
            "description": "Test record",
        }

        record_id = storage.insert(record)
        retrieved = storage.get(record_id)

        assert retrieved["id"] == 1
        assert retrieved["flag"] == True
        assert retrieved["created_date"] == 1609459200
        assert isinstance(retrieved["location"], Point)
        assert retrieved["location"].x == 1.5
        assert retrieved["location"].y == 2.5
        assert retrieved["description"] == "Test record"

    def test_pack_record_data_edge_cases(self, temp_dir, mixed_types_table_info):
        """Test empaquetado de datos con casos extremos"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        # Test con POINT como None
        record_data = {
            "id": 1,
            "flag": False,
            "created_date": 0,
            "location": None,  # Debería usar valores por defecto (0.0, 0.0)
            "description": "Test",
            "next": storage.RECORD_NORMAL,
        }

        packed_data = storage._pack_record_data(record_data)
        assert packed_data is not None
        assert len(packed_data) == storage.record_size

    def test_convert_search_value_edge_cases(self, temp_dir, rtree_table_info):
        """Test conversión de valores de búsqueda con casos extremos"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Test con valor inválido para POINT
        converted = storage._convert_search_value("location", "invalid")
        assert isinstance(converted, Point)
        assert converted.x == 0.0
        assert converted.y == 0.0

        # Test con atributo inexistente
        converted = storage._convert_search_value("nonexistent", "value")
        assert converted == "value"

    def test_record_position_calculation(self, temp_dir, mixed_types_table_info):
        """Test cálculo de posiciones de registros"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        # Test posiciones consecutivas
        pos1 = storage._get_record_position(1)
        pos2 = storage._get_record_position(2)
        pos3 = storage._get_record_position(3)

        assert pos1 == storage.header_size
        assert pos2 == pos1 + storage.record_size
        assert pos3 == pos2 + storage.record_size

    def test_file_metadata_creation(self, temp_dir, mixed_types_table_info):
        """Test creación de archivos de metadatos"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        metadata_path = storage._get_metadata_path()
        assert os.path.exists(metadata_path)

        # Verificar que contiene la información de la tabla
        import json

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        assert metadata == mixed_types_table_info

    def test_record_format_calculation(self, temp_dir, mixed_types_table_info):
        """Test cálculo del formato de registro"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        # Verificar que el formato incluye todos los tipos
        assert "i" in storage.record_format  # INT para id
        assert "?" in storage.record_format  # BOOL para flag
        assert "I" in storage.record_format  # DATE para created_date
        assert "dd" in storage.record_format  # POINT para location (dos doubles)
        assert "s" in storage.record_format  # VARCHAR para description

        # Verificar que el tamaño es correcto
        expected_size = struct.calcsize(storage.record_format)
        assert storage.record_size == expected_size

    def test_delete_already_deleted_record(self, temp_dir, mixed_types_table_info):
        """Test eliminación de registro ya eliminado"""
        storage = TableStorageManager("mixed_table", mixed_types_table_info, temp_dir)

        # Insertar y eliminar un registro
        record = {
            "id": 1,
            "flag": True,
            "created_date": 123456,
            "location": Point(1, 1),
            "description": "Test",
        }
        record_id = storage.insert(record)
        storage.delete(record_id)

        # Intentar eliminar nuevamente
        result = storage.delete(record_id)
        assert result == False

    def test_multiple_point_formats_in_insert(self, temp_dir, rtree_table_info):
        """Test inserción con diferentes formatos de Point"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Test con tupla
        record1 = {"id": 1, "location": (3.0, 4.0), "name": "Tuple"}
        id1 = storage.insert(record1)
        retrieved1 = storage.get(id1)
        assert retrieved1["location"].x == 3.0
        assert retrieved1["location"].y == 4.0

        # Test con diccionario
        record2 = {"id": 2, "location": {"x": 5.0, "y": 6.0}, "name": "Dict"}
        id2 = storage.insert(record2)
        retrieved2 = storage.get(id2)
        assert retrieved2["location"].x == 5.0
        assert retrieved2["location"].y == 6.0

        # Test con valor inválido (debería usar defaults)
        record3 = {"id": 3, "location": "invalid", "name": "Invalid"}
        id3 = storage.insert(record3)
        retrieved3 = storage.get(id3)
        assert retrieved3["location"].x == 0.0
        assert retrieved3["location"].y == 0.0

    def test_select_combined_conditions(self, temp_dir, rtree_table_info):
        """Test select con múltiples tipos de condiciones combinadas"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Insertar datos de prueba
        records = [
            {"id": 1, "location": Point(0, 0), "name": "Origin"},
            {"id": 2, "location": Point(3, 4), "name": "Point1"},
            {"id": 3, "location": Point(1, 1), "name": "Close"},
        ]

        for record in records:
            storage.insert(record)

        # Test con condiciones espaciales y búsquedas (si hubiera índices apropiados)
        # Por ahora solo test espaciales ya que 'id' podría no tener índice
        result = storage.select(lista_espaciales=[("RADIUS", "location", Point(0, 0), 2.0)])

        assert result["error"] == False
        assert isinstance(result["numeros_registro"], list)

    def test_empty_select_conditions(self, temp_dir, rtree_table_info):
        """Test select sin condiciones (retorna todos los registros)"""
        storage = TableStorageManager("rtree_table", rtree_table_info, temp_dir)

        # Insertar algunos registros
        records = [
            {"id": 1, "location": Point(0, 0), "name": "Record1"},
            {"id": 2, "location": Point(1, 1), "name": "Record2"},
        ]

        for record in records:
            storage.insert(record)

        # Select sin condiciones
        result = storage.select()

        assert result["error"] == False
        assert len(result["numeros_registro"]) == 2
        assert 1 in result["numeros_registro"]
        assert 2 in result["numeros_registro"]
