import pytest
import json
import os
import tempfile
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, MagicMock

# Importar tu aplicación
from main import app, point_serializer, serialize_records_data, startup
from estructuras.point_class import Point


# Cliente de prueba
client = TestClient(app)


class TestMainEndpoints:
    """Tests para los endpoints principales de FastAPI"""
    
    def test_root_endpoint(self):
        """Test del endpoint raíz /"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "BD2" in data["message"]
        assert data["status"] == "active"
    
    @patch('main.sql_manager')
    def test_sql_endpoint_success(self, mock_sql_manager):
        """Test exitoso del endpoint /sql"""
        # Mock del resultado
        mock_sql_manager.execute_sql.return_value = [
            ("CREATE", "test_table"),
            ("INSERT", {
                'records': [{'id': 1, 'name': 'test'}],
                'inserted_ids': [1]
            })
        ]
        
        # Petición de prueba
        sql_request = {"sql": "CREATE TABLE test_table (id INT, name VARCHAR(50))"}
        response = client.post("/sql", json=sql_request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "execution_time" in data
        assert len(data["results"]) == 2
    
    @patch('main.sql_manager')
    def test_sql_endpoint_select_with_points(self, mock_sql_manager):
        """Test del endpoint /sql con datos Point"""
        # Mock del storage manager
        mock_storage = MagicMock()
        mock_storage.get.return_value = {
            'id': 1, 
            'location': Point(10.5, 20.3),
            'name': 'test_point'
        }
        
        mock_sql_manager.execute_sql.return_value = [
            ("SELECT", {
                'error': False,
                'table_name': 'locations',
                'resultado': {
                    'error': False,
                    'numeros_registro': [1],
                    'requested_attributes': ['id', 'location', 'name']
                }
            })
        ]
        mock_sql_manager.get_storage_manager.return_value = mock_storage
        
        sql_request = {"sql": "SELECT * FROM locations"}
        response = client.post("/sql", json=sql_request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verificar que el Point se serializó correctamente
        records = data["results"][0]["records"]
        assert len(records) > 0
        if "location" in records[0]:
            location = records[0]["location"]
            assert location["type"] == "POINT"
            assert location["x"] == pytest.approx(10.5)
            assert location["y"] == pytest.approx(20.3)
    
    def test_sql_endpoint_without_manager(self):
        """Test del endpoint /sql sin manager inicializado"""
        with patch('main.sql_manager', None):
            sql_request = {"sql": "SELECT * FROM test"}
            response = client.post("/sql", json=sql_request)
            assert response.status_code == 500
    
    def test_sql_endpoint_invalid_sql(self):
        """Test del endpoint /sql con SQL inválido"""
        with patch('main.sql_manager') as mock_manager:
            mock_manager.execute_sql.side_effect = Exception("SQL Error")
            
            sql_request = {"sql": "INVALID SQL SYNTAX"}
            response = client.post("/sql", json=sql_request)
            assert response.status_code == 400
            assert "Error SQL" in response.json()["detail"]
    
    @patch('main.sql_manager')
    def test_get_tables_endpoint(self, mock_sql_manager):
        """Test del endpoint /tables"""
        # Mock de tablas
        mock_sql_manager.get_all_tables.return_value = {
            'users': {
                'primary_key': 'id',
                'attributes': [
                    {'name': 'id', 'data_type': 'INT', 'is_key': True},
                    {'name': 'name', 'data_type': 'VARCHAR', 'is_key': False}
                ]
            }
        }
        
        # Mock del storage manager
        mock_storage = MagicMock()
        mock_storage._get_record_count.return_value = 10
        mock_storage._get_all_active_record_numbers.return_value = [1, 2, 3, 4, 5]
        mock_sql_manager.get_storage_manager.return_value = mock_storage
        
        response = client.get("/tables")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "users" in data["tables"]
        assert data["total_tables"] == 1
    
    def test_get_tables_without_manager(self):
        """Test del endpoint /tables sin manager"""
        with patch('main.sql_manager', None):
            response = client.get("/tables")
            assert response.status_code == 500
    
    @patch('main.sql_manager')
    def test_get_specific_table_endpoint(self, mock_sql_manager):
        """Test del endpoint /tables/{table_name}"""
        table_info = {
            'primary_key': 'id',
            'attributes': [
                {'name': 'id', 'data_type': 'INT', 'is_key': True},
                {'name': 'name', 'data_type': 'VARCHAR', 'is_key': False}
            ]
        }
        
        mock_sql_manager.get_table.return_value = table_info
        
        # Mock storage manager
        mock_storage = MagicMock()
        mock_storage.get_all_records.return_value = [
            {'id': 1, 'name': 'user1'},
            {'id': 2, 'name': 'user2'}
        ]
        mock_sql_manager.get_storage_manager.return_value = mock_storage
        
        response = client.get("/tables/users")
        
        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "users"
        assert data["total_records"] == 2
        assert len(data["sample_records"]) == 2
    
    @patch('main.sql_manager')
    def test_get_nonexistent_table(self, mock_sql_manager):
        """Test del endpoint para tabla inexistente"""
        mock_sql_manager.get_table.return_value = None
        
        response = client.get("/tables/nonexistent")
        assert response.status_code == 404
        assert "no encontrada" in response.json()["detail"]


class TestSerializationFunctions:
    """Tests para las funciones de serialización"""
    
    def test_point_serializer(self):
        """Test del serializador de Point"""
        point = Point(15.5, 25.3)
        result = point_serializer(point)
        
        assert result["type"] == "POINT"
        assert result["x"] == pytest.approx(15.5)
        assert result["y"] == pytest.approx(25.3)
        assert "string_representation" in result
    
    def test_point_serializer_with_dict_object(self):
        """Test del serializador con objeto que tiene __dict__"""
        class TestObj:
            def __init__(self):
                self.attr1 = "value1"
                self.attr2 = "value2"
        
        obj = TestObj()
        result = point_serializer(obj)
        
        assert result == {"attr1": "value1", "attr2": "value2"}
    
    def test_point_serializer_with_string(self):
        """Test del serializador con string"""
        test_str = "test string"
        result = point_serializer(test_str)
        assert result == "test string"
    
    def test_serialize_records_data(self):
        """Test de serialización de registros con Points"""
        records = [
            {
                'id': 1,
                'name': 'Location 1',
                'coordinates': Point(10.0, 20.0)
            },
            {
                'id': 2,
                'name': 'Location 2',
                'coordinates': Point(30.0, 40.0)
            }
        ]
        
        result = serialize_records_data(records)
        
        assert len(result) == 2
        assert result[0]['id'] == 1
        assert result[0]['coordinates']['type'] == 'POINT'
        assert result[0]['coordinates']['x'] == pytest.approx(10.0)
        assert result[1]['coordinates']['y'] == pytest.approx(40.0)
    
    def test_serialize_records_data_without_points(self):
        """Test de serialización sin Points"""
        records = [
            {'id': 1, 'name': 'Test 1'},
            {'id': 2, 'name': 'Test 2'}
        ]
        
        result = serialize_records_data(records)
        
        assert result == records


class TestStartupEvent:
    """Tests para el evento de startup"""
    
    @patch('main.os.makedirs')
    @patch('main.SQLTableManager')
    def test_startup_event(self, mock_sql_manager_class, mock_makedirs):
        """Test del evento de startup"""
        # Ejecutar startup manualmente
        import asyncio
        asyncio.run(startup())
        
        # Verificar que se crearon los directorios
        mock_makedirs.assert_any_call('tablas', exist_ok=True)
        mock_makedirs.assert_any_call('indices', exist_ok=True)
        
        # Verificar que se inicializó el SQL manager
        mock_sql_manager_class.assert_called_once()


@pytest.fixture
def sample_point():
    """Fixture para crear un Point de ejemplo"""
    return Point(12.5, 34.7)


@pytest.fixture
def sample_records_with_points():
    """Fixture para records con Points"""
    return [
        {
            'id': 1,
            'location': Point(10.0, 20.0),
            'name': 'Test Location'
        }
    ]


# Tests de integración usando fixtures
class TestIntegrationWithFixtures:
    """Tests de integración usando fixtures"""
    
    def test_point_in_record_serialization(self, sample_records_with_points):
        """Test de serialización usando fixture"""
        result = serialize_records_data(sample_records_with_points)
        
        assert len(result) == 1
        assert result[0]['location']['type'] == 'POINT'
        assert result[0]['location']['x'] == pytest.approx(10.0)