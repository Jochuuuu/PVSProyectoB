import pytest
import tempfile
import os
from pathlib import Path
from sql import SQLTableManager
from tabla import TableStorageManager  # Asumiendo que existe
from estructuras.point_class import Point


def test_init(tmp_path):
    """Test para verificar inicialización del SQLTableManager"""
    manager = SQLTableManager(base_dir=str(tmp_path))
    assert manager.base_dir == str(tmp_path)
    assert manager.tables == {}
    assert manager.storage_managers == {}
    assert Path(tmp_path).exists()


def test_clean_sql_statement_edge_cases():
    """Test para casos edge de limpieza SQL"""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        from sql import SQLTableManager

        manager = SQLTableManager(base_dir=tmp_dir)

        # Test 1: Comentarios múltiples en una línea
        sql = "SELECT * FROM test; -- primer comentario -- segundo comentario"
        cleaned = manager._clean_sql_statement(sql)
        print(f"Test 1 - Input: {sql}")
        print(f"Test 1 - Output: {cleaned}")
        assert "primer comentario" not in cleaned
        assert "segundo comentario" not in cleaned
        assert "SELECT * FROM test;" in cleaned

        # Test 2: Comentarios dentro de strings (NO deberían eliminarse)
        sql_with_string = "SELECT 'texto con -- guiones' FROM test;"
        cleaned_string = manager._clean_sql_statement(sql_with_string)
        print(f"Test 2 - Input: {sql_with_string}")
        print(f"Test 2 - Output: {cleaned_string}")
        assert "texto con -- guiones" in cleaned_string

        # Test 3: Comentarios multilínea
        sql_multiline = "SELECT * /* comentario */ FROM test;"
        cleaned_multiline = manager._clean_sql_statement(sql_multiline)
        print(f"Test 3 - Input: {sql_multiline}")
        print(f"Test 3 - Output: {cleaned_multiline}")
        assert "comentario" not in cleaned_multiline
        assert "SELECT * FROM test;" in cleaned_multiline

        # Test 4: String con comillas dobles
        sql_double_quotes = 'SELECT "texto con -- guiones" FROM test;'
        cleaned_double = manager._clean_sql_statement(sql_double_quotes)
        print(f"Test 4 - Input: {sql_double_quotes}")
        print(f"Test 4 - Output: {cleaned_double}")
        assert "texto con -- guiones" in cleaned_double


def test_extract_sql_operations():
    """Test para verificar extracción de operaciones SQL"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manager = SQLTableManager(base_dir=tmp_dir)

        sql = """
        CREATE TABLE test (id INT PRIMARY KEY);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """

        operations = manager._extract_sql_operations(sql)
        assert len(operations) == 3
        assert operations[0][0] == "CREATE"
        assert operations[1][0] == "INSERT"
        assert operations[2][0] == "SELECT"


def test_parse_sql_create_table():
    """Test para verificar parsing de CREATE TABLE"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manager = SQLTableManager(base_dir=tmp_dir)

        sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50], location POINT)"
        table_info = manager.parse_sql_create_table(sql)

        assert table_info is not None
        assert table_info["table_name"] == "test"
        assert table_info["primary_key"] == "id"
        assert len(table_info["attributes"]) == 3

        # Verificar atributos
        attrs = {attr["name"]: attr for attr in table_info["attributes"]}
        assert "id" in attrs
        assert attrs["id"]["data_type"] == "INT"
        assert attrs["id"]["is_key"] == True
        assert "name" in attrs
        assert attrs["name"]["data_type"] == "VARCHAR[50]"
        assert "location" in attrs
        assert attrs["location"]["data_type"] == "POINT"


# Test adicional para casos edge
def test_clean_sql_statement_edge_cases():
    """Test para casos edge de limpieza SQL"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manager = SQLTableManager(base_dir=tmp_dir)

        # Comentarios múltiples en una línea
        sql = "SELECT * FROM test; -- primer comentario -- segundo comentario"
        cleaned = manager._clean_sql_statement(sql)
        assert "primer comentario" not in cleaned
        assert "segundo comentario" not in cleaned

        # Comentarios dentro de strings (no deberían eliminarse)
        sql_with_string = "SELECT 'texto con -- guiones' FROM test;"
        cleaned_string = manager._clean_sql_statement(sql_with_string)
        assert "texto con -- guiones" in cleaned_string

        # SQL vacío
        empty_sql = "   "
        cleaned_empty = manager._clean_sql_statement(empty_sql)
        assert cleaned_empty.strip() == ""


def test_module_imports():
    import sql

    assert hasattr(sql, "SQLTableManager")


import pytest
import tempfile
import os
from sql import SQLTableManager
from tabla import TableStorageManager  # Asumiendo que existe


class TestParseSqlStatement:
    """Tests específicos para cubrir las líneas rojas en parse_sql_statement"""

    def setup_method(self):
        """Setup para cada test"""
        self.tmp_dir = tempfile.mkdtemp()
        self.manager = SQLTableManager(storage_class=TableStorageManager, base_dir=self.tmp_dir)

    def teardown_method(self):
        """Cleanup después de cada test"""
        import shutil

        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_parse_sql_statement_empty(self):
        """Test con statement vacío"""
        result = self.manager.parse_sql_statement("")
        assert result == []

    def test_parse_sql_statement_multiple_operations(self):
        """Test con múltiples operaciones SQL"""
        sql = """
        CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50]);
        INSERT INTO test VALUES (1, 'Juan');
        SELECT * FROM test;
        DELETE FROM test WHERE id = 1;
        """

        result = self.manager.parse_sql_statement(sql)

        # Debe retornar 4 operaciones
        assert len(result) == 4

        # Verificar tipos de operación
        operation_types = [op[0] for op in result]
        assert "CREATE" in operation_types
        assert "INSERT" in operation_types
        assert "SELECT" in operation_types
        assert "DELETE" in operation_types

    def test_parse_sql_create_table_success(self):
        """Test CREATE TABLE exitoso - cubre líneas rojas del bloque CREATE"""
        sql = "CREATE TABLE usuarios (id INT PRIMARY KEY, nombre VARCHAR[100]);"

        result = self.manager.parse_sql_statement(sql)

        assert len(result) == 1
        assert result[0][0] == "CREATE"
        assert result[0][1] == "usuarios"  # Nombre de tabla creada

        # Verificar que la tabla se creó en el manager
        assert "usuarios" in self.manager.tables

    def test_parse_sql_create_table_failure(self):
        """Test CREATE TABLE con formato inválido"""
        sql = "CREATE TABLE;"  # SQL inválido

        result = self.manager.parse_sql_statement(sql)

        # No debe procesar nada si el formato es inválido
        assert len(result) == 0 or (len(result) == 1 and result[0][1] is None)

    def test_parse_sql_insert_success(self):
        """Test INSERT exitoso - cubre líneas rojas del bloque INSERT"""
        # Primero crear la tabla
        create_sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50]);"
        self.manager.parse_sql_statement(create_sql)

        # Luego insertar
        insert_sql = "INSERT INTO test VALUES (1, 'Test');"
        result = self.manager.parse_sql_statement(insert_sql)

        assert len(result) == 1
        assert result[0][0] == "INSERT"
        assert result[0][1] is not None
        assert result[0][1]["table_name"] == "test"

    def test_parse_sql_insert_failure(self):
        """Test INSERT en tabla que no existe"""
        insert_sql = "INSERT INTO tabla_inexistente VALUES (1, 'Test');"
        result = self.manager.parse_sql_statement(insert_sql)

        # Puede retornar vacío o con error, dependiendo de la implementación
        if len(result) > 0:
            # Si retorna algo, debería indicar error
            assert result[0][1] is None or result[0][1].get("error", False)

    def test_parse_sql_select_success(self):
        """Test SELECT exitoso - cubre líneas rojas del bloque SELECT"""
        # Crear tabla e insertar datos
        setup_sql = """
        CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50]);
        INSERT INTO test VALUES (1, 'Test');
        """
        self.manager.parse_sql_statement(setup_sql)

        # Ejecutar SELECT
        select_sql = "SELECT * FROM test WHERE id = 1;"
        result = self.manager.parse_sql_statement(select_sql)

        assert len(result) == 1
        assert result[0][0] == "SELECT"
        assert result[0][1] is not None

    def test_parse_sql_select_failure(self):
        """Test SELECT con error"""
        select_sql = "SELECT * FROM tabla_inexistente;"
        result = self.manager.parse_sql_statement(select_sql)

        if len(result) > 0:
            # Debería indicar error
            assert result[0][1] is None or result[0][1].get("error", False)

    def test_parse_sql_delete_success(self):
        """Test DELETE exitoso - cubre líneas rojas del bloque DELETE"""
        # Crear tabla e insertar datos
        setup_sql = """
        CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50]);
        INSERT INTO test VALUES (1, 'Test');
        """
        self.manager.parse_sql_statement(setup_sql)

        # Ejecutar DELETE
        delete_sql = "DELETE FROM test WHERE id = 1;"
        result = self.manager.parse_sql_statement(delete_sql)

        assert len(result) == 1
        assert result[0][0] == "DELETE"
        assert result[0][1] is not None

    def test_parse_sql_delete_failure(self):
        """Test DELETE con error"""
        delete_sql = "DELETE FROM tabla_inexistente WHERE id = 1;"
        result = self.manager.parse_sql_statement(delete_sql)

        if len(result) > 0:
            # Debería indicar error
            assert result[0][1] is None or result[0][1].get("error", False)

    def test_parse_sql_import_csv_success(self):
        """Test IMPORT CSV exitoso - cubre líneas rojas del bloque IMPORT_CSV"""
        # Crear tabla
        create_sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR[50]);"
        self.manager.parse_sql_statement(create_sql)

        # Crear archivo CSV temporal
        csv_path = os.path.join(self.tmp_dir, "test.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,name\n1,Test\n2,Test2\n")

        # Ejecutar IMPORT CSV
        import_sql = f"IMPORT FROM CSV '{csv_path}' INTO test;"
        result = self.manager.parse_sql_statement(import_sql)

        assert len(result) == 1
        assert result[0][0] == "IMPORT_CSV"
        assert result[0][1] is not None

    def test_parse_sql_import_csv_failure(self):
        """Test IMPORT CSV con error"""
        import_sql = "IMPORT FROM CSV 'archivo_inexistente.csv' INTO tabla_inexistente;"
        result = self.manager.parse_sql_statement(import_sql)

        if len(result) > 0:
            # Debería indicar error
            assert result[0][1] is None or result[0][1].get("error", False)

    def test_parse_sql_statement_mixed_success_failure(self):
        """Test con operaciones mixtas (algunas exitosas, otras fallan)"""
        sql = """
        CREATE TABLE test (id INT PRIMARY KEY);
        INSERT INTO tabla_inexistente VALUES (1);
        INSERT INTO test VALUES (1);
        SELECT * FROM test;
        """

        result = self.manager.parse_sql_statement(sql)

        # Debe procesar todas las operaciones, algunas exitosas y otras no
        assert len(result) >= 1  # Al menos CREATE debe ser exitoso

        # Verificar que al menos una operación fue exitosa
        successful_ops = [op for op in result if op[1] is not None]
        assert len(successful_ops) >= 1

    def test_parse_sql_statement_with_comments(self):
        """Test con comentarios SQL"""
        sql = """
        -- Crear tabla de prueba
        CREATE TABLE test (id INT PRIMARY KEY);
        /* Insertar datos */
        INSERT INTO test VALUES (1);
        """

        result = self.manager.parse_sql_statement(sql)

        # Los comentarios no deben afectar el procesamiento
        assert len(result) == 2
        assert result[0][0] == "CREATE"
        assert result[1][0] == "INSERT"


class TestComparisonToRange:
    """Tests específicos para cubrir todas las líneas rojas en _comparison_to_range"""

    def setup_method(self):
        """Setup para cada test"""
        self.tmp_dir = tempfile.mkdtemp()
        self.manager = SQLTableManager(base_dir=self.tmp_dir)

        # Crear tabla de prueba con diferentes tipos de datos
        self.manager.tables["test_table"] = {
            "table_name": "test_table",
            "attributes": [
                {"name": "id", "data_type": "INT", "is_key": True, "index": "hash"},
                {"name": "price", "data_type": "DECIMAL", "is_key": False, "index": "hash"},
                {"name": "location", "data_type": "POINT", "is_key": False, "index": "hash"},
                {"name": "name", "data_type": "VARCHAR[50]", "is_key": False, "index": "hash"},
            ],
            "primary_key": "id",
        }

    def teardown_method(self):
        """Cleanup después de cada test"""
        import shutil

        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # TESTS PARA OPERADOR '>'

    def test_comparison_to_range_greater_than_int(self):
        """Test operador '>' con entero - cubre líneas rojas del primer if"""
        result = self.manager._comparison_to_range("id", ">", 5, "test_table")

        assert result is not None
        assert result[0] == "id"  # attr_name
        assert result[1] == 6  # min_val (5 + 1 epsilon para int)
        assert result[2] == 2147483647  # max_val (INT máximo)

    def test_comparison_to_range_greater_than_float(self):
        """Test operador '>' con float - cubre líneas rojas del primer if"""
        result = self.manager._comparison_to_range("price", ">", 10.5, "test_table")

        assert result is not None
        assert result[0] == "price"
        assert result[1] == 10.51  # min_val (10.5 + 0.01 epsilon para float)
        assert result[2] == 999999999.99  # max_val (DECIMAL máximo)

    def test_comparison_to_range_greater_than_point(self):
        """Test operador '>' con Point - cubre líneas rojas del primer elif isinstance Point"""
        point_val = Point(1.0, 2.0)
        result = self.manager._comparison_to_range("location", ">", point_val, "test_table")

        assert result is not None
        assert result[0] == "location"
        assert isinstance(result[1], Point)
        assert result[1].x == 1.01  # point.x + 0.01
        assert result[1].y == 2.01  # point.y + 0.01
        assert isinstance(result[2], Point)
        assert result[2].x == 999999.0  # Point máximo
        assert result[2].y == 999999.0

    # TESTS PARA OPERADOR '>='

    def test_comparison_to_range_greater_equal_int(self):
        """Test operador '>=' - cubre líneas rojas del elif operator == '>='"""
        result = self.manager._comparison_to_range("id", ">=", 5, "test_table")

        assert result is not None
        assert result[0] == "id"
        assert result[1] == 5  # min_val (valor exacto)
        assert result[2] == 2147483647  # max_val

    def test_comparison_to_range_greater_equal_point(self):
        """Test operador '>=' con Point"""
        point_val = Point(3.0, 4.0)
        result = self.manager._comparison_to_range("location", ">=", point_val, "test_table")

        assert result is not None
        assert result[0] == "location"
        assert result[1] == point_val  # min_val (valor exacto)
        assert isinstance(result[2], Point)

    # TESTS PARA OPERADOR '<'

    def test_comparison_to_range_less_than_int(self):
        """Test operador '<' con int - cubre líneas rojas del elif operator == '<'"""
        result = self.manager._comparison_to_range("id", "<", 10, "test_table")

        assert result is not None
        assert result[0] == "id"
        assert result[1] == -2147483648  # min_val (INT mínimo)
        assert result[2] == 9  # max_val (10 - 1 epsilon)

    def test_comparison_to_range_less_than_float(self):
        """Test operador '<' con float"""
        result = self.manager._comparison_to_range("price", "<", 100.0, "test_table")

        assert result is not None
        assert result[0] == "price"
        assert result[1] == -999999999.99  # min_val (DECIMAL mínimo)
        assert result[2] == 99.99  # max_val (100.0 - 0.01)

    def test_comparison_to_range_less_than_point(self):
        """Test operador '<' con Point - cubre líneas rojas del elif isinstance Point"""
        point_val = Point(5.0, 6.0)
        result = self.manager._comparison_to_range("location", "<", point_val, "test_table")

        assert result is not None
        assert result[0] == "location"
        assert isinstance(result[1], Point)
        assert result[1].x == -999999.0  # Point mínimo
        assert result[1].y == -999999.0
        assert isinstance(result[2], Point)
        assert result[2].x == 4.99  # point.x - 0.01
        assert result[2].y == 5.99  # point.y - 0.01

    # TESTS PARA OPERADOR '<='

    def test_comparison_to_range_less_equal_int(self):
        """Test operador '<=' - cubre líneas rojas del elif operator == '<='"""
        result = self.manager._comparison_to_range("id", "<=", 15, "test_table")

        assert result is not None
        assert result[0] == "id"
        assert result[1] == -2147483648  # min_val (INT mínimo)
        assert result[2] == 15  # max_val (valor exacto)

    def test_comparison_to_range_less_equal_point(self):
        """Test operador '<=' con Point"""
        point_val = Point(7.0, 8.0)
        result = self.manager._comparison_to_range("location", "<=", point_val, "test_table")

        assert result is not None
        assert result[0] == "location"
        assert isinstance(result[1], Point)
        assert result[2] == point_val  # max_val (valor exacto)

    # TEST PARA CASO DE ERROR (return None)

    def test_comparison_to_range_unsupported_operator(self):
        """Test operador no soportado - cubre línea roja final 'return None'"""
        result = self.manager._comparison_to_range("id", "!=", 5, "test_table")

        assert result is None

    def test_comparison_to_range_unsupported_type(self):
        """Test tipo de dato no soportado para comparación"""
        # Usar un tipo que no sea int, float, o Point
        result = self.manager._comparison_to_range("name", ">", "test_string", "test_table")

        # Para strings, no debería generar rangos válidos con Point
        assert result is None

    # TESTS PARA MÉTODOS HELPER

    def test_get_attribute_data_type(self):
        """Test método helper _get_attribute_data_type"""
        data_type = self.manager._get_attribute_data_type("test_table", "id")
        assert data_type == "INT"

        data_type = self.manager._get_attribute_data_type("test_table", "location")
        assert data_type == "POINT"

        data_type = self.manager._get_attribute_data_type("nonexistent_table", "id")
        assert data_type == "UNKNOWN"

        data_type = self.manager._get_attribute_data_type("test_table", "nonexistent_attr")
        assert data_type == "UNKNOWN"

    def test_get_max_min_values_for_types(self):
        """Test métodos helper para valores máximos y mínimos"""
        # Test max values
        assert self.manager._get_max_value_for_type("INT") == 2147483647
        assert self.manager._get_max_value_for_type("DECIMAL") == 999999999.99
        assert isinstance(self.manager._get_max_value_for_type("POINT"), Point)
        assert self.manager._get_max_value_for_type("VARCHAR") == "ZZZZZZZZZ"

        # Test min values
        assert self.manager._get_min_value_for_type("INT") == -2147483648
        assert self.manager._get_min_value_for_type("DECIMAL") == -999999999.99
        assert isinstance(self.manager._get_min_value_for_type("POINT"), Point)
        assert self.manager._get_min_value_for_type("VARCHAR") == ""


# Test adicional para verificar integración con parse_where_with_ranges
def test_integration_with_parse_where():
    """Test integración con el sistema de parsing WHERE"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        manager = SQLTableManager(base_dir=tmp_dir)

        # Crear tabla
        manager.tables["products"] = {
            "table_name": "products",
            "attributes": [
                {"name": "price", "data_type": "DECIMAL", "is_key": False, "index": "hash"},
                {"name": "location", "data_type": "POINT", "is_key": False, "index": "hash"},
            ],
            "primary_key": None,
        }

        # Test parsing de condiciones con operadores de comparación
        where_clause = "price > 100 AND location <= '(5.0, 10.0)'"
        busquedas, rangos = manager._parse_where_with_ranges(where_clause, "products")

        # Debe generar 2 rangos (no búsquedas exactas)
        assert len(busquedas) == 0
        assert len(rangos) == 2


if __name__ == "__main__":
    pytest.main([__file__])
