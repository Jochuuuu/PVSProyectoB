import re
from pathlib import Path
import os
import csv
import json
from estructuras.point_class import Point  # Importar la clase Point

SELECT_KEYWORD = "SELECT "
FROM_KEYWORD = "FROM"
WHERE_KEYWORD = "WHERE "
DELETE_KEYWORD = "DELETE "


class SQLTableManager:
    """
    Clase para gestionar tablas SQL, permitiendo analizar y almacenar
    definiciones de tablas a partir de declaraciones CREATE TABLE, INSERT INTO, SELECT, DELETE FROM e IMPORT FROM CSV.
    VERSIÓN ACTUALIZADA con soporte completo para tipo POINT.
    """

    def __init__(self, storage_class=None, base_dir="tablas"):
        """
        Inicializa el gestor de tablas.

        Args:
            storage_class: Clase que se usará para almacenamiento (e.g., TableStorageManager)
            base_dir: Directorio base para almacenar las tablas
        """
        self.tables = {}
        self.storage_managers = {}
        self.storage_class = storage_class
        self.base_dir = base_dir
        self.operations = []
        self.blacklisted_tables = {"auth_usuario_xa"}
        Path(base_dir).mkdir(exist_ok=True)

        self.load_existing_tables()

    def load_existing_tables(self):
        """
        Carga todas las tablas existentes desde el sistema de archivos.
        Busca archivos *_meta.json en el directorio base y recrea las estructuras.
        """
        if not os.path.exists(self.base_dir):
            return

        # Buscar archivos de metadata
        for filename in os.listdir(self.base_dir):
            if filename.endswith("_meta.json"):
                table_name = filename.replace("_meta.json", "")
                metadata_path = os.path.join(self.base_dir, filename)

                try:
                    # Cargar metadata de la tabla
                    with open(metadata_path, "r", encoding="utf-8") as f:
                        table_info = json.load(f)

                    # Verificar que existe el archivo de datos
                    data_file = os.path.join(self.base_dir, f"{table_name}.bin")
                    if not os.path.exists(data_file):
                        continue

                    self.tables[table_name] = table_info

                    if self.storage_class:
                        self.storage_managers[table_name] = self.storage_class(table_name, table_info, self.base_dir)

                except Exception:
                    continue

    def parse_sql_statement(self, sql_statement):
        """
        Analiza una instrucción SQL que puede contener múltiples declaraciones.
        Versión actualizada que incluye CREATE, INSERT, SELECT, DELETE e IMPORT FROM CSV.

        Args:
            sql_statement (str): La instrucción SQL completa.

        Returns:
            list: Lista de operaciones procesadas.
        """
        # Limpiar el statement (eliminar comentarios, etc.)
        clean_sql = self._clean_sql_statement(sql_statement)

        # Extraer todas las operaciones SQL (CREATE TABLE, INSERT INTO, SELECT, DELETE, IMPORT FROM CSV, etc.)
        operations = self._extract_sql_operations(clean_sql)

        # Procesar cada operación en el orden que aparece
        processed_operations = []
        for op_type, op_content in operations:
            result = None

            if op_type == "CREATE":
                result = self._process_create_table(op_content)
                if result:
                    processed_operations.append(("CREATE", result))

            elif op_type == "INSERT":
                result = self._process_insert(op_content)
                if result:
                    processed_operations.append(("INSERT", result))

            elif op_type == "SELECT":
                result = self._process_select(op_content)
                if result:
                    processed_operations.append(("SELECT", result))

            elif op_type == "DELETE":
                result = self._process_delete(op_content)
                if result:
                    processed_operations.append(("DELETE", result))
            elif op_type == "IMPORT_CSV":
                result = self._process_import_csv(op_content)
                if result:
                    processed_operations.append(("IMPORT_CSV", result))

        self.operations.extend(processed_operations)
        return processed_operations

    def _clean_sql_statement(self, sql_statement):
        """
        Limpia una instrucción SQL eliminando comentarios y normalizando espacios en blanco.
        VERSIÓN REFACTORIZADA con menor complejidad cognitiva.

        Args:
            sql_statement (str): La instrucción SQL original.

        Returns:
            str: La instrucción SQL limpia.
        """
        result = []
        i = 0
        length = len(sql_statement)

        while i < length:
            char = sql_statement[i]

            # Procesar strings con comillas
            if char in ["'", '"']:
                i = self._process_quoted_string(sql_statement, i, result)
                continue

            # Procesar comentarios de línea
            if self._is_line_comment_start(sql_statement, i):
                i = self._skip_line_comment(sql_statement, i, result)
                continue

            # Procesar comentarios multilínea
            if self._is_multiline_comment_start(sql_statement, i):
                i = self._skip_multiline_comment(sql_statement, i)
                continue

            # Carácter normal
            result.append(char)
            i += 1

        return self._normalize_whitespace("".join(result))

    def _process_quoted_string(self, sql_statement, start_index, result):
        """Procesa un string entre comillas y retorna el nuevo índice."""
        i = start_index
        quote_char = sql_statement[i]
        result.append(quote_char)
        i += 1

        while i < len(sql_statement):
            char = sql_statement[i]
            result.append(char)

            if char == quote_char:
                # Verificar escape con doble comilla
                if self._is_escaped_quote(sql_statement, i, quote_char):
                    i += 1
                    result.append(sql_statement[i])
                else:
                    break  # Fin del string
            i += 1

        return i + 1

    def _is_escaped_quote(self, sql_statement, index, quote_char):
        """Verifica si la comilla está escapada (doble comilla)."""
        return index + 1 < len(sql_statement) and sql_statement[index + 1] == quote_char

    def _is_line_comment_start(self, sql_statement, index):
        """Verifica si en la posición actual inicia un comentario de línea."""
        return sql_statement[index] == "-" and index + 1 < len(sql_statement) and sql_statement[index + 1] == "-"

    def _skip_line_comment(self, sql_statement, start_index, result):
        """Salta un comentario de línea y retorna el nuevo índice."""
        i = start_index
        # Encontrar el final de la línea
        while i < len(sql_statement) and sql_statement[i] != "\n":
            i += 1

        # Agregar el salto de línea si existe
        if i < len(sql_statement):
            result.append("\n")
            i += 1

        return i

    def _is_multiline_comment_start(self, sql_statement, index):
        """Verifica si en la posición actual inicia un comentario multilínea."""
        return sql_statement[index] == "/" and index + 1 < len(sql_statement) and sql_statement[index + 1] == "*"

    def _skip_multiline_comment(self, sql_statement, start_index):
        """Salta un comentario multilínea y retorna el nuevo índice."""
        i = start_index + 2  # Saltar /*

        # Buscar */
        while i < len(sql_statement) - 1:
            if sql_statement[i] == "*" and sql_statement[i + 1] == "/":
                return i + 2  # Saltar */
            i += 1

        return i

    def _normalize_whitespace(self, text):
        """Normaliza los espacios en blanco."""
        return " ".join(text.split())

    def _extract_sql_operations(self, sql_statement):
        """
        Extrae todas las operaciones SQL de un statement.
        Versión actualizada que incluye CREATE, INSERT, SELECT, DELETE e IMPORT FROM CSV.

        Args:
            sql_statement (str): La instrucción SQL limpia.

        Returns:
            list: Lista de tuplas (tipo_operación, contenido_operación).
        """
        # Dividir por punto y coma, pero teniendo cuidado con los strings que pueden contener punto y coma
        statements = []
        current = ""
        in_string = False
        string_char = None

        for char in sql_statement:
            if char in ["'", '"'] and (not in_string or string_char == char):
                in_string = not in_string
                if in_string:
                    string_char = char
                else:
                    string_char = None

            if char == ";" and not in_string:
                if current.strip():
                    statements.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            statements.append(current.strip())

        # Clasificar cada statement
        operations = []
        for stmt in statements:
            if re.match(r"^\s*CREATE\s+TABLE", stmt, re.IGNORECASE):
                operations.append(("CREATE", stmt))
            elif re.match(r"^\s*INSERT\s+INTO", stmt, re.IGNORECASE):
                operations.append(("INSERT", stmt))
            elif re.match(r"^\s*SELECT", stmt, re.IGNORECASE):
                operations.append(("SELECT", stmt))
            elif re.match(r"^\s*DELETE\s+FROM", stmt, re.IGNORECASE):
                operations.append(("DELETE", stmt))
            # ✨ NUEVO: Detectar IMPORT FROM CSV
            elif re.match(r"^\s*IMPORT\s+FROM\s+CSV", stmt, re.IGNORECASE):
                operations.append(("IMPORT_CSV", stmt))

        return operations

    def _process_create_table(self, sql_statement):
        """
        Procesa una instrucción CREATE TABLE.

        Args:
            sql_statement (str): La instrucción SQL CREATE TABLE.

        Returns:
            str: Nombre de la tabla creada, o None si hubo un error.
        """
        table_info = self.parse_sql_create_table(sql_statement)
        if table_info:
            table_name = table_info["table_name"]
            self.tables[table_name] = table_info

            # Crear un gestor de almacenamiento para esta tabla si se proporcionó la clase
            if self.storage_class:
                # Si ya existe un gestor para esta tabla, lo eliminamos primero
                if table_name in self.storage_managers:
                    return table_name

                # Crear un nuevo gestor
                self.storage_managers[table_name] = self.storage_class(table_name, table_info, self.base_dir)

            return table_name
        return None

    def _process_insert(self, sql_statement):
        """
        Procesa una instrucción INSERT INTO.

        Args:
            sql_statement (str): La instrucción SQL INSERT INTO.

        Returns:
            dict: Información del insert procesado, o None si hubo un error.
        """
        insert_info = self.parse_sql_insert(sql_statement)
        if insert_info:
            table_name = insert_info["table_name"]
            records = insert_info["records"]

            # Verificar que la tabla existe
            if table_name not in self.tables:
                return None

            # Si hay un gestor de almacenamiento para esta tabla, insertar los registros
            if table_name in self.storage_managers:
                storage_manager = self.storage_managers[table_name]
                inserted_ids = []

                for record in records:
                    try:
                        record_id = storage_manager.insert(record)
                        inserted_ids.append(record_id)
                    except Exception as e:
                        print(f"Error al insertar registro en tabla '{table_name}': {e}")

                return {"table_name": table_name, "records": records, "inserted_ids": inserted_ids}
            else:
                print(f"Advertencia: No hay un gestor de almacenamiento para la tabla '{table_name}'.")

            return {"table_name": table_name, "records": records}

        return None

    def _process_import_csv(self, sql_statement):
        """
        Procesa una instrucción IMPORT FROM CSV y la convierte en inserciones.

        Args:
            sql_statement (str): La instrucción SQL IMPORT FROM CSV.

        Returns:
            dict: Información del import procesado, similar a _process_insert.
        """
        import_info = self.parse_sql_import_csv(sql_statement)

        # Si hubo error en el parsing, retornarlo directamente
        if import_info.get("error", False):
            print(f"Error en IMPORT FROM CSV: {import_info['message']}")
            return import_info

        table_name = import_info["table_name"]
        records = import_info["records"]  # Lista de diccionarios como en INSERT

        # Verificar que la tabla existe
        if table_name not in self.tables:
            error_result = {"error": True, "message": f"La tabla '{table_name}' no existe"}
            print(f"Error: {error_result['message']}")
            return error_result

        # Si hay un gestor de almacenamiento para esta tabla, insertar los registros
        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]
            inserted_ids = []
            failed_inserts = []

            for i, record in enumerate(records, 1):
                try:
                    record_id = storage_manager.insert(record)
                    if record_id:
                        inserted_ids.append(record_id)
                    else:
                        failed_inserts.append(i)
                except Exception:
                    failed_inserts.append(i)

            success_count = len(inserted_ids)
            fail_count = len(failed_inserts)

            return {
                "error": False,
                "table_name": table_name,
                "csv_file": import_info["csv_file"],
                "records": records,
                "inserted_ids": inserted_ids,
                "failed_inserts": failed_inserts,
                "total_records": len(records),
                "successful_inserts": success_count,
                "failed_inserts_count": fail_count,
            }
        else:
            error_result = {
                "error": True,
                "message": f"No hay un gestor de almacenamiento para la tabla '{table_name}'",
            }
            print(f"Error: {error_result['message']}")
            return error_result

    def safe_parse_import_csv_statement(self, sql_statement):
        """Parsea statement IMPORT FROM CSV de manera segura"""
        sql = sql_statement.strip()
        if sql.endswith(";"):
            sql = sql[:-1].strip()

        sql_upper = sql.upper()

        # Verificar que empiece con IMPORT FROM CSV
        if not sql_upper.startswith("IMPORT FROM CSV"):
            return None, None, None

        # Buscar la ruta del archivo (entre comillas simples)
        after_csv = sql[15:].strip()

        if not after_csv.startswith("'"):
            return None, None, None

        # Encontrar la comilla de cierre
        quote_end = after_csv.find("'", 1)
        if quote_end == -1:
            return None, None, None

        csv_file_path = after_csv[1:quote_end]

        # Buscar "INTO"
        remaining = after_csv[quote_end + 1 :].strip()
        if not remaining.upper().startswith("INTO"):
            return None, None, None

        # Extraer nombre de tabla
        after_into = remaining[4:].strip()
        table_name = ""
        i = 0
        while i < len(after_into) and (after_into[i].isalnum() or after_into[i] == "_"):
            table_name += after_into[i]
            i += 1

        if not table_name:
            return None, None, None

        # Buscar opciones WITH (opcional)
        remaining_after_table = after_into[len(table_name) :].strip()
        options_str = None

        if remaining_after_table.upper().startswith("WITH"):
            options_str = remaining_after_table[4:].strip()

        return csv_file_path, table_name, options_str

    def parse_sql_import_csv(self, sql_statement):
        """
        Analiza una instrucción SQL IMPORT FROM CSV y lee el archivo CSV.
        Convierte el CSV en una lista de records (diccionarios) igual que parse_sql_insert.

        Formato soportado:
        IMPORT FROM CSV 'ruta/archivo.csv' INTO tabla_name;
        IMPORT FROM CSV 'ruta/archivo.csv' INTO tabla_name WITH DELIMITER ';';

        Args:
            sql_statement (str): La instrucción SQL IMPORT FROM CSV.

        Returns:
            dict: Información extraída con records en formato lista de diccionarios
        """

        # Patrón para IMPORT FROM CSV
        csv_file_path, table_name, options_str = self.safe_parse_import_csv_statement(sql_statement)
        if not table_name:
            return {
                "error": True,
                "message": "Formato de IMPORT FROM CSV no válido. Use: IMPORT FROM CSV 'archivo.csv' INTO tabla;",
            }

        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {"error": True, "message": f"La tabla '{table_name}' no existe"}

        # Verificar que el archivo existe
        if not os.path.exists(csv_file_path):
            return {"error": True, "message": f"El archivo CSV '{csv_file_path}' no existe"}

        # Parsear opciones si existen
        delimiter = ","
        encoding = "utf-8"
        skip_header = True

        if options_str:
            if "DELIMITER" in options_str.upper():
                delimiter_match = re.search(r"DELIMITER\s*['\"]([^'\"]+)['\"]", options_str, re.IGNORECASE)
                if delimiter_match:
                    delimiter = delimiter_match.group(1)

            if "ENCODING" in options_str.upper():
                encoding_match = re.search(r"ENCODING\s*['\"]([^'\"]+)['\"]", options_str, re.IGNORECASE)
                if encoding_match:
                    encoding = encoding_match.group(1)

            if "NO_HEADER" in options_str.upper():
                skip_header = False

        table_info = self.tables[table_name]

        try:
            # Leer el archivo CSV
            with open(csv_file_path, "r", encoding=encoding, newline="") as csvfile:
                # Auto-detectar dialecto si es necesario
                sample = csvfile.read(1024)
                csvfile.seek(0)

                try:
                    sniffer = csv.Sniffer()
                    dialect = sniffer.sniff(sample, delimiters=delimiter + ";\t|")
                    if hasattr(dialect, "delimiter"):
                        delimiter = dialect.delimiter
                except Exception:
                    pass  # Usar delimiter especificado

                reader = csv.reader(csvfile, delimiter=delimiter)
                rows = list(reader)

                if not rows:
                    return {"error": True, "message": "El archivo CSV está vacío"}

                # Procesar headers
                headers = None
                data_rows = rows

                if skip_header and len(rows) > 0:
                    headers = [col.strip() for col in rows[0]]
                    data_rows = rows[1:]
                else:
                    headers = [attr["name"] for attr in table_info["attributes"]]

                # Crear mapeo de columnas CSV a atributos de tabla
                column_mapping = self._create_csv_column_mapping(table_info, headers)

                if not column_mapping:
                    return {
                        "error": True,
                        "message": "No se pudo mapear las columnas del CSV a la tabla",
                    }

                # Convertir filas CSV a records (lista de diccionarios)
                records = []
                primary_key_attr = table_info.get("primary_key")

                for row_num, row in enumerate(data_rows, 1):
                    try:
                        record = {}

                        # Inicializar todos los atributos con valores por defecto
                        for attr in table_info["attributes"]:
                            attr_name = attr["name"]
                            default_value = self._get_default_value_for_type(attr["data_type"])
                            record[attr_name] = default_value

                        # Mapear cada columna del CSV que tengamos
                        for csv_col_index, table_attr in column_mapping.items():
                            if csv_col_index < len(row):
                                value_str = row[csv_col_index].strip() if row[csv_col_index] else None

                                # Si el valor no está vacío, convertirlo
                                if value_str and value_str.lower() not in [
                                    "",
                                    "null",
                                    "none",
                                    "n/a",
                                    "na",
                                ]:
                                    converted_value = self._convert_csv_value(value_str, table_attr, table_info)
                                    if converted_value is not None:
                                        record[table_attr] = converted_value
                                # Si está vacío, ya tiene el valor por defecto

                        # Validar que el primary key no esté vacío (debe ser un valor real, no el default)
                        if primary_key_attr:
                            pk_value = record.get(primary_key_attr)
                            default_pk = self._get_default_value_for_type(
                                next(attr["data_type"] for attr in table_info["attributes"] if attr["name"] == primary_key_attr)
                            )

                            # Si el PK sigue siendo el valor por defecto, verificar si vino del CSV
                            pk_col_in_csv = False
                            for csv_col_index, table_attr in column_mapping.items():
                                if table_attr == primary_key_attr and csv_col_index < len(row):
                                    csv_value = row[csv_col_index].strip() if row[csv_col_index] else None
                                    if csv_value and csv_value.lower() not in [
                                        "",
                                        "null",
                                        "none",
                                        "n/a",
                                        "na",
                                    ]:
                                        pk_col_in_csv = True
                                    break

                            if not pk_col_in_csv or pk_value == default_pk:
                                continue

                        records.append(record)

                    except Exception as e:
                        continue

                if not records:
                    return {
                        "error": True,
                        "message": "No se pudieron convertir registros válidos del CSV",
                    }

                return {
                    "error": False,
                    "table_name": table_name,
                    "csv_file": csv_file_path,
                    "records": records,  # Lista de diccionarios igual que parse_sql_insert
                    "total_rows_in_csv": len(data_rows),
                    "valid_records": len(records),
                }

        except Exception as e:
            return {"error": True, "message": f"Error al procesar archivo CSV: {str(e)}"}

    def _create_csv_column_mapping(self, table_info, csv_headers):
        """
        Crea mapeo automático entre headers CSV y atributos de tabla.

        Returns:
            dict: {indice_csv: nombre_atributo_tabla}
        """
        mapping = {}
        table_attributes = {attr["name"].lower(): attr["name"] for attr in table_info["attributes"]}

        for i, header in enumerate(csv_headers):
            header_clean = header.lower().strip()

            # Coincidencia exacta
            if header_clean in table_attributes:
                mapping[i] = table_attributes[header_clean]
                continue

            # Coincidencia parcial
            for table_attr_lower, table_attr_original in table_attributes.items():
                if (
                    header_clean in table_attr_lower
                    or table_attr_lower in header_clean
                    or header_clean.replace("_", "").replace(" ", "") == table_attr_lower.replace("_", "")
                ):
                    mapping[i] = table_attr_original
                    break

        return mapping

    def _convert_csv_value(self, value_str, attr_name, table_info):
        """
        Convierte valor CSV al tipo correcto según la tabla.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        # Buscar tipo de dato del atributo
        data_type = None
        for attr in table_info["attributes"]:
            if attr["name"] == attr_name:
                data_type = attr["data_type"].upper()
                break

        if not data_type:
            return value_str

        try:
            if data_type == "POINT":
                # Intentar convertir string a Point
                # Formatos soportados: "(1.5, 2.3)", "1.5, 2.3", "1.5;2.3", etc.
                return Point.from_string(value_str)
            elif "INT" in data_type:
                return int(float(value_str))  # float primero por si hay decimales
            elif "DECIMAL" in data_type or "FLOAT" in data_type:
                return float(value_str)
            elif "BOOL" in data_type:
                return value_str.lower() in ("true", "yes", "1", "t", "y", "sí")
            else:
                return str(value_str)
        except (ValueError, TypeError):
            return self._get_default_value_for_type(data_type)

    def _get_default_value_for_type(self, data_type):
        """
        Obtiene el valor por defecto para un tipo de dato cuando el campo está vacío.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            data_type (str): Tipo de dato del atributo

        Returns:
            Valor por defecto según el tipo
        """
        data_type_upper = data_type.upper()

        if data_type_upper == "POINT":
            return Point(0.0, 0.0)  # Punto origen como valor por defecto
        elif "INT" in data_type_upper:
            return 0
        elif "DECIMAL" in data_type_upper or "FLOAT" in data_type_upper or "DOUBLE" in data_type_upper:
            return 0.0
        elif "BOOL" in data_type_upper:
            return False
        elif "DATE" in data_type_upper:
            return 0  # Timestamp 0 = 1970-01-01
        else:
            # Para VARCHAR, CHAR, etc.
            return " "  # Un espacio como solicitado

    def safe_parse_insert_statement(self, sql_statement):
        """Parsea statement INSERT de manera segura"""
        sql_upper = sql_statement.upper()

        # Buscar "INSERT INTO"
        insert_pos = sql_upper.find("INSERT INTO")
        if insert_pos == -1:
            return None, None, None

        # Buscar nombre de tabla después de "INSERT INTO"
        after_insert = sql_statement[insert_pos + 11 :].strip()

        # Encontrar el nombre de la tabla (primera palabra)
        table_name = ""
        i = 0
        while i < len(after_insert) and after_insert[i].isalnum() or after_insert[i] == "_":
            table_name += after_insert[i]
            i += 1

        if not table_name:
            return None, None, None

        # Buscar paréntesis de columnas (opcional)
        remaining = after_insert[len(table_name) :].strip()
        columns_str = None

        if remaining.startswith("("):
            # Extraer columnas entre paréntesis
            paren_end = remaining.find(")")
            if paren_end != -1:
                columns_str = remaining[1:paren_end].strip()
                remaining = remaining[paren_end + 1 :].strip()

        # Buscar "VALUES"
        values_pos = remaining.upper().find("VALUES")
        if values_pos == -1:
            return None, None, None

        values_part = remaining[values_pos + 6 :].strip()

        return table_name, columns_str, values_part

    def safe_extract_insert_values(self, values_part):
        """Extrae valores de INSERT de manera segura con baja complejidad cognitiva"""
        value_sets = []
        i = 0

        while i < len(values_part):  # +1 (while)
            i = self._skip_to_opening_paren(values_part, i)

            if i >= len(values_part):  # +1 (if) +1 (nested)
                break

            content = self._extract_paren_content(values_part, i)
            if content is not None:  # +1 (if) +1 (nested)
                value_sets.append(content)

            i = self._advance_past_closing_paren(values_part, i)

        return value_sets

    def _skip_to_opening_paren(self, text, start_pos):
        """Busca el siguiente paréntesis de apertura"""
        while start_pos < len(text) and text[start_pos] != "(":  # +1 (while)
            start_pos += 1
        return start_pos

    def _extract_paren_content(self, text, start_pos):
        """Extrae contenido entre paréntesis balanceados"""
        # Retorno temprano para casos edge
        if start_pos >= len(text) or text[start_pos] != "(":  # +1 (if)
            return None

        paren_count = 0
        current_pos = start_pos

        while current_pos < len(text):  # +1 (while)
            char = text[current_pos]

            if char == "(":  # +1 (if) +1 (nested)
                paren_count += 1
            elif char == ")":  # +1 (elif) +1 (nested)
                paren_count -= 1
                if paren_count == 0:  # +1 (if) +2 (nested)
                    return text[start_pos + 1 : current_pos]

            current_pos += 1

        return None  # Paréntesis no balanceados

    def _advance_past_closing_paren(self, text, start_pos):
        """Avanza hasta después del paréntesis de cierre"""
        # Retorno temprano
        if start_pos >= len(text):  # +1 (if)
            return start_pos

        paren_count = 0
        current_pos = start_pos

        while current_pos < len(text):  # +1 (while)
            if text[current_pos] == "(":  # +1 (if) +1 (nested)
                paren_count += 1
            elif text[current_pos] == ")":  # +1 (elif) +1 (nested)
                paren_count -= 1
                if paren_count == 0:  # +1 (if) +2 (nested)
                    return current_pos + 1
            current_pos += 1

        return current_pos

    def parse_sql_create_table(self, sql_statement):
        """
        Analiza una instrucción SQL CREATE TABLE para extraer información sobre la tabla,
        sus atributos y tipos de datos, incluyendo información sobre claves primarias e índices.

        Args:
            sql_statement (str): La instrucción SQL CREATE TABLE.

        Returns:
            dict: Un diccionario que contiene la información extraída, o None si no es válida.
        """
        # Expresión regular para encontrar el nombre de la tabla
        table_name_pattern = re.compile(r"CREATE\s+TABLE\s+([A-Za-z_\d]+)\s*\(", re.IGNORECASE)

        # Expresión regular mejorada para captuxrar atributos con sus tipos, claves e índices
        attribute_pattern = re.compile(
            r"\s*(\w+)\s+([A-Za-z_\d\[\]]+)"  # Nombre y tipo de dato (como VARCHAR[50] o POINT)
            r"(?:\s+(PRIMARY\s+KEY|KEY))?"  # PRIMARY KEY o KEY
            r"(?:\s+INDEX\s+(\w+))?"  # INDEX con tipo (como BTree)
            r"(?:\s+SEQ)?"  # SEQ opcional
            r"\s*(?:,|$)",  # Coma final o fin de línea
            re.IGNORECASE,
        )

        # Buscar el nombre de la tabla
        table_name_match = table_name_pattern.search(sql_statement)
        if not table_name_match:
            return None

        table_name = table_name_match.group(1)

        # Extraer el contenido entre paréntesis
        content = self.safe_extract_parentheses_content(sql_statement)

        if content is None:
            return None

        # Buscar los atributos
        attributes = []
        primary_key = None

        attribute_matches = attribute_pattern.finditer(content)
        for match in attribute_matches:
            attribute_name = match.group(1)
            attribute_data_type = match.group(2)
            is_key = match.group(3) if match.group(3) else None
            index_type = match.group(4).lower() if match.group(4) else "hash"  # Por defecto: hash

            attribute = {
                "name": attribute_name,
                "data_type": attribute_data_type,
                "is_key": False,
                "index": index_type,
            }

            # Verificar si es clave primaria
            if is_key and is_key.upper() in ["PRIMARY KEY", "KEY"]:
                attribute["is_key"] = True
                primary_key = attribute_name

            attributes.append(attribute)

        # Crear el diccionario con la información extraída
        table_info = {
            "table_name": table_name,
            "attributes": attributes,
            "primary_key": primary_key,
        }

        return table_info

    def parse_sql_insert(self, sql_statement):
        """
        Analiza una instrucción SQL INSERT INTO para extraer el nombre de la tabla
        y los valores a insertar.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            sql_statement (str): La instrucción SQL INSERT INTO.

        Returns:
            dict: Un diccionario con la información extraída, o None si no es válida.
        """
        # Patrones para analizar la sentencia INSERT
        # Parsear de manera segura
        table_name, columns_str, values_part = self.safe_parse_insert_statement(sql_statement)
        if not table_name:
            return None

        # Si las columnas no están especificadas, usar todas las columnas de la tabla
        columns = []
        if columns_str:
            columns = [col.strip() for col in columns_str.split(",")]
        else:
            # Si no se especificaron columnas, usar las de la definición de la tabla
            if table_name in self.tables:
                columns = [attr["name"] for attr in self.tables[table_name]["attributes"]]

        # Buscar los valores a insertar
        value_sets_str = self.safe_extract_insert_values(values_part)
        if not value_sets_str:
            return None

        # Extraer todos los conjuntos de valores

        # Primero, extraer todo lo que sigue a VALUES

        # Función auxiliar para dividir por comas teniendo en cuenta paréntesis y strings
        def split_values(values_str):
            result = []
            current = ""
            depth = 0
            in_string = False
            string_char = None

            for char in values_str:
                if char in ["'", '"'] and (not in_string or string_char == char):
                    in_string = not in_string
                    if in_string:
                        string_char = char
                    else:
                        string_char = None

                if char == "(" and not in_string:
                    depth += 1
                    if depth == 1:  # Inicio de un conjunto de valores
                        current = ""
                        continue
                elif char == ")" and not in_string:
                    depth -= 1
                    if depth == 0:  # Fin de un conjunto de valores
                        result.append(current.strip())
                        continue

                if depth > 0:
                    current += char

            return result

        # Extraer todos los conjuntos de valores
        value_sets_str = split_values(values_part)

        records = []
        for value_set_str in value_sets_str:
            # Analizar cada valor del conjunto
            values = self._parse_values(value_set_str)

            # Asociar cada valor con su columna correspondiente
            record = {}
            for i, value in enumerate(values):
                if i < len(columns):
                    column_name = columns[i]
                    # Convertir el valor según el tipo de dato de la columna
                    record[column_name] = self._convert_value(value, table_name, column_name)

            records.append(record)

        return {"table_name": table_name, "columns": columns, "records": records}

    def clean_leading_trailing_ands(self, clause):
        """Limpia ANDs al inicio y final de manera segura"""
        clause = clause.strip()

        # Remover AND al inicio
        if clause.upper().startswith("AND "):
            clause = clause[4:].strip()

        # Remover AND al final
        if clause.upper().endswith(" AND"):
            clause = clause[:-4].strip()

        return clause

    def clean_duplicate_ands(self, clause):
        """Limpia ANDs duplicados de manera segura sin regex vulnerable"""
        # Dividir por espacios y reconstruir
        words = clause.split()
        result = []
        prev_was_and = False

        for word in words:
            word_upper = word.upper()
            if word_upper == "AND":
                if not prev_was_and:
                    result.append(word)
                    prev_was_and = True
            else:
                result.append(word)
                prev_was_and = False

        return " ".join(result)

    def safe_split_and_conditions(self, clause):
        """Divide condiciones por AND de manera segura"""
        if not clause:
            return []

        # Convertir a mayúsculas para búsqueda
        clause_upper = clause.upper()
        parts = []
        current_part = ""
        i = 0

        while i < len(clause):
            # Buscar " AND " (con espacios)
            if (
                i <= len(clause) - 5
                and clause_upper[i : i + 5] == " AND "
                and (i == 0 or clause[i - 1] == " ")
                and (i + 5 >= len(clause) or clause[i + 5] == " ")
            ):

                # Encontramos un AND, guardar parte actual
                if current_part.strip():
                    parts.append(current_part.strip())
                current_part = ""
                i += 5  # Saltar " AND "
            else:
                current_part += clause[i]
                i += 1

        # Agregar última parte
        if current_part.strip():
            parts.append(current_part.strip())

        return parts

    def safe_extract_parentheses_content(self, sql_statement):
        """Extrae contenido entre paréntesis de manera segura"""
        # Buscar el primer paréntesis de apertura
        start_idx = sql_statement.find("(")
        if start_idx == -1:
            return None

        # Encontrar el paréntesis de cierre balanceado
        paren_count = 0
        end_idx = start_idx

        for i in range(start_idx, len(sql_statement)):
            if sql_statement[i] == "(":
                paren_count += 1
            elif sql_statement[i] == ")":
                paren_count -= 1
                if paren_count == 0:
                    end_idx = i
                    break

        if paren_count != 0:
            return None  # Paréntesis no balanceados

        # Extraer y limpiar el contenido
        content = sql_statement[start_idx + 1 : end_idx].strip()
        return content

    def _parse_where_with_ranges(self, where_clause, table_name):
        """
        Analiza una cláusula WHERE y la separa en búsquedas exactas y por rango.
        VERSIÓN ACTUALIZADA que soporta operadores de comparación: >, <, >=, <=
        Y maneja tipo POINT correctamente.

        Soporta:
        - attr=value
        - attr BETWEEN min_val AND max_val
        - attr > value (convertido a rango)
        - attr < value (convertido a rango)
        - attr >= value (convertido a rango)
        - attr <= value (convertido a rango)
        - Combinaciones con AND

        Args:
            where_clause (str): La cláusula WHERE sin la palabra WHERE.
            table_name (str): Nombre de la tabla para conversión de tipos.

        Returns:
            tuple: (lista_busquedas, lista_rangos)
        """
        lista_busquedas = []
        lista_rangos = []

        print(f"Analizando WHERE clause: '{where_clause}'")

        # 1. Primero extraer condiciones BETWEEN
        #between_pattern = r"(\w+)\s+BETWEEN\s+([^\s]+(?:\s+[^\s]+)*?)\s+AND\s+([^\s]+(?:\s+[^\s]+)*?
        between_pattern = r"(\w+)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)"
        between_matches = list(re.finditer(between_pattern, where_clause, re.IGNORECASE))

        remaining_clause = where_clause

        # Procesar condiciones BETWEEN
        for match in reversed(between_matches):
            attr_name = match.group(1)
            min_val_str = match.group(2).strip()
            max_val_str = match.group(3).strip()

            min_val = self._convert_value(min_val_str, table_name, attr_name)
            max_val = self._convert_value(max_val_str, table_name, attr_name)

            lista_rangos.append([attr_name, min_val, max_val])
            print(f"BETWEEN encontrado: {attr_name} BETWEEN {min_val} AND {max_val}")

            # Remover del remaining_clause
            remaining_clause = remaining_clause[: match.start()] + remaining_clause[match.end() :]

        # 2. Extraer operadores de comparación (>=, <=, >, <)
        comparison_patterns = [
            (r"(\w+)\s*>=\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)", ">="),
            (r"(\w+)\s*<=\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)", "<="),
            (r"(\w+)\s*>\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)", ">"),
            (r"(\w+)\s*<\s*([^\s]+(?:\s+[^\s]+)*?)(?:\s+AND\s+|$)", "<"),
        ]

        for pattern, operator in comparison_patterns:
            matches = list(re.finditer(pattern, remaining_clause, re.IGNORECASE))

            for match in reversed(matches):
                attr_name = match.group(1)
                value_str = match.group(2).strip()
                value = self._convert_value(value_str, table_name, attr_name)

                # Convertir operador de comparación a rango
                range_result = self._comparison_to_range(attr_name, operator, value, table_name)
                if range_result:
                    lista_rangos.append(range_result)
                    print(f"Comparación encontrada: {attr_name} {operator} {value} → rango")

                # Remover del remaining_clause
                remaining_clause = remaining_clause[: match.start()] + remaining_clause[match.end() :]

        # 3. Limpiar remaining_clause
        remaining_clause = self.clean_duplicate_ands(remaining_clause)
        remaining_clause = self.clean_leading_trailing_ands(remaining_clause)
        remaining_clause = remaining_clause.strip()

        # 4. Procesar condiciones de igualdad restantes
        if remaining_clause:
            and_parts = self.safe_split_and_conditions(remaining_clause)

            for part in and_parts:
                part = part.strip()
                if not part:
                    continue

                # Buscar condiciones de igualdad
                eq_match = re.match(r"(\w+)\s*=\s*(.+)", part)
                if eq_match:
                    attr_name = eq_match.group(1)
                    value_str = eq_match.group(2).strip()
                    value = self._convert_value(value_str, table_name, attr_name)
                    lista_busquedas.append([attr_name, value])
                    print(f"Condición exacta: {attr_name} = {value}")
                else:
                    print(f"Advertencia: Condición no soportada: {part}")

        print(f"Resultado final - Búsquedas exactas: {lista_busquedas}, Rangos: {lista_rangos}")
        return lista_busquedas, lista_rangos

    def _comparison_to_range(self, attr_name, operator, value, table_name):
        """
        Convierte un operador de comparación a un rango para búsqueda.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            attr_name (str): Nombre del atributo
            operator (str): Operador (>, <, >=, <=)
            value: Valor a comparar
            table_name (str): Nombre de la tabla

        Returns:
            list: [attr_name, min_val, max_val] o None si no se puede convertir
        """
        # Determinar el tipo de dato para establecer límites
        data_type = self._get_attribute_data_type(table_name, attr_name)

        if operator == ">":
            # attr > value → rango desde (value + epsilon) hasta infinito
            if isinstance(value, (int, float)):
                epsilon = 0.01 if isinstance(value, float) else 1
                min_val = value + epsilon
                max_val = self._get_max_value_for_type(data_type)
                return [attr_name, min_val, max_val]
            elif isinstance(value, Point):
                # Para Point, usar distancia al origen + epsilon
                min_val = Point(value.x + 0.01, value.y + 0.01)
                max_val = self._get_max_value_for_type(data_type)
                return [attr_name, min_val, max_val]

        elif operator == ">=":
            # attr >= value → rango desde value hasta infinito
            min_val = value
            max_val = self._get_max_value_for_type(data_type)
            return [attr_name, min_val, max_val]

        elif operator == "<":
            if isinstance(value, (int, float)):
                epsilon = 0.01 if isinstance(value, float) else 1
                min_val = self._get_min_value_for_type(data_type)
                max_val = value - epsilon
                return [attr_name, min_val, max_val]
            elif isinstance(value, Point):
                min_val = self._get_min_value_for_type(data_type)
                max_val = Point(value.x - 0.01, value.y - 0.01)
                return [attr_name, min_val, max_val]

        elif operator == "<=":
            # attr <= value → rango desde -infinito hasta value
            min_val = self._get_min_value_for_type(data_type)
            max_val = value
            return [attr_name, min_val, max_val]

        return None

    def _get_attribute_data_type(self, table_name, attr_name):
        """
        Obtiene el tipo de dato de un atributo.
        """
        if table_name not in self.tables:
            return "UNKNOWN"

        for attr in self.tables[table_name]["attributes"]:
            if attr["name"] == attr_name:
                return attr["data_type"].upper()

        return "UNKNOWN"

    def _get_max_value_for_type(self, data_type):
        """
        Obtiene el valor máximo para un tipo de dato.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        if data_type == "POINT":
            return Point(999999.0, 999999.0)  # Punto máximo
        elif "INT" in data_type:
            return 2147483647  # INT máximo
        elif "DECIMAL" in data_type or "FLOAT" in data_type:
            return 999999999.99  # Decimal grande
        elif "VARCHAR" in data_type or "CHAR" in data_type:
            return "ZZZZZZZZZ"  # String máximo lexicográfico
        else:
            return 999999999

    def _get_min_value_for_type(self, data_type):
        """
        Obtiene el valor mínimo para un tipo de dato.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        if data_type == "POINT":
            return Point(-999999.0, -999999.0)  # Punto mínimo
        elif "INT" in data_type:
            return -2147483648  # INT mínimo
        elif "DECIMAL" in data_type or "FLOAT" in data_type:
            return -999999999.99  # Decimal pequeño
        elif "VARCHAR" in data_type or "CHAR" in data_type:
            return ""  # String vacío
        else:
            return -999999999

    def _parse_values(self, values_str):
        """
        Divide una cadena de valores separados por comas, respetando strings con comillas.

        Args:
            values_str (str): Cadena con valores separados por comas.

        Returns:
            list: Lista de valores extraídos.
        """
        values = []
        current = ""
        in_string = False
        string_char = None

        for char in values_str:
            if char in ["'", '"'] and (not in_string or string_char == char):
                in_string = not in_string
                if in_string:
                    string_char = char
                else:
                    string_char = None
                current += char
            elif char == "," and not in_string:
                values.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            values.append(current.strip())

        return values

    def _convert_value(self, value_str, table_name, column_name):
        """
        Convierte un valor de string al tipo adecuado según la definición de la columna.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            value_str (str): Valor como string.
            table_name (str): Nombre de la tabla.
            column_name (str): Nombre de la columna.

        Returns:
            El valor convertido al tipo adecuado.
        """
        # Si la tabla no existe, devolver el valor tal cual
        if table_name not in self.tables:
            return value_str

        # Buscar el tipo de dato de la columna
        data_type = None
        for attr in self.tables[table_name]["attributes"]:
            if attr["name"] == column_name:
                data_type = attr["data_type"].upper()
                break

        if not data_type:
            return value_str

        # Eliminar comillas si es un string
        if value_str.startswith(("'", '"')) and value_str.endswith(("'", '"')):
            value_str = value_str[1:-1]

        # Convertir según el tipo de dato
        try:
            if data_type == "POINT":
                # Convertir string a Point
                return Point.from_string(value_str)
            elif "INT" in data_type:
                return int(value_str)
            elif "DECIMAL" in data_type or "FLOAT" in data_type or "DOUBLE" in data_type:
                return float(value_str)
            elif "BOOL" in data_type:
                return value_str.lower() in ("true", "yes", "1", "t", "y")
            else:
                # Para VARCHAR, CHAR, etc., devolver como string
                return value_str
        except (ValueError, TypeError):
            # Si hay un error de conversión, devolver el valor original
            return value_str

    # Resto de métodos siguen siendo los mismos...
    def get_table(self, table_name):
        """
        Obtiene la información de una tabla específica.

        Args:
            table_name (str): Nombre de la tabla.

        Returns:
            dict: Información de la tabla o None si no existe.
        """

        if table_name in self.blacklisted_tables:
            return None

        return self.tables.get(table_name)

    def get_storage_manager(self, table_name):
        """
        Obtiene el gestor de almacenamiento de una tabla.

        Args:
            table_name (str): Nombre de la tabla.

        Returns:
            TableStorageManager: El gestor de almacenamiento o None si no existe.
        """
        return self.storage_managers.get(table_name)

    def get_all_tables(self):
        """
        Obtiene todas las tablas almacenadas.

        Returns:
            dict: Diccionario con todas las tablas.
        """

        return {name: info for name, info in self.tables.items() if name not in self.blacklisted_tables}

    def execute_sql(self, sql_statement):
        """
        Ejecuta una instrucción SQL.

        Args:
            sql_statement (str): La instrucción SQL a ejecutar.

        Returns:
            list: Resultados de la ejecución.
        """
        return self.parse_sql_statement(sql_statement)

    def execute_delete(self, sql_delete_statement):
        """
        Ejecuta una consulta DELETE directamente y retorna el resultado.

        Args:
            sql_delete_statement (str): La instrucción DELETE.

        Returns:
            dict: Resultado con información de éxito/error y cantidad eliminada
        """
        try:
            operations = self.parse_sql_statement(sql_delete_statement)

            for op_type, result in operations:
                if op_type == "DELETE":
                    return result

            return {"error": True, "message": "No se encontró operación DELETE"}

        except Exception as e:
            return {"error": True, "message": f"Error al ejecutar DELETE: {str(e)}"}

    def _process_select(self, sql_statement):
        """
        Procesa una instrucción SELECT y ejecuta la búsqueda.
        VERSIÓN ACTUALIZADA con soporte para funciones espaciales R-Tree.
        """
        select_info = self.parse_sql_select(sql_statement)

        if select_info.get("error", False):
            print(f"Error en SELECT: {select_info['message']}")
            return select_info

        table_name = select_info["table_name"]
        lista_busquedas = select_info["lista_busquedas"]
        lista_rangos = select_info["lista_rangos"]
        lista_espaciales = select_info.get("lista_espaciales", [])
        requested_attributes = select_info["requested_attributes"]

        # Ejecutar la consulta si hay un gestor de almacenamiento
        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]

            try:
                # Ejecutar el select con listas de búsquedas, rangos Y espaciales
                resultado = storage_manager.select(
                    lista_busquedas=lista_busquedas if lista_busquedas else None,
                    lista_rangos=lista_rangos if lista_rangos else None,
                    lista_espaciales=lista_espaciales if lista_espaciales else None,
                    requested_attributes=requested_attributes,
                )

                return {
                    "error": False,
                    "table_name": table_name,
                    "lista_busquedas": lista_busquedas,
                    "lista_rangos": lista_rangos,
                    "lista_espaciales": lista_espaciales,
                    "requested_attributes": requested_attributes,
                    "resultado": resultado,
                }

            except Exception as e:
                error_result = {
                    "error": True,
                    "message": f"Error al ejecutar SELECT en tabla '{table_name}': {str(e)}",
                }
                print(f"Error: {error_result['message']}")
                return error_result
        else:
            error_result = {
                "error": True,
                "message": f"No hay un gestor de almacenamiento para la tabla '{table_name}'",
            }
            print(f"Error: {error_result['message']}")
            return error_result

    def _safe_parse_basic_delete(self, sql_statement: str):
        """
        Parser lineal y seguro para:
        DELETE FROM <tabla> [WHERE <condiciones>][;]
        Respeta comillas y paréntesis para no romper funciones/strings en WHERE.
        Retorna: (table_name, where_clause|None)
        """
        # 1) Normalización (sin regex)
        s = sql_statement.strip().rstrip(";")
        up = s.upper()

        # 2) Validación del prefijo DELETE
        if not up.startswith(DELETE_KEYWORD):
            return None, None

        # 3) Buscar FROM fuera de comillas/paréntesis
        from_pos = self._find_keyword_outside_quotes_parens(s, FROM_KEYWORD, len(DELETE_KEYWORD))
        if from_pos == -1:
            return None, None

        # 4) Extraer nombre de tabla
        after_from = s[from_pos + len(FROM_KEYWORD) :].lstrip()
        table_name = self._extract_table_name(after_from)
        if not table_name:
            return None, None

        # 5) Restante tras la tabla
        rest = after_from[len(table_name) :].lstrip()
        if not rest:
            # No hay WHERE → DELETE FROM <tabla>
            return table_name, None

        # 6) Si hay algo, debe ser WHERE (delimitado) al inicio
        where_pos = self._find_keyword_outside_quotes_parens(rest, WHERE_KEYWORD, 0)
        if where_pos == 0:
            where_clause = rest[len(WHERE_KEYWORD) :].strip()
            return table_name, (where_clause if where_clause else None)

        # Si aparece WHERE más adelante o hay basura antes, es inválido
        return None, None

    def _safe_parse_basic_select(self, sql_statement: str):
        """
        Parser lineal y seguro para: SELECT <cols> FROM <tabla> [WHERE <cláusula>][;]
        Respeta comillas y paréntesis para no romper con funciones/strings.
        """
        s = sql_statement.strip().rstrip(";")
        up = s.upper()

        if not up.startswith(SELECT_KEYWORD):
            return None, None, None

        # Encontrar la posición de FROM
        from_pos = self._find_keyword_outside_quotes_parens(s, FROM_KEYWORD, len(SELECT_KEYWORD))
        if from_pos == -1:
            return None, None, None

        # Extraer las partes principales
        columns_part = s[len(SELECT_KEYWORD) : from_pos].strip()
        if not columns_part:
            return None, None, None

        after_from = s[from_pos + len(FROM_KEYWORD) :].strip()
        table_name = self._extract_table_name(after_from)
        if not table_name:
            return None, None, None

        # Extraer cláusula WHERE si existe
        rest = after_from[len(table_name) :].strip()
        where_clause = self._extract_where_clause(rest)

        return columns_part, table_name, where_clause

    def _find_keyword_outside_quotes_parens(self, src: str, keyword: str, start: int = 0) -> int:
        """Encuentra una palabra clave fuera de comillas y paréntesis."""
        upsrc = src.upper()
        keyword_len = len(keyword)
        i = start

        while i <= len(src) - keyword_len:
            # Saltar contenido entre comillas
            if src[i] in ("'", '"'):
                i = self._skip_quoted_content(src, i)
                continue

            # Saltar contenido entre paréntesis
            if src[i] == "(":
                i = self._skip_parentheses_content(src, i)
                continue

            # Verificar si encontramos la palabra clave
            if self._is_keyword_match(upsrc, keyword, i, keyword_len):
                return i

            i += 1

        return -1

    def _skip_quoted_content(self, src: str, start: int) -> int:
        """Salta el contenido entre comillas y retorna la posición después del cierre."""
        quote_char = src[start]
        i = start + 1

        while i < len(src):
            if src[i] == quote_char:
                return i + 1
            i += 1

        return len(src)  # Si no se encuentra el cierre, ir al final

    def _skip_parentheses_content(self, src: str, start: int) -> int:
        """Salta el contenido entre paréntesis balanceados."""
        depth = 1
        i = start + 1

        while i < len(src) and depth > 0:
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
            elif src[i] in ("'", '"'):
                i = self._skip_quoted_content(src, i)
                continue
            i += 1

        return i

    def _is_keyword_match(self, upsrc: str, keyword: str, pos: int, keyword_len: int) -> bool:
        """Verifica si hay una coincidencia de palabra clave en la posición dada."""
        if upsrc[pos : pos + keyword_len] != keyword:
            return False

        # Verificar límites de palabra (espacios o bordes)
        left_boundary = pos == 0 or upsrc[pos - 1].isspace()
        right_boundary = (pos + keyword_len == len(upsrc)) or upsrc[pos + keyword_len].isspace()

        return left_boundary and right_boundary

    def _extract_table_name(self, after_from: str) -> str:
        """Extrae el nombre de la tabla del texto después de FROM."""
        table_name = ""
        i = 0

        while i < len(after_from) and self._is_valid_table_char(after_from[i]):
            table_name += after_from[i]
            i += 1

        return table_name

    def _is_valid_table_char(self, char: str) -> bool:
        """Verifica si un carácter es válido para un nombre de tabla."""
        return char.isalnum() or char == "_"

    def _extract_where_clause(self, rest: str):
        """Extrae la cláusula WHERE si existe."""
        if rest and rest.upper().startswith(WHERE_KEYWORD):
            return rest[len(WHERE_KEYWORD) :].strip()
        return None

    def parse_sql_select(self, sql_statement):
        """
        Analiza una instrucción SQL SELECT y extrae las condiciones de búsqueda.
        VERSIÓN ACTUALIZADA con soporte para funciones espaciales RADIUS() y KNN().

        Soporta:
        - SELECT * FROM tabla WHERE attr=value
        - SELECT * FROM tabla WHERE attr BETWEEN min_val AND max_val
        - SELECT * FROM tabla WHERE RADIUS(attr, center_point, radius)
        - SELECT * FROM tabla WHERE KNN(attr, center_point, k)
        """
        # Patrón para SELECT básico
        columns_str, table_name, where_clause = self._safe_parse_basic_select(sql_statement)
        if not columns_str or not table_name:
            return {"error": True, "message": "Formato de SELECT no válido"}

        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {"error": True, "message": f"La tabla '{table_name}' no existe"}

        # Procesar columnas solicitadas (código existente)
        requested_attributes = []
        if columns_str == "*":
            requested_attributes = [attr["name"] for attr in self.tables[table_name]["attributes"]]
        else:
            attr_names = [attr.strip() for attr in columns_str.split(",")]
            table_attributes = {attr["name"] for attr in self.tables[table_name]["attributes"]}

            for attr_name in attr_names:
                if attr_name not in table_attributes:
                    return {
                        "error": True,
                        "message": f"El atributo '{attr_name}' no existe en la tabla '{table_name}'",
                    }

            requested_attributes = attr_names

        # Analizar condiciones WHERE y separar en búsquedas exactas, rangos Y espaciales
        lista_busquedas = []
        lista_rangos = []
        lista_espaciales = []

        if where_clause:
            try:
                # 🔧 USAR SOLO EL NUEVO MÉTODO que soporta espaciales
                lista_busquedas, lista_rangos, lista_espaciales = self._parse_where_with_spatial(where_clause, table_name)
            except Exception as e:
                return {"error": True, "message": f"Error al parsear condiciones WHERE: {str(e)}"}

        return {
            "error": False,
            "table_name": table_name,
            "columns": columns_str,
            "requested_attributes": requested_attributes,
            "lista_busquedas": lista_busquedas,
            "lista_rangos": lista_rangos,
            "lista_espaciales": lista_espaciales,
        }

    def _parse_where_with_spatial(self, where_clause, table_name):
        """
        Parser espacial ULTRA SIMPLE usando split manual.
        """
        lista_busquedas = []
        lista_rangos = []
        lista_espaciales = []

        remaining_clause = where_clause.strip()

        # Función helper para extraer función espacial
        def extract_spatial_function(clause, func_name):
            """Extrae función espacial usando búsqueda manual"""
            func_upper = func_name.upper()

            # Buscar inicio de la función (case insensitive)
            start_idx = clause.upper().find(func_upper + "(")
            if start_idx == -1:
                return None, clause

            # Encontrar paréntesis balanceados
            paren_start = start_idx + len(func_upper)
            paren_count = 0
            end_idx = paren_start

            while end_idx < len(clause):
                if clause[end_idx] == "(":
                    paren_count += 1
                elif clause[end_idx] == ")":
                    paren_count -= 1
                    if paren_count == 0:
                        break
                end_idx += 1

            if paren_count != 0:
                return None, clause

            # Extraer contenido entre paréntesis
            content = clause[paren_start + 1 : end_idx]

            # PARSING MANUAL SUPER SIMPLE
            # Buscar las tres partes separadas por comas, pero respetando las comillas
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            paren_depth = 0

            for char in content:
                if not in_quotes and char in ["'", '"']:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif in_quotes and char == quote_char:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif not in_quotes and char == "(":
                    paren_depth += 1
                    current_part += char
                elif not in_quotes and char == ")":
                    paren_depth -= 1
                    current_part += char
                elif not in_quotes and char == "," and paren_depth == 0:
                    # Encontramos un separador válido
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char

            # Agregar última parte
            if current_part.strip():
                parts.append(current_part.strip())

            if len(parts) == 3:
                attr_name = parts[0].strip()
                center_str = parts[1].strip()
                param_str = parts[2].strip()

                # Remover comillas externas del center_str
                if (center_str.startswith("'") and center_str.endswith("'")) or (center_str.startswith('"') and center_str.endswith('"')):
                    center_str = center_str[1:-1]

                new_clause = clause[:start_idx] + clause[end_idx + 1 :]

                return (attr_name, center_str, param_str), new_clause.strip()

            return None, clause

        # 1. Extraer RADIUS
        radius_result, remaining_clause = extract_spatial_function(remaining_clause, "RADIUS")
        if radius_result:
            try:
                attr_name, center_str, radius_str = radius_result

                center_point = self._convert_value(center_str, table_name, attr_name)
                radius = float(radius_str)

                lista_espaciales.append(["RADIUS", attr_name, center_point, radius])

            except Exception as e:
                import traceback

                traceback.print_exc()

        # 2. Extraer KNN
        knn_result, remaining_clause = extract_spatial_function(remaining_clause, "KNN")
        if knn_result:
            try:
                attr_name, center_str, k_str = knn_result

                center_point = self._convert_value(center_str, table_name, attr_name)
                k = int(k_str)

                lista_espaciales.append(["KNN", attr_name, center_point, k])

            except Exception as e:
                import traceback

                traceback.print_exc()

        # 3. Procesar resto de condiciones
        remaining_clause = remaining_clause.strip()

        # Limpiar ANDs sobrantes
        while remaining_clause.startswith("AND "):
            remaining_clause = remaining_clause[4:].strip()
        while remaining_clause.endswith(" AND"):
            remaining_clause = remaining_clause[:-4].strip()

        if remaining_clause:
            try:
                temp_busquedas, temp_rangos = self._parse_where_with_ranges(remaining_clause, table_name)
                lista_busquedas.extend(temp_busquedas)
                lista_rangos.extend(temp_rangos)
            except Exception as e:
                print(f"Error en condiciones restantes: {e}")

        print(f"RESULTADO: Exactas={lista_busquedas}, Rangos={lista_rangos}, Espaciales={lista_espaciales}")
        return lista_busquedas, lista_rangos, lista_espaciales

    def _process_delete(self, sql_statement):
        """
        Procesa una instrucción DELETE FROM y ejecuta la eliminación.
        VERSIÓN ACTUALIZADA con soporte para funciones espaciales.
        """
        delete_info = self.parse_sql_delete(sql_statement)

        if delete_info.get("error", False):
            print(f"Error en DELETE: {delete_info['message']}")
            return delete_info

        table_name = delete_info["table_name"]
        lista_busquedas = delete_info["lista_busquedas"]
        lista_rangos = delete_info["lista_rangos"]
        lista_espaciales = delete_info.get("lista_espaciales", [])

        print(f"Procesando DELETE en tabla: {table_name}")
        if lista_busquedas:
            print(f"Condiciones exactas: {lista_busquedas}")
        if lista_rangos:
            print(f"Condiciones de rango: {lista_rangos}")
        if lista_espaciales:
            print(f"Condiciones espaciales: {lista_espaciales}")

        # Verificar que la tabla existe
        if table_name not in self.tables:
            error_result = {"error": True, "message": f"La tabla '{table_name}' no existe"}
            print(f"Error: {error_result['message']}")
            return error_result

        if table_name in self.storage_managers:
            storage_manager = self.storage_managers[table_name]

            try:
                resultado_busqueda = storage_manager.select(
                    lista_busquedas=lista_busquedas if lista_busquedas else None,
                    lista_rangos=lista_rangos if lista_rangos else None,
                    lista_espaciales=lista_espaciales if lista_espaciales else None,
                )

                if resultado_busqueda.get("error", False):
                    print(f"Error al buscar registros para eliminar: {resultado_busqueda.get('message', 'Error desconocido')}")
                    return resultado_busqueda

                records_to_delete = resultado_busqueda.get("numeros_registro", [])

                if not records_to_delete:
                    print("No se encontraron registros que cumplan las condiciones para eliminar")
                    return {
                        "error": False,
                        "table_name": table_name,
                        "records_deleted": [],
                        "count": 0,
                        "message": "No se encontraron registros para eliminar",
                    }

                print(f"Se encontraron {len(records_to_delete)} registros para eliminar: {records_to_delete}")

                # Eliminar los registros usando el método delete_records del storage manager
                deleted_count = storage_manager.delete_records(records_to_delete)

                success_message = f"Se eliminaron {deleted_count} registro(s) exitosamente"

                return {
                    "error": False,
                    "table_name": table_name,
                    "lista_busquedas": lista_busquedas,
                    "lista_rangos": lista_rangos,
                    "lista_espaciales": lista_espaciales,
                    "records_deleted": records_to_delete,
                    "count": deleted_count,
                    "message": success_message,
                }

            except Exception as e:
                error_result = {
                    "error": True,
                    "message": f"Error al ejecutar DELETE en tabla '{table_name}': {str(e)}",
                }
                print(f"Error: {error_result['message']}")
                return error_result
        else:
            error_result = {
                "error": True,
                "message": f"No hay un gestor de almacenamiento para la tabla '{table_name}'",
            }
            print(f"Error: {error_result['message']}")
            return error_result

    def parse_sql_delete(self, sql_statement):
        """
        Analiza una instrucción SQL DELETE FROM y extrae las condiciones.
        VERSIÓN ACTUALIZADA con soporte para funciones espaciales.
        """
        # Patrón para DELETE básico
        table_name, where_clause = self._safe_parse_basic_delete(sql_statement)
        if not table_name:
            return {
                "error": True,
                "message": "Formato de DELETE no válido. Use: DELETE FROM tabla [WHERE condiciones]",
            }

        # Verificar que la tabla existe
        if table_name not in self.tables:
            return {"error": True, "message": f"La tabla '{table_name}' no existe"}

        # Si no hay WHERE clause, es un DELETE sin condiciones (eliminar todo)
        if not where_clause:
            return {
                "error": True,
                "message": "DELETE sin WHERE no está permitido por seguridad. Especifique condiciones WHERE.",
            }

        # Analizar condiciones WHERE y separar en búsquedas exactas, rangos Y espaciales
        lista_busquedas = []
        lista_rangos = []
        lista_espaciales = []

        try:
            lista_busquedas, lista_rangos, lista_espaciales = self._parse_where_with_spatial(where_clause, table_name)
        except Exception as e:
            return {"error": True, "message": f"Error al parsear condiciones WHERE: {str(e)}"}

        return {
            "error": False,
            "table_name": table_name,
            "lista_busquedas": lista_busquedas,
            "lista_rangos": lista_rangos,
            "lista_espaciales": lista_espaciales,
        }
