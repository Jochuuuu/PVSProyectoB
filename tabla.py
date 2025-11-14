import os
import struct
import json
import re
from pathlib import Path
from estructuras.hash import ExtendibleHashFile
from estructuras.avl import AVLFile
from estructuras.point_class import Point
from estructuras.rtree_class import RTreeFile


class TableStorageManager:
    """
    Gestiona una tabla de almacenamiento con registros adaptados según los atributos de la tabla
    usando una estrategia de lista libre para reutilizar registros eliminados.
    VERSIÓN ACTUALIZADA con soporte completo para tipo POINT.
    """

    # Constantes para los valores especiales de 'next'
    RECORD_NORMAL = -2  # Registro normal (no eliminado)
    RECORD_END = -1  # Último registro eliminado en la lista libre

    # Mapeo de tipos de datos a formatos de struct
    TYPE_FORMATS = {
        "INT": "i",  # 4 bytes para enteros
        "DECIMAL": "d",  # 8 bytes para decimales (double)
        "CHAR": "s",  # Char de longitud fija
        "VARCHAR": "s",  # Varchar de longitud fija
        "BOOL": "?",  # 1 byte para booleanos
        "DATE": "I",  # 4 bytes para fechas (timestamp)
        "POINT": "dd",  # 16 bytes para punto 2D (dos doubles: x, y)
    }

    # Tamaños por defecto para los tipos de datos (en bytes)
    TYPE_SIZES = {"INT": 4, "DECIMAL": 8, "BOOL": 1, "DATE": 4, "POINT": 16}

    def __init__(self, table_name, table_info, base_dir="tablas"):
        """
        Inicializa el administrador de la tabla.

        Args:
            table_name: Nombre de la tabla
            table_info: Diccionario con información sobre la estructura de la tabla
            base_dir: Directorio base donde se almacenarán los archivos de la tabla
        """
        self.table_name = table_name
        self.table_info = table_info
        self.base_dir = base_dir

        # Asegurar que el directorio base existe
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs("indices", exist_ok=True)

        # Nombre del archivo binario de la tabla
        self.filename = os.path.join(base_dir, f"{table_name}.bin")

        # Crear formatos de struct para los atributos
        self.attribute_formats = {}
        self.attribute_sizes = {}
        self.indices = {}
        self.primary_key_attr = None

        # Crear formato para cada atributo
        record_format_parts = []
        for attr in table_info["attributes"]:
            fmt, size = self._get_format_for_attribute(attr)
            self.attribute_formats[attr["name"]] = fmt
            self.attribute_sizes[attr["name"]] = size
            record_format_parts.append(fmt)

            if attr.get("is_key", False):
                self.primary_key_attr = attr["name"]

        # Añadir formato para el campo 'next' que se usa para la lista libre
        self.next_format = "i"  # 4 bytes para next (int)
        record_format_parts.append(self.next_format)

        # Formato completo del registro
        self.record_format = f"<{''.join(record_format_parts)}"

        # Tamaño del registro en bytes
        self.record_size = struct.calcsize(self.record_format)

        # Tamaño del encabezado (contiene el puntero a la cabecera de la lista libre)
        self.header_format = "<i"  # 4 bytes para la cabecera (int)
        self.header_size = struct.calcsize(self.header_format)

        if not os.path.exists(self.filename):
            self._initialize_file()

        INDEX_CLASSES = {
            "hash": ExtendibleHashFile,
            "avl": AVLFile,
            "btree": AVLFile,
            "isam": AVLFile,
            "rtree": RTreeFile,
        }

        for attr_index, attr in enumerate(table_info["attributes"], 1):
            if attr.get("index"):
                index_type = attr["index"].lower()
                print(index_type, "y atr", attr, attr_index)
                if index_type not in INDEX_CLASSES:
                    index_type = "avl"

                real_attr_index = self._calculate_real_attr_index_for_index(attr_index)

                # Parámetros comunes
                index_params = {
                    "record_format": self.record_format,
                    "index_attr": real_attr_index,
                    "table_name": self.table_name,
                    "is_key": attr.get("is_key", False),
                }

                # Crear el índice
                index_class = INDEX_CLASSES[index_type]
                self.indices[attr["name"]] = index_class(**index_params)

    def _is_rtree_spatial_index(self, attr_name):
        """
        Verifica si un atributo tiene índice R-Tree y es de tipo POINT.

        Args:
            attr_name (str): Nombre del atributo

        Returns:
            bool: True si es R-Tree espacial
        """
        if attr_name not in self.indices:
            return False

        # Verificar si el índice es R-Tree
        if not isinstance(self.indices[attr_name], RTreeFile):
            return False

        # Verificar si el tipo de dato es POINT
        for attr in self.table_info["attributes"]:
            if attr["name"] == attr_name and attr["data_type"].upper() == "POINT":
                return True

        return False

    def spatial_radius_search(self, attr_name, center_point, radius):
        """
        Realiza búsqueda radial usando R-Tree espacial.

        Args:
            attr_name (str): Nombre del atributo POINT
            center_point (Point): Punto central
            radius (float): Radio de búsqueda

        Returns:
            list: Números de registro dentro del radio
        """
        if not self._is_rtree_spatial_index(attr_name):
            raise ValueError(f"El atributo '{attr_name}' no tiene índice R-Tree espacial")

        # Convertir a objeto Point si es necesario
        if not isinstance(center_point, Point):
            center_point = self._convert_search_value(attr_name, center_point)

        rtree_index = self.indices[attr_name]
        return rtree_index.range_search_radius(center_point, radius)

    def spatial_knn_search(self, attr_name, center_point, k):
        """
        Realiza búsqueda de K vecinos más cercanos usando R-Tree espacial.

        Args:
            attr_name (str): Nombre del atributo POINT
            center_point (Point): Punto central
            k (int): Número de vecinos más cercanos

        Returns:
            list: Números de registro de los k vecinos más cercanos
        """
        if not self._is_rtree_spatial_index(attr_name):
            raise ValueError(f"El atributo '{attr_name}' no tiene índice R-Tree espacial")

        # Convertir a objeto Point si es necesario
        if not isinstance(center_point, Point):
            center_point = self._convert_search_value(attr_name, center_point)

        rtree_index = self.indices[attr_name]
        return rtree_index.range_search_knn_simple(center_point, k)

    def _calculate_real_attr_index_for_index(self, logical_attr_index):
        """
        Calcula el índice real del atributo considerando que los campos POINT
        ocupan 2 posiciones en el struct, pero se cuentan como 1 atributo lógico.

        Args:
            logical_attr_index (int): Índice lógico del atributo (1-based)

        Returns:
            int: Índice real para usar en los índices (1-based)
        """
        real_position = 0

        # Iterar hasta el atributo solicitado (excluido)
        for i in range(logical_attr_index - 1):  # -1 porque logical_attr_index es 1-based
            if i < len(self.table_info["attributes"]):
                attr_type = self.table_info["attributes"][i]["data_type"].upper()
                if attr_type == "POINT":
                    real_position += 2  # POINT ocupa 2 posiciones (x, y)
                else:
                    real_position += 1  # Otros tipos ocupan 1 posición
            else:
                real_position += 1

        # Retornar posición real + 1 (porque los índices esperan 1-based)
        return real_position + 1

    def _get_format_for_attribute(self, attr):
        """
        Obtiene el formato de struct y tamaño para un atributo.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            attr: Diccionario con información del atributo

        Returns:
            Tupla (formato, tamaño)
        """
        data_type = attr["data_type"].upper()

        # Manejar tipo POINT específicamente
        if data_type == "POINT":
            return self.TYPE_FORMATS["POINT"], self.TYPE_SIZES["POINT"]

        # Manejar VARCHAR y CHAR con tamaño específico
        size_match = re.match(r"(VARCHAR|CHAR)\[(\d+)\]", data_type)
        if size_match:
            base_type = size_match.group(1)
            size = int(size_match.group(2))
            return f"{size}s", size

        # Para los tipos básicos
        for base_type in self.TYPE_FORMATS:
            if data_type.startswith(base_type):
                return self.TYPE_FORMATS[base_type], self.TYPE_SIZES.get(base_type, 4)

        # Tipo no reconocido, usar un formato genérico
        return "i", 4

    def _initialize_file(self):
        """Inicializa el archivo con un encabezado que indica que no hay registros eliminados."""
        with open(self.filename, "wb") as f:
            f.write(struct.pack(self.header_format, -1))
            metadata_file = self._get_metadata_path()
            with open(metadata_file, "w") as mf:
                json.dump(self.table_info, mf, indent=2)

    def _get_metadata_path(self):
        """Obtiene la ruta del archivo de metadatos."""
        return os.path.join(self.base_dir, f"{self.table_name}_meta.json")

    def _read_header(self):
        """Lee el valor de la cabecera (puntero al primer registro eliminado)."""
        with open(self.filename, "rb") as f:
            header_data = f.read(self.header_size)
            return struct.unpack(self.header_format, header_data)[0]

    def _write_header(self, header_value):
        """Actualiza el valor de la cabecera."""
        with open(self.filename, "r+b") as f:
            f.write(struct.pack(self.header_format, header_value))

    def _get_record_position(self, id):
        """
        Calcula la posición del registro en el archivo basado en su id.
        Asume que los ids son consecutivos empezando desde 1.
        """
        return self.header_size + (id - 1) * self.record_size

    def _read_record(self, id):
        """
        Lee un registro específico por su id.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.
        """
        position = self._get_record_position(id)

        with open(self.filename, "rb") as f:
            f.seek(position)
            record_data = f.read(self.record_size)

            if not record_data or len(record_data) < self.record_size:
                return None

            # Desempaquetar los datos en una lista
            unpacked_data = list(struct.unpack(self.record_format, record_data))

            # Convertir los valores según el tipo
            result = {}
            data_index = 0

            for attr in self.table_info["attributes"]:
                attr_name = attr["name"]
                data_type = attr["data_type"].upper()

                if data_type == "POINT":
                    # Para POINT, tomar dos valores consecutivos (x, y)
                    if data_index + 1 < len(unpacked_data):
                        x_value = unpacked_data[data_index]
                        y_value = unpacked_data[data_index + 1]
                        value = Point(x_value, y_value)
                        data_index += 2  # Saltar dos posiciones para POINT
                    else:
                        value = Point(0.0, 0.0)  # Valor por defecto
                        data_index += 2
                elif data_type.startswith(("VARCHAR", "CHAR")):
                    # Convertir strings y eliminar padding
                    value = unpacked_data[data_index]
                    value = value.decode("utf-8").rstrip("\x00")
                    data_index += 1
                else:
                    # Otros tipos de datos
                    value = unpacked_data[data_index]
                    data_index += 1

                result[attr_name] = value

            # El último valor siempre es el puntero 'next'
            result["next"] = unpacked_data[-1]

            return result

    def _pack_record_data(self, record_data):
        """
        Empaqueta los datos de un registro para almacenarlos.
        VERSIÓN ACTUALIZADA que maneja tipo POINT.

        Args:
            record_data: Diccionario con los datos del registro

        Returns:
            Datos empaquetados listos para escribir en el archivo
        """
        values = []

        for attr in self.table_info["attributes"]:
            attr_name = attr["name"]
            data_type = attr["data_type"].upper()
            value = record_data.get(attr_name)

            # Convertir según el tipo de dato
            if data_type == "POINT":
                # Para POINT, extraer x e y y añadir ambos valores
                if isinstance(value, Point):
                    values.append(value.x)
                    values.append(value.y)
                elif isinstance(value, (list, tuple)) and len(value) >= 2:
                    # Si viene como lista o tupla, usar los primeros dos elementos
                    values.append(float(value[0]))
                    values.append(float(value[1]))
                elif isinstance(value, str):
                    # Si viene como string, intentar parsearlo como Point
                    try:
                        point = Point.from_string(value)
                        values.append(point.x)
                        values.append(point.y)
                    except:
                        # Si falla el parsing, usar valores por defecto
                        values.append(0.0)
                        values.append(0.0)
                else:
                    # Valor por defecto para POINT
                    values.append(0.0)
                    values.append(0.0)

            elif data_type.startswith(("VARCHAR", "CHAR")):
                size_match = re.match(r"(VARCHAR|CHAR)\[(\d+)\]", data_type)
                if size_match:
                    size = int(size_match.group(2))
                    # Asegurar que es una cadena y codificarla
                    if not isinstance(value, str):
                        value = str(value)
                    value = value.encode("utf-8")[:size].ljust(size, b"\x00")
                values.append(value)
            else:
                # Otros tipos de datos
                values.append(value)

        # Añadir el valor de 'next'
        values.append(record_data.get("next", self.RECORD_NORMAL))

        # Empaquetar todos los valores
        return struct.pack(self.record_format, *values)

    def _write_record(self, id, record_data):
        """
        Escribe un registro en la posición correspondiente a su id.

        Args:
            id: ID para posicionar el registro
            record_data: Diccionario con los datos del registro
        """
        position = self._get_record_position(id)

        # Empaquetar los datos
        packed_data = self._pack_record_data(record_data)

        # Escribir en el archivo
        with open(self.filename, "r+b") as f:
            f.seek(position)
            f.write(packed_data)

    def _get_file_size(self):
        """Obtiene el tamaño actual del archivo."""
        return os.path.getsize(self.filename)

    def _get_record_count(self):
        """Obtiene el número de registros en el archivo (incluyendo los eliminados)."""
        file_size = self._get_file_size()
        return (file_size - self.header_size) // self.record_size

    def insert(self, record_data):
        """
        Inserta un nuevo registro, reutilizando espacios eliminados si están disponibles.
        VERSIÓN ACTUALIZADA que maneja validación de tipos incluyendo POINT.

        Args:
            record_data: Diccionario con los datos del registro

        Returns:
            ID del registro insertado
        """
        validated_record = self._validate_and_convert_record(record_data)

        for attr in self.table_info["attributes"]:
            if attr["name"] not in validated_record:
                raise ValueError(f"Falta el atributo {attr['name']} en los datos del registro")

        if self.primary_key_attr in self.indices:
            primary_key_value = validated_record[self.primary_key_attr]
            index = self.indices[self.primary_key_attr]

            existing_records = index.search(primary_key_value)
            if existing_records:
                return None

        cabecera = self._read_header()
        record_id = None

        if cabecera != -1:
            deleted_record = self._read_record(cabecera)
            next_free = deleted_record["next"]

            self._write_header(next_free)

            record_id = cabecera

            validated_record["next"] = self.RECORD_NORMAL

            self._write_record(record_id, validated_record)
        else:
            record_count = self._get_record_count()
            record_id = record_count + 1

            validated_record["next"] = self.RECORD_NORMAL

            self._write_record(record_id, validated_record)

        for attr_name, index in self.indices.items():
            index.insert_record(record_id)

        return record_id

    def _validate_and_convert_record(self, record_data):
        """
        Valida y convierte los tipos de datos del registro incluyendo POINT.

        Args:
            record_data: Diccionario con los datos del registro

        Returns:
            dict: Registro con tipos validados y convertidos
        """
        validated_record = {}

        for attr in self.table_info["attributes"]:
            attr_name = attr["name"]
            data_type = attr["data_type"].upper()
            value = record_data.get(attr_name)

            if data_type == "POINT":
                # Convertir a objeto Point si no lo es ya
                if isinstance(value, Point):
                    validated_record[attr_name] = value
                elif isinstance(value, (list, tuple)) and len(value) >= 2:
                    validated_record[attr_name] = Point(float(value[0]), float(value[1]))
                elif isinstance(value, str):
                    try:
                        validated_record[attr_name] = Point.from_string(value)
                    except:
                        validated_record[attr_name] = Point(0.0, 0.0)
                elif isinstance(value, dict) and "x" in value and "y" in value:
                    validated_record[attr_name] = Point(float(value["x"]), float(value["y"]))
                else:
                    validated_record[attr_name] = Point(0.0, 0.0)
            else:
                # Para otros tipos, usar el valor tal como viene
                validated_record[attr_name] = value

        return validated_record

    def delete(self, id):
        """
        Marca un registro como eliminado y lo agrega a la lista de registros libres.
        VERSIÓN CORREGIDA que NO elimina de índices (eso se hace en delete_records).

        Args:
            id: ID del registro a eliminar

        Returns:
            True si se eliminó correctamente, False si no se encontró
        """
        record = self._read_record(id)
        if not record:
            return False

        if record["next"] != self.RECORD_NORMAL:
            return False

        cabecera = self._read_header()

        record["next"] = cabecera
        self._write_record(id, record)

        self._write_header(id)

        return True

    def delete_records(self, record_numbers):
        """
        Elimina múltiples registros especificados por sus números de registro.
        VERSIÓN CON DEBUG COMPLETO para mostrar estado de índices.

        Args:
            record_numbers (list): Lista de números de registro a eliminar

        Returns:
            int: Cantidad de registros eliminados exitosamente
        """
        if not record_numbers:
            print("No hay registros para eliminar")
            return 0

        deleted_count = 0
        failed_records = []

        for record_num in record_numbers:
            try:
                record = self._read_record(record_num)
                if not record or record.get("next") != self.RECORD_NORMAL:
                    failed_records.append(record_num)
                    continue

                indices_success = True

                for attr_name, index in self.indices.items():
                    try:
                        if hasattr(index, "delete_record"):
                            result = index.delete_record(record_num)
                        else:
                            if isinstance(index, dict) and attr_name in record:
                                value = record[attr_name]
                                if value in index and record_num in index[value]:
                                    index[value].remove(record_num)
                                    if not index[value]:
                                        del index[value]

                    except Exception as e:
                        print(f"  Error al eliminar del índice {attr_name}: {e}")
                        indices_success = False

                if indices_success:
                    if self.delete(record_num):
                        deleted_count += 1
                    else:
                        failed_records.append(record_num)
                else:
                    failed_records.append(record_num)

            except Exception as e:
                print(f"Error general al eliminar registro {record_num}: {e}")
                failed_records.append(record_num)

        return deleted_count

    def _remove_from_all_indices(self, record, record_num):
        """
        Elimina un registro de todos los índices usando delete_record().

        Args:
            record (dict): Datos del registro
            record_num (int): Número del registro
        """
        for attr_name, index in self.indices.items():
            try:
                if hasattr(index, "delete_record"):
                    result = index.delete_record(record_num)
                else:
                    if attr_name in record:
                        value = record[attr_name]
                        if value in index and record_num in index[value]:
                            index[value].remove(record_num)
                            if not index[value]:
                                del index[value]

            except Exception as e:
                print(f" Error al eliminar del índice {attr_name}: {e}")

    def select(
        self,
        lista_busquedas=None,
        lista_rangos=None,
        lista_espaciales=None,
        requested_attributes=None,
    ):
        """
        Busca registros que cumplan todas las condiciones especificadas.
        VERSIÓN ACTUALIZADA con soporte para búsquedas espaciales R-Tree.

        Args:
            lista_busquedas: Lista de búsquedas exactas [attr_name, value]
            lista_rangos: Lista de rangos [attr_name, min_val, max_val]
            lista_espaciales: Lista de búsquedas espaciales [tipo, attr_name, center_point, param]
                            donde tipo es 'RADIUS' o 'KNN' y param es radio o k
            requested_attributes: Atributos solicitados
        """
        print("quee", lista_busquedas, lista_rangos, lista_espaciales, "gaa")
        if not lista_busquedas and not lista_rangos and not lista_espaciales:
            print("No hay condiciones WHERE - retornando todos los registros")
            all_records = self._get_all_active_record_numbers()
            return {
                "error": False,
                "numeros_registro": all_records,
                "requested_attributes": requested_attributes,
            }

        conjuntos_resultados = []
        errores = []

        # Procesar búsquedas exactas (código existente)
        if lista_busquedas:
            for i, (attr_name, valor) in enumerate(lista_busquedas):
                converted_value = self._convert_search_value(attr_name, valor)

                if attr_name in self.indices:
                    indice = self.indices[attr_name]
                    resultados = indice.search(converted_value)

                    if not resultados:
                        return {
                            "error": False,
                            "numeros_registro": [],
                            "requested_attributes": requested_attributes,
                        }

                    conjuntos_resultados.append(set(resultados))
                else:
                    error_msg = f"No existe índice para {attr_name}"
                    errores.append({"error": True, "message": error_msg, "type": "no_index"})

        if lista_rangos:
            for i, (attr_name, min_val, max_val) in enumerate(lista_rangos):
                converted_min = self._convert_search_value(attr_name, min_val)
                converted_max = self._convert_search_value(attr_name, max_val)

                if attr_name in self.indices:
                    indice = self.indices[attr_name]
                    resultado = indice.range_search(converted_min, converted_max)
                    print("aver resultado", resultado)
                    print(i, attr_name)
                    if isinstance(resultado, dict) and resultado.get("error", False):
                        errores.append(resultado)
                        continue

                    if not resultado:
                        return {
                            "error": False,
                            "numeros_registro": [],
                            "requested_attributes": requested_attributes,
                        }

                    conjuntos_resultados.append(set(resultado))
                else:
                    error_msg = f"No existe índice para {attr_name}"
                    errores.append({"error": True, "message": error_msg, "type": "no_index"})

        if lista_espaciales:
            for i, (tipo, attr_name, center_point, param) in enumerate(lista_espaciales):
                try:
                    if tipo.upper() == "RADIUS":
                        # Búsqueda por radio
                        radio = float(param)
                        resultados = self.spatial_radius_search(attr_name, center_point, radio)
                        print(f"Búsqueda radial: {attr_name} centro={center_point} radio={radio} → {len(resultados)} resultados")

                    elif tipo.upper() == "KNN":
                        # Búsqueda K vecinos más cercanos
                        k = int(param)
                        resultados = self.spatial_knn_search(attr_name, center_point, k)
                        print(f"Búsqueda KNN: {attr_name} centro={center_point} k={k} → {len(resultados)} resultados")

                    else:
                        error_msg = f"Tipo de búsqueda espacial '{tipo}' no soportado"
                        errores.append({"error": True, "message": error_msg, "type": "unsupported_spatial"})
                        continue

                    if not resultados:
                        return {
                            "error": False,
                            "numeros_registro": [],
                            "requested_attributes": requested_attributes,
                        }

                    conjuntos_resultados.append(set(resultados))

                except Exception as e:
                    error_msg = f"Error en búsqueda espacial {tipo} para {attr_name}: {str(e)}"
                    errores.append({"error": True, "message": error_msg, "type": "spatial_error"})

        if errores:
            return {
                "error": True,
                "errores": errores,
                "message": "Se encontraron errores en la consulta",
            }

        if not conjuntos_resultados:
            return {
                "error": True,
                "message": "No se pudieron procesar las búsquedas",
                "errores": [],
            }

        # Intersección de todos los conjuntos de resultados
        interseccion_final = conjuntos_resultados[0]
        for conjunto in conjuntos_resultados[1:]:
            interseccion_final = interseccion_final.intersection(conjunto)
            if not interseccion_final:
                return {
                    "error": False,
                    "numeros_registro": [],
                    "requested_attributes": requested_attributes,
                }

        resultado_final = list(interseccion_final)

        return {
            "error": False,
            "numeros_registro": resultado_final,
            "requested_attributes": requested_attributes,
        }

    def _convert_search_value(self, attr_name, value):
        """
        Convierte un valor de búsqueda al tipo apropiado según el atributo.

        Args:
            attr_name: Nombre del atributo
            value: Valor a convertir

        Returns:
            Valor convertido al tipo apropiado
        """
        # Buscar el tipo de dato del atributo
        data_type = None
        for attr in self.table_info["attributes"]:
            if attr["name"] == attr_name:
                data_type = attr["data_type"].upper()
                break

        if not data_type:
            return value

        if data_type == "POINT":
            # Convertir a objeto Point si no lo es ya
            if isinstance(value, Point):
                return value
            elif isinstance(value, (list, tuple)) and len(value) >= 2:
                return Point(float(value[0]), float(value[1]))
            elif isinstance(value, str):
                try:
                    return Point.from_string(value)
                except:
                    return Point(0.0, 0.0)
            elif isinstance(value, dict) and "x" in value and "y" in value:
                return Point(float(value["x"]), float(value["y"]))
            else:
                return Point(0.0, 0.0)
        else:
            # Para otros tipos, retornar el valor tal como viene
            return value

    def _get_all_active_record_numbers(self):
        """
        Obtiene todos los números de registro activos (no eliminados) en la tabla.

        Returns:
            list: Lista de números de registro activos
        """
        record_count = self._get_record_count()
        active_records = []

        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if record and record.get("next") == self.RECORD_NORMAL:
                active_records.append(i)

        # print(f"Encontrados {len(active_records)} registros activos: {active_records}")
        return active_records

    def get(self, id):
        """
        Obtiene un registro por su ID.

        Args:
            id: ID del registro a buscar

        Returns:
            Diccionario con los datos del registro o None si no se encontró
        """
        record = self._read_record(id)

        if not record or record["next"] != self.RECORD_NORMAL:
            return None

        # Eliminar el campo 'next' para devolver solo los atributos de la tabla
        del record["next"]
        return record

    def get_all_records(self):
        """Obtiene todos los registros no eliminados."""
        record_count = self._get_record_count()
        result = []

        for i in range(1, record_count + 1):
            record = self._read_record(i)
            if record and record["next"] == self.RECORD_NORMAL:
                record_copy = record.copy()
                del record_copy["next"]
                result.append(record_copy)

        return result
