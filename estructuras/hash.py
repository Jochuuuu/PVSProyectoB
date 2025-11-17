import struct
import os
import re
import json
from estructuras.point_class import Point

FB = 5
D = 5
BUCKETS_INICIAL = 2**D


class Bucket:
    def __init__(self, records=None, next=-1):
        self.records = records or []
        self.next = next

    def is_full(self):
        return len(self.records) >= FB

    def to_bytes(self):
        data = self.records + [-1] * (FB - len(self.records)) + [self.next]
        fmt = f"{FB + 1}i"
        return struct.pack(fmt, *data)

    @staticmethod
    def from_bytes(data):
        fmt = f"{FB + 1}i"
        values = list(struct.unpack(fmt, data))
        records = [x for x in values[:FB] if x != -1]
        next_ptr = values[FB]
        return Bucket(records, next_ptr)


class HashIndexEntry:
    def __init__(self, prefix, bucket_id):
        self.prefix = prefix  # 0, 01, 11
        self.bucket_id = bucket_id


class ExtendibleHashFile:
    def __init__(self, record_format="<i50sdii", index_attr=2, table_name="Productos", is_key=False):
        self.record_format = record_format
        self.index_attr = index_attr
        self.table_name = table_name
        self.is_key = is_key  # Indica si el atributo es una clave (no permite duplicados)
        self.filename = f"tablas/{table_name}.bin"

        # Cargar metadata de la tabla para obtener información de tipos
        self.table_metadata = self._load_table_metadata()

        # Analizar el formato para determinar los tipos de campo
        self.field_types = self._parse_format(record_format)

        # Configurar el formato del registro
        self.record_size = struct.calcsize(self.record_format)

        # Configurar cabecera
        self.header_format = "<i"  # 4 bytes para la cabecera (int)
        self.header_size = struct.calcsize(self.header_format)

        # Configuración estándar del hash
        self.bucket_size = struct.calcsize(f"{FB + 1}i")
        self.index = []  # esto se cargará desde hash_index.dat

        # Asegurar que los directorios existan
        os.makedirs("tablas", exist_ok=True)
        os.makedirs("indices", exist_ok=True)

        # Nombres de archivos de índice
        self.index_file = f"indices/{table_name}_{index_attr}_index.dat"
        self.buckets_file = f"indices/{table_name}_{index_attr}_buckets.dat"

        self.init_files()

    def _load_table_metadata(self):
        """
        Carga los metadatos de la tabla desde el archivo _meta.json

        Returns:
            dict: Metadatos de la tabla o None si no se puede cargar
        """
        metadata_path = f"tablas/{self.table_name}_meta.json"

        try:
            if os.path.exists(metadata_path):
                with open(metadata_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            else:
                print(f"Advertencia: No se encontró el archivo de metadatos {metadata_path}")
                return None
        except Exception as e:
            print(f"Error al cargar metadatos: {e}")
            return None

    def _get_attribute_type(self, attr_index):
        """
        Obtiene el tipo de dato del atributo según los metadatos.

        Args:
            attr_index (int): Índice del atributo (empezando desde 1)

        Returns:
            str: Tipo de dato del atributo
        """
        if not self.table_metadata or "attributes" not in self.table_metadata:
            return "UNKNOWN"

        attributes = self.table_metadata["attributes"]

        # attr_index empieza desde 1, pero el array desde 0
        if 1 <= attr_index <= len(attributes):
            return attributes[attr_index - 1]["data_type"].upper()

        return "UNKNOWN"

    def _parse_format(self, format_str):
        """Analiza el formato del registro para determinar los tipos de cada campo."""
        # Patrón para extraer los tipos de campos del formato
        pattern = r"([<>]?)([a-zA-Z])(\d*)"
        matches = re.findall(pattern, format_str)

        field_types = []
        for _, type_char, size_str in matches:
            if type_char == "i":
                field_types.append("int")
            elif type_char == "d":
                field_types.append("double")
            elif type_char == "s":
                field_types.append("string")
            elif type_char == "c":
                field_types.append("char")
            else:
                field_types.append("unknown")

        return field_types

    def _get_record_position(self, record_num):
        """
        Calcula la posición en bytes del registro en el archivo basado en su número.
        Asume que los números de registro son consecutivos empezando desde 1.
        """
        return self.header_size + (record_num - 1) * self.record_size

    def get_attribute_from_record_num(self, record_num):
        """
        Obtiene el valor del atributo indexado desde un número de registro.
        VERSIÓN CORREGIDA que maneja strings VARCHAR correctamente.
        """
        position = self._get_record_position(record_num)
        return self.get_attribute_from_position(position)

    # =============================================================================
    # MÉTODO 2: get_attribute_from_position (línea aproximada 200-300)
    # =============================================================================

    def get_attribute_from_position(self, position):
        """
        Obtiene el valor del atributo indexado desde una posición en bytes.
        VERSIÓN CORREGIDA que maneja strings VARCHAR y Point correctamente.
        """
        try:
            with open(self.filename, "rb") as f:
                f.seek(position)
                record_data = f.read(self.record_size)

                if not record_data or len(record_data) < self.record_size:
                    return None

                # Desempaquetar los datos usando el formato
                unpacked_data = list(struct.unpack(self.record_format, record_data))

                # Obtener el valor del atributo indexado
                if self.index_attr < 1 or self.index_attr > len(unpacked_data):
                    return None

                current_index = self.index_attr - 1
                raw_value = unpacked_data[current_index]

                # DETECCIÓN INTELIGENTE DE TIPOS:

                # CASO 1: POINT - Dos números consecutivos
                if (
                    current_index + 1 < len(unpacked_data)
                    and isinstance(raw_value, (int, float))
                    and isinstance(unpacked_data[current_index + 1], (int, float))
                ):

                    x_value = float(raw_value)
                    y_value = float(unpacked_data[current_index + 1])
                    point = Point(x_value, y_value)
                    return point

                elif isinstance(raw_value, bytes):
                    decoded_value = raw_value.decode("utf-8").rstrip("\x00").strip()
                    return decoded_value

                else:
                    return raw_value

        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"Error al leer atributo desde posición {position}: {e}")
            return None

    def hash_bin(self, value):
        """
        Genera el hash binario para un valor según su tipo.
        VERSIÓN ACTUALIZADA que maneja objetos Point.
        """
        # Obtener el tipo de dato del atributo usando metadatos
        attribute_type = self._get_attribute_type(self.index_attr)

        if attribute_type == "POINT":
            if isinstance(value, Point):
                distance = value.distance_to_origin()
                hash_val = int(distance * 1000) % (2**D)
            else:
                hash_val = 0
        else:
            field_type = self.field_types[self.index_attr - 1] if self.index_attr - 1 < len(self.field_types) else "unknown"

            if field_type == "int":
                hash_val = value % (2**D)
            elif field_type == "double":
                hash_val = int(value * 1000) % (2**D)
            elif field_type == "string" or field_type == "char":
                hash_val = sum(ord(c) for c in str(value)) % (2**D)
            else:
                hash_val = 0

        return bin(hash_val)[2:].zfill(D)

    def init_files(self):
        """Inicializa los archivos de índice si no existen."""
        # Verificar si los archivos ya existen
        if os.path.exists(self.index_file) and os.path.exists(self.buckets_file):
            self.load_index()
            return

        # Crear directorio de índices si no existe
        os.makedirs(os.path.dirname(self.index_file), exist_ok=True)

        # inicia con: "0" y "1"
        with open(self.index_file, "w") as f:
            f.write("0 0\n")
            f.write("1 1\n")

        # buckets.dat reserva los primeros 2^D (vacíos)
        with open(self.buckets_file, "wb") as f:
            for _ in range(2**D):
                f.write(Bucket().to_bytes())

        self.load_index()

    def load_index(self):
        """Carga el índice desde el archivo."""
        self.index = []
        with open(self.index_file, "r") as f:
            for line in f:
                if line.strip():
                    prefix, bucket = line.strip().split()
                    self.index.append(HashIndexEntry(prefix, int(bucket)))

    def save_index(self):
        """Guarda el índice en el archivo."""
        with open(self.index_file, "w") as f:
            for entry in self.index:
                f.write(f"{entry.prefix} {entry.bucket_id}\n")

    def insert_record(self, record_num):
        """
        Inserta un número de registro en el índice hash.
        El número de registro debe ser válido y existir en el archivo de datos.
        """
        # Obtener el valor del atributo indexado
        value = self.get_attribute_from_record_num(record_num)
        if value is None:
            print(f"Error: No se pudo obtener el valor del atributo del registro {record_num}")
            return False

        # Si es una clave, verificar que no exista otro registro con el mismo valor
        if self.is_key:
            existing_records = self.search(value)
            if existing_records:
                existing_value = self.get_attribute_from_record_num(existing_records[0])
                print(f"Error: Ya existe un registro con el valor '{existing_value}' (es clave única)")
                return False

        # Calcular el hash binario
        hbin = self.hash_bin(value)

        self.load_index()

        # Empezamos con el hash completo y vamos recortando un bit a la vez desde la izquierda
        matching = None
        b = hbin  # Comenzamos con el hash completo

        # Recortar el binario desde el primer bit
        while b:
            matching = next((e for e in self.index if e.prefix == b), None)
            if matching:
                break  # Salir si encontramos una coincidencia
            b = b[1:]  # Recortamos el primer bit del prefijo

        if matching:
            bucket_id = matching.bucket_id

            # Leer el bucket desde archivo
            bucket = self.read_bucket(bucket_id)

            # Verificar si el registro ya existe en el bucket o sus overflow
            current_id = bucket_id
            while current_id != -1:
                current_bucket = self.read_bucket(current_id)
                if record_num in current_bucket.records:
                    print(f"El registro {record_num} ya existe en el bucket {current_id}.")
                    return False
                current_id = current_bucket.next

            # Insertar si hay espacio
            if not bucket.is_full():
                bucket.records.append(record_num)
                self.write_bucket(bucket_id, bucket)
                return True

            # Si el bucket está lleno, verificar si se puede hacer un split
            if len(matching.prefix) < D:
                # SPLIT
                new_prefix0 = "0" + matching.prefix
                new_prefix1 = "1" + matching.prefix
                new_id0 = int(new_prefix0, 2) if new_prefix0 else 0
                new_id1 = int(new_prefix1, 2) if new_prefix1 else 1

                # Crear nuevos buckets vacíos
                self.write_bucket(new_id0, Bucket())
                self.write_bucket(new_id1, Bucket())

                # Redistribuir los registros
                all_records = bucket.records + [record_num]
                self.index.remove(matching)
                self.index.append(HashIndexEntry(new_prefix0, new_id0))
                self.index.append(HashIndexEntry(new_prefix1, new_id1))

                # Distribuir los registros entre los nuevos buckets
                for rec in all_records:
                    self._distribute_record(rec)

                self.save_index()
                return True

            else:
                # Manejo de overflow
                overflow_id = self.get_next_available_bucket_id()
                while bucket.next != -1:
                    bucket_id = bucket.next
                    bucket = self.read_bucket(bucket_id)
                    if not bucket.is_full():
                        bucket.records.append(record_num)
                        self.write_bucket(bucket_id, bucket)
                        return True
                bucket.next = overflow_id
                self.write_bucket(bucket_id, bucket)
                self.write_bucket(overflow_id, Bucket([record_num]))
                return True

        else:
            print(f"No se encontró un prefijo coincidente para el hash {hbin}.")
            return False

    def _distribute_record(self, record_num):
        """Distribuye un registro entre los buckets correspondientes."""
        # Obtener el valor del atributo indexado
        value = self.get_attribute_from_record_num(record_num)
        if value is None:
            print(f"Error: No se pudo obtener el valor del atributo del registro {record_num}")
            return

        # Calcular el hash binario
        hbin = self.hash_bin(value)

        # Buscar el prefijo correspondiente
        matching = None
        b = hbin
        while b:
            matching = next((e for e in self.index if e.prefix == b), None)
            if matching:
                break
            b = b[1:]  # Recortamos el primer bit del prefijo

        if matching:
            bucket_id = matching.bucket_id
            bucket = self.read_bucket(bucket_id)

            if not bucket.is_full():
                bucket.records.append(record_num)
                self.write_bucket(bucket_id, bucket)
            else:
                # Si el bucket está lleno, manejamos el overflow
                overflow_id = self.get_next_available_bucket_id()
                bucket.next = overflow_id
                self.write_bucket(bucket_id, bucket)
                self.write_bucket(overflow_id, Bucket([record_num]))

    def search(self, search_value):
        """
        Busca registros que coincidan con el valor de búsqueda en el atributo indexado.
        VERSIÓN ACTUALIZADA que maneja búsquedas con objetos Point.
        Retorna los números de registro que coinciden.
        """
        self.load_index()
        hbin = self.hash_bin(search_value)

        # Lista para almacenar los registros encontrados
        found_records = []

        # Buscar el prefijo coincidente
        matching = None
        b = hbin
        while b:
            matching = next((e for e in self.index if e.prefix == b), None)
            if matching:
                break
            b = b[1:]

        # Buscar en los buckets
        if matching:
            bucket_id = matching.bucket_id

            # Recorrer el bucket junto con su overflow
            while bucket_id != -1:
                bucket = self.read_bucket(bucket_id)

                # Para cada registro en el bucket, verificar si el valor coincide
                for record_num in bucket.records:
                    value = self.get_attribute_from_record_num(record_num)

                    # Usar las operaciones sobrecargadas para comparar
                    try:
                        if value == search_value:
                            found_records.append(record_num)

                            # Si es una clave y ya encontramos un registro, podemos salir
                            if self.is_key and found_records:
                                return found_records
                    except TypeError:
                        # Si la comparación falla, convertir a string y comparar
                        if str(value) == str(search_value):
                            found_records.append(record_num)
                            if self.is_key and found_records:
                                return found_records

                bucket_id = bucket.next  # Seguir al siguiente bucket si hay overflow

        return found_records  # Devolvemos los números de registro

    def range_search(self, min_value, max_value):
        """
        Busca registros en un rango de valores.
        Los índices hash NO soportan búsquedas por rango eficientes.

        Args:
            min_value: Valor mínimo del rango (inclusive)
            max_value: Valor máximo del rango (inclusive)

        Returns:
            dict: Diccionario con error indicando que no se soporta
        """
        # Los índices hash no soportan búsquedas por rango eficientes
        error_response = {
            "error": True,
            "message": f"El indice hash no soporta búsquedas por rango. "
            f"Rango solicitado: [{min_value}, {max_value}] en atributo índice {self.index_attr}",
            "index_type": "hash",
            "attribute": self.index_attr,
            "table": self.table_name,
            "range": [min_value, max_value],
        }

        return error_response

    def delete_record(self, record_num):
        """
        Elimina un registro específico del índice hash.
        Retorna el número de registro eliminado si tuvo éxito, o None en caso contrario.
        """
        # Obtener el valor del atributo indexado
        value = self.get_attribute_from_record_num(record_num)
        if value is None:
            print(f"Error: No se pudo obtener el valor del atributo del registro {record_num}")
            return None

        # Calcular el hash binario
        hbin = self.hash_bin(value)

        self.load_index()

        # Buscar el prefijo coincidente
        matching = None
        b = hbin
        while b:
            matching = next((e for e in self.index if e.prefix == b), None)
            if matching:
                break
            b = b[1:]

        if matching:
            bucket_id = matching.bucket_id

            # Leer el bucket desde archivo
            bucket = self.read_bucket(bucket_id)

            # Eliminar el registro si existe en el bucket
            if record_num in bucket.records:
                bucket.records.remove(record_num)
                self.write_bucket(bucket_id, bucket)

                # Reorganizar overflow si es necesario
                bucket_next_t = bucket.next
                while bucket_next_t != -1:
                    actual_bucket = self.read_bucket(bucket_next_t)
                    if actual_bucket.records:
                        bucket.records.append(actual_bucket.records.pop(0))
                        self.write_bucket(bucket_next_t, actual_bucket)
                        self.write_bucket(bucket_id, bucket)
                    bucket_id = bucket_next_t
                    bucket = self.read_bucket(bucket_id)
                    bucket_next_t = actual_bucket.next

                # Retornar el número de registro eliminado
                return record_num
            else:
                # Buscar en los buckets de overflow
                prev_bucket_id = bucket_id
                current_bucket_id = bucket.next

                while current_bucket_id != -1:
                    current_bucket = self.read_bucket(current_bucket_id)
                    if record_num in current_bucket.records:
                        current_bucket.records.remove(record_num)
                        self.write_bucket(current_bucket_id, current_bucket)

                        # Si el bucket de overflow quedó vacío, actualizar enlaces
                        if not current_bucket.records:
                            prev_bucket = self.read_bucket(prev_bucket_id)
                            prev_bucket.next = current_bucket.next
                            self.write_bucket(prev_bucket_id, prev_bucket)

                        # Retornar el número de registro eliminado
                        return record_num

                    prev_bucket_id = current_bucket_id
                    current_bucket_id = current_bucket.next

                print(f"El registro {record_num} no se encontró en ningún bucket.")
                return None
        else:
            print(f"No se encontró un prefijo coincidente para el hash {hbin}.")
            return None

    def get_next_available_bucket_id(self):
        """Obtiene el siguiente ID de bucket disponible."""
        size = os.path.getsize(self.buckets_file)
        return size // self.bucket_size

    def read_bucket(self, bucket_id):
        """Lee un bucket desde el archivo."""
        with open(self.buckets_file, "rb") as f:
            f.seek(bucket_id * self.bucket_size)
            data = f.read(self.bucket_size)
            if not data or len(data) < self.bucket_size:
                return Bucket()
            return Bucket.from_bytes(data)

    def write_bucket(self, bucket_id, bucket):
        """Escribe un bucket en el archivo."""
        with open(self.buckets_file, "r+b") as f:
            f.seek(bucket_id * self.bucket_size)
            f.write(bucket.to_bytes())
