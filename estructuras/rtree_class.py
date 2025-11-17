import struct
import os
import json
import math
from rtree import index
from estructuras.point_class import Point


class RTreeFile:
    """
    Índice espacial RTree para atributos de tipo Point usando librería rtree.
    Compatible con el sistema de índices existente (.dat files).

    Operaciones implementadas:
    1. rangeSearch(point, radio) - Búsqueda por radio
    2. rangeSearch(point, k) - K vecinos más cercanos
    3. Búsquedas rectangulares tradicionales
    """

    def __init__(self, record_format="<i50sdii", index_attr=2, table_name="Productos", is_key=False):
        self.record_format = record_format
        self.index_attr = index_attr  # El atributo a indexar (debe ser Point)
        self.table_name = table_name
        self.is_key = is_key  # Para Points normalmente False

        # Cargar metadata de la tabla
        self.table_metadata = self._load_table_metadata()

        # Configurar el formato del registro
        self.record_size = struct.calcsize(self.record_format)

        # Asegurar que los directorios existan
        os.makedirs("indices", exist_ok=True)

        # Configurar archivos del índice (compatible con el sistema .dat)
        self.index_filename = f"indices/{table_name}_{index_attr}_rtree"
        self.index_file_dat = f"{self.index_filename}.dat"
        self.index_file_idx = f"{self.index_filename}.idx"
        self.metadata_file = f"{self.index_filename}_meta.json"

        # Configurar propiedades del RTree
        p = index.Property()
        p.dimension = 2  # Espacial 2D para Points
        p.variant = index.RT_Star  # Usar R*-tree (más eficiente)
        p.buffering_capacity = 10  # Capacidad de buffer
        p.leaf_capacity = 100  # Capacidad de hojas
        p.fill_factor = 0.7  # Factor de llenado

        # Diccionario para mapear IDs a coordenadas (para operaciones avanzadas)
        self.id_to_point = {}

        # Inicializar el índice RTree
        self._initialize_rtree(p)

        print(f"📍 RTree inicializado: {self.index_filename}")

    def _initialize_rtree(self, properties):
        """Inicializa el índice RTree"""
        try:
            # Intentar cargar índice existente
            if os.path.exists(self.index_file_idx) and os.path.exists(self.index_file_dat):
                self.rtree_index = index.Index(self.index_filename, properties=properties)
                self._load_metadata()
            else:
                # Crear nuevo índice
                self.rtree_index = index.Index(self.index_filename, properties=properties)
                self.id_to_point = {}

        except Exception:
            # Crear índice en memoria como respaldo
            self.rtree_index = index.Index(properties=properties)
            self.id_to_point = {}

    def _load_table_metadata(self):
        """Carga los metadatos de la tabla desde el archivo _meta.json"""
        metadata_path = f"tablas/{self.table_name}_meta.json"

        try:
            if os.path.exists(metadata_path):
                with open(metadata_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            else:
                return None
        except Exception as e:
            print(f"Error al cargar metadatos: {e}")
            return None

    def _save_metadata(self):
        """Guarda el mapeo ID->Point para operaciones avanzadas"""
        try:
            metadata = {
                "table_name": self.table_name,
                "index_attr": self.index_attr,
                "id_to_point": {str(k): [v.x, v.y] for k, v in self.id_to_point.items()},
                "total_records": len(self.id_to_point),
                "version": "1.0",
            }

            with open(self.metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            return True

        except Exception as e:
            print(f"Error al guardar metadata RTree: {e}")
            return False

    def _load_metadata(self):
        """Carga el mapeo ID->Point desde metadata"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                # Reconstruir el mapeo ID->Point
                self.id_to_point = {}
                for id_str, coords in metadata.get("id_to_point", {}).items():
                    record_id = int(id_str)
                    point = Point(coords[0], coords[1])
                    self.id_to_point[record_id] = point

                return True

        except Exception as e:
            print(f"Error al cargar metadata RTree: {e}")

        self.id_to_point = {}
        return False

    def get_attribute_from_record_num(self, record_num):
        """
        Obtiene el valor Point del atributo indexado desde un número de registro.
        VERSIÓN OPTIMIZADA que usa cache primero.
        """
        # Primero intentar desde cache
        if record_num in self.id_to_point:
            return self.id_to_point[record_num]

        # Si no está en cache, leer desde archivo
        tabla_filename = f"tablas/{self.table_name}.bin"
        tabla_header_format = "<i"
        tabla_header_size = struct.calcsize(tabla_header_format)

        try:
            with open(tabla_filename, "rb") as f:
                position = tabla_header_size + (record_num - 1) * self.record_size
                f.seek(position)

                record_data = f.read(self.record_size)

                if not record_data or len(record_data) < self.record_size:
                    return None

                unpacked_data = list(struct.unpack(self.record_format, record_data))

                # Obtener el índice del atributo (convertir de base-1 a base-0)
                current_index = self.index_attr - 1

                if current_index < 0 or current_index >= len(unpacked_data):
                    return None

                # DETECCIÓN AUTOMÁTICA DE POINT
                if (
                    current_index + 1 < len(unpacked_data)
                    and isinstance(unpacked_data[current_index], (int, float))
                    and isinstance(unpacked_data[current_index + 1], (int, float))
                ):

                    x_value = float(unpacked_data[current_index])
                    y_value = float(unpacked_data[current_index + 1])

                    point = Point(x_value, y_value)

                    # Guardar en cache
                    self.id_to_point[record_num] = point

                    return point

                return None

        except FileNotFoundError:
            return None
        except Exception:
            return None

    def insert_record(self, record_num):
        """
        Inserta un registro en el R-Tree.

        Args:
            record_num (int): Número de registro a insertar

        Returns:
            bool: True si se insertó correctamente
        """
        try:
            # Obtener el Point del registro
            point = self.get_attribute_from_record_num(record_num)

            if not isinstance(point, Point):
                return False

            # Crear bounding box del punto (punto como rectángulo mínimo)
            bbox = (point.x, point.y, point.x, point.y)

            # Insertar en el RTree
            self.rtree_index.insert(record_num, bbox)

            # Actualizar cache
            self.id_to_point[record_num] = point

            return True

        except Exception as e:
            print(f"Error al insertar en RTree: {e}")
            return False

    def delete_record(self, record_num):
        """
        Elimina un registro del R-Tree.

        Args:
            record_num (int): Número de registro a eliminar

        Returns:
            int: record_num si se eliminó correctamente, None en caso contrario
        """
        try:
            # Obtener el Point del registro (desde cache o archivo)
            point = self.get_attribute_from_record_num(record_num)

            if not isinstance(point, Point):
                return None

            # Crear bounding box del punto
            bbox = (point.x, point.y, point.x, point.y)

            # Eliminar del RTree
            self.rtree_index.delete(record_num, bbox)

            # Eliminar del cache
            if record_num in self.id_to_point:
                del self.id_to_point[record_num]

            return record_num

        except Exception as e:
            print(f"Error al eliminar del RTree: {e}")
            return None

    def search(self, target_point):
        """
        Busca registros en la posición exacta del punto.

        Args:
            target_point (Point): Punto a buscar

        Returns:
            list: Lista de números de registro que coinciden exactamente
        """
        try:
            if not isinstance(target_point, Point):
                return []

            # Buscar registros que intersecten con el punto exacto
            bbox = (target_point.x, target_point.y, target_point.x, target_point.y)

            # Obtener candidatos del RTree
            candidates = list(self.rtree_index.intersection(bbox))

            # Verificar exactitud (filtro fino)
            exact_matches = []
            for record_num in candidates:
                point = self.get_attribute_from_record_num(record_num)
                if point and isinstance(point, Point):
                    if abs(point.x - target_point.x) < 1e-10 and abs(point.y - target_point.y) < 1e-10:
                        exact_matches.append(record_num)

            return exact_matches

        except Exception:
            return []

    def range_search(self, min_point, max_point):
        """
        Busca registros en un rango rectangular.

        Args:
            min_point (Point): Esquina inferior izquierda del rectángulo
            max_point (Point): Esquina superior derecha del rectángulo

        Returns:
            list: Lista de números de registro en el rango
        """
        try:
            if not isinstance(min_point, Point) or not isinstance(max_point, Point):
                return []

            # Crear bounding box del rango
            bbox = (min_point.x, min_point.y, max_point.x, max_point.y)

            # Buscar en el RTree
            candidates = list(self.rtree_index.intersection(bbox))

            # Verificar que estén realmente dentro del rango
            results = []
            for record_num in candidates:
                point = self.get_attribute_from_record_num(record_num)
                if point and isinstance(point, Point):
                    if point.is_in_range(min_point, max_point):
                        results.append(record_num)

            return results

        except Exception:
            return []

    def range_search_radius(self, center_point, radius):
        """
        OPERACIÓN REQUERIDA 1: Búsqueda por rango con radio.
        rangeSearch(point, radio) - Búsqueda por radio

        Args:
            center_point (Point): Punto central de la búsqueda
            radius (float): Radio de búsqueda

        Returns:
            list: Lista de números de registro dentro del radio
        """
        try:
            if not isinstance(center_point, Point):
                return []

            if radius <= 0:
                return []

            # Crear bounding box que contiene el círculo
            bbox = (
                center_point.x - radius,
                center_point.y - radius,
                center_point.x + radius,
                center_point.y + radius,
            )

            # Obtener candidatos del RTree (filtro grueso)
            candidates = list(self.rtree_index.intersection(bbox))

            # Filtrar por distancia real (círculo, no cuadrado) - filtro fino
            results = []
            for record_num in candidates:
                point = self.get_attribute_from_record_num(record_num)
                if point and isinstance(point, Point):
                    # Calcular distancia euclidiana
                    distance = math.sqrt((point.x - center_point.x) ** 2 + (point.y - center_point.y) ** 2)
                    if distance <= radius:
                        results.append(record_num)

            return results

        except Exception:
            return []

    def range_search_knn(self, center_point, k):
        """
        🌟 OPERACIÓN REQUERIDA 2: Búsqueda de K vecinos más cercanos.
        rangeSearch(point, k) - K vecinos más cercanos

        Args:
            center_point (Point): Punto central de la búsqueda
            k (int): Número de vecinos más cercanos a buscar

        Returns:
            list: Lista de tuplas (record_num, distance) ordenadas por distancia
        """
        try:
            if not isinstance(center_point, Point):
                return []

            if k <= 0:
                return []

            # Punto como coordenadas para la búsqueda KNN
            point_coords = (center_point.x, center_point.y)

            # Usar la funcionalidad nearest del RTree para obtener los k más cercanos
            nearest_ids = list(self.rtree_index.nearest(point_coords, k))

            # Calcular distancias y crear resultado
            results = []
            for record_num in nearest_ids:
                point = self.get_attribute_from_record_num(record_num)
                if point and isinstance(point, Point):
                    distance = math.sqrt((point.x - center_point.x) ** 2 + (point.y - center_point.y) ** 2)
                    results.append((record_num, distance))

            # Ordenar por distancia (aunque ya debería estar ordenado)
            results.sort(key=lambda x: x[1])

            # Limitar a k resultados
            results = results[:k]

            return results

        except Exception:
            return []

    def range_search_knn_simple(self, center_point, k):
        """
        Versión simplificada de KNN que retorna solo los IDs de registro.

        Args:
            center_point (Point): Punto central de la búsqueda
            k (int): Número de vecinos más cercanos

        Returns:
            list: Lista de números de registro ordenados por distancia
        """
        knn_results = self.range_search_knn(center_point, k)
        return [record_num for record_num, distance in knn_results]

    def finalize(self):
        """
        Finaliza el índice guardando metadatos y cerrando archivos.
        Compatible con el sistema de tabla.py
        """
        try:
            # Guardar metadatos
            self._save_metadata()

            # Cerrar el índice RTree (se guarda automáticamente)
            if hasattr(self, "rtree_index") and self.rtree_index:
                self.rtree_index.close()

            return True

        except Exception:
            return False

    def close(self):
        """Alias para finalize() - compatibilidad"""
        return self.finalize()

    def __del__(self):
        """Destructor para asegurar que el índice se cierre correctamente."""
        try:
            if hasattr(self, "rtree_index") and self.rtree_index:
                self.rtree_index.close()
        except Exception:
            pass

    def get_stats(self):
        """
        Obtiene estadísticas del índice RTree.

        Returns:
            dict: Estadísticas del índice
        """
        try:
            # Contar registros en el cache
            total_records = len(self.id_to_point)

            # Calcular bounding box general
            if self.id_to_point:
                all_points = list(self.id_to_point.values())
                min_x = min(p.x for p in all_points)
                max_x = max(p.x for p in all_points)
                min_y = min(p.y for p in all_points)
                max_y = max(p.y for p in all_points)

                bounding_box = {
                    "min_x": min_x,
                    "max_x": max_x,
                    "min_y": min_y,
                    "max_y": max_y,
                    "width": max_x - min_x,
                    "height": max_y - min_y,
                }
            else:
                bounding_box = None

            stats = {
                "total_records": total_records,
                "bounding_box": bounding_box,
                "index_files": {
                    "dat": self.index_file_dat,
                    "idx": self.index_file_idx,
                    "meta": self.metadata_file,
                },
                "table_name": self.table_name,
                "indexed_attribute": self.index_attr,
                "index_type": "R-Tree (R* variant)",
                "operations_supported": [
                    "exact_search",
                    "range_search",
                    "radius_search",
                    "knn_search",
                ],
            }

            return stats

        except Exception as e:
            return {"error": str(e)}

    def rebuild_index(self):
        """
        Reconstruye el índice desde cero leyendo todos los registros de la tabla.
        """
        try:

            # Limpiar cache
            self.id_to_point = {}

            # Leer tabla y reconstruir índice
            tabla_filename = f"tablas/{self.table_name}.bin"
            tabla_header_format = "<i"
            tabla_header_size = struct.calcsize(tabla_header_format)

            with open(tabla_filename, "rb") as f:
                # Determinar número de registros
                file_size = os.path.getsize(tabla_filename)
                num_records = (file_size - tabla_header_size) // self.record_size

                rebuilt_count = 0

                # Insertar cada registro válido en el índice
                for record_num in range(1, num_records + 1):
                    # Leer registro para verificar que existe y no está eliminado
                    position = tabla_header_size + (record_num - 1) * self.record_size
                    f.seek(position)
                    record_data = f.read(self.record_size)

                    if record_data and len(record_data) == self.record_size:
                        # Verificar que no esté eliminado (next != -2)
                        unpacked = struct.unpack(self.record_format, record_data)
                        next_value = unpacked[-1]  # Último valor es 'next'

                        if next_value == -2:  # Registro activo
                            if self.insert_record(record_num):
                                rebuilt_count += 1

                # Guardar metadatos
                self._save_metadata()

                return True

        except FileNotFoundError:
            return False
        except Exception:
            return False
