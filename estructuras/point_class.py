import math


class Point:

    def __init__(self, x=0.0, y=0.0):

        self.x = float(x)
        self.y = float(y)

    def __str__(self):
        """Representación como string del punto."""
        return f"({self.x}, {self.y})"

    def __repr__(self):
        """Representación para debugging."""
        return f"Point({self.x}, {self.y})"

    def __lt__(self, other):
        """Menor que: basado en distancia al origen."""
        if not isinstance(other, Point):
            return NotImplemented
        return self.distance_to_origin() < other.distance_to_origin()

    def __le__(self, other):
        """Menor o igual que."""
        if not isinstance(other, Point):
            return NotImplemented
        return self.distance_to_origin() <= other.distance_to_origin()

    def __gt__(self, other):
        """Mayor que."""
        if not isinstance(other, Point):
            return NotImplemented
        return self.distance_to_origin() > other.distance_to_origin()

    def __ge__(self, other):
        """Mayor o igual que."""
        if not isinstance(other, Point):
            return NotImplemented
        return self.distance_to_origin() >= other.distance_to_origin()

    def __eq__(self, other):
        """Igualdad: ambas coordenadas deben ser iguales."""
        if not isinstance(other, Point):
            return NotImplemented
        return abs(self.x - other.x) < 1e-10 and abs(self.y - other.y) < 1e-10

    def __ne__(self, other):
        """Desigualdad."""
        return not self.__eq__(other)

    def __hash__(self):
        return hash((round(self.x, 10), round(self.y, 10)))

    def __add__(self, other):
        if not isinstance(other, Point):
            return NotImplemented
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other):
        if not isinstance(other, Point):
            return NotImplemented
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar):
        """Multiplicación por escalar."""
        if not isinstance(scalar, (int, float)):
            return NotImplemented
        return Point(self.x * scalar, self.y * scalar)

    def __rmul__(self, scalar):
        """Multiplicación por escalar (orden inverso)."""
        return self.__mul__(scalar)

    def __truediv__(self, scalar):
        """División por escalar."""
        if not isinstance(scalar, (int, float)):
            return NotImplemented
        if scalar == 0:
            raise ZeroDivisionError("No se puede dividir un punto por cero")
        return Point(self.x / scalar, self.y / scalar)

    def distance_to_origin(self):
        """Calcula la distancia del punto al origen (0, 0)."""
        return math.sqrt(self.x**2 + self.y**2)

    def distance_to(self, other):
        """Calcula la distancia euclidiana a otro punto."""
        if not isinstance(other, Point):
            raise TypeError("Se requiere otro objeto Point")
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def magnitude(self):
        """Alias para distance_to_origin."""
        return self.distance_to_origin()

    def normalize(self):
        """Retorna un punto normalizado (vector unitario)."""
        mag = self.magnitude()
        if mag == 0:
            return Point(0, 0)
        return Point(self.x / mag, self.y / mag)

    def dot_product(self, other):
        if not isinstance(other, Point):
            raise TypeError("Se requiere otro objeto Point")
        return self.x * other.x + self.y * other.y

    def cross_product_magnitude(self, other):
        """Magnitud del producto cruz (para puntos 2D)."""
        if not isinstance(other, Point):
            raise TypeError("Se requiere otro objeto Point")
        return abs(self.x * other.y - self.y * other.x)

    def rotate(self, angle_radians):
        cos_angle = math.cos(angle_radians)
        sin_angle = math.sin(angle_radians)
        new_x = self.x * cos_angle - self.y * sin_angle
        new_y = self.x * sin_angle + self.y * cos_angle
        return Point(new_x, new_y)

    def to_tuple(self):
        return (self.x, self.y)

    def to_list(self):
        return [self.x, self.y]

    @classmethod
    def from_string(cls, point_str):

        if not isinstance(point_str, str):
            raise ValueError("Se requiere un string")

        clean_str = point_str.strip().replace("(", "").replace(")", "")

        parts = None
        for delimiter in [",", " ", ";"]:
            if delimiter in clean_str:
                parts = clean_str.split(delimiter)
                break

        if parts is None or len(parts) != 2:
            raise ValueError(f"Formato de punto inválido: {point_str}. Use '(x, y)' o 'x, y'")

        try:
            x = float(parts[0].strip())
            y = float(parts[1].strip())
            return cls(x, y)
        except ValueError:
            raise ValueError(f"No se pudieron convertir las coordenadas a números: {point_str}")

    @classmethod
    def origin(cls):
        """Retorna el punto origen (0, 0)."""
        return cls(0.0, 0.0)

    @classmethod
    def unit_x(cls):
        """Retorna el vector unitario en X (1, 0)."""
        return cls(1.0, 0.0)

    @classmethod
    def unit_y(cls):
        """Retorna el vector unitario en Y (0, 1)."""
        return cls(0.0, 1.0)

    def is_in_range(self, min_point, max_point):

        if not isinstance(min_point, Point) or not isinstance(max_point, Point):
            raise TypeError("Se requieren objetos Point para min_point y max_point")

        return min_point.x <= self.x <= max_point.x and min_point.y <= self.y <= max_point.y

    def is_in_circle(self, center, radius):

        if not isinstance(center, Point):
            raise TypeError("Se requiere un objeto Point para center")

        return self.distance_to(center) <= radius
