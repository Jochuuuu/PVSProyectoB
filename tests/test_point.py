from estructuras.point_class import Point
import math

def test_point_basics():
    p = Point(3,4)
    assert p.distance_to_origin() == 5
    assert p + Point(1,1) == Point(4,5)
    assert Point.from_string("(2, 3)") == Point(2,3)

    assert p * 2 == Point(6, 8)
    assert str(p) == "(3.0, 4.0)"
    assert Point.origin() == Point(0, 0)



 
def test_is_in_range():
    p = Point(3, 4)
    min_p = Point(0, 0)
    max_p = Point(5, 5)
    
    assert p.is_in_range(min_p, max_p) == True
    
    p_outside = Point(6, 7)
    assert p_outside.is_in_range(min_p, max_p) == False
    
    p_edge = Point(5, 5)
    assert p_edge.is_in_range(min_p, max_p) == True

def test_is_in_circle():
    p = Point(3, 4)
    center = Point(0, 0)
    
    assert p.is_in_circle(center, 6) == True
    
    assert p.is_in_circle(center, 4) == False
    
    assert p.is_in_circle(center, 5) == True


def test_rotate():
    p = Point(1, 0)
    rotated = p.rotate(math.pi / 2)
    
    assert abs(rotated.x - 0) < 1e-10
    assert abs(rotated.y - 1) < 1e-10
    
    p2 = Point(3, 4)
    rotated_180 = p2.rotate(math.pi)
    
    assert abs(rotated_180.x - (-3)) < 1e-10
    assert abs(rotated_180.y - (-4)) < 1e-10
    
    p3 = Point(2, 5)
    rotated_0 = p3.rotate(0)
    
    assert rotated_0.x == p3.x
    assert rotated_0.y == p3.y



def test_normalize():
    # Vector (3,4) tiene magnitud 5, normalizado debe ser (0.6, 0.8)
    p = Point(3, 4)
    normalized = p.normalize()
    
    assert abs(normalized.x - 0.6) < 1e-10
    assert abs(normalized.y - 0.8) < 1e-10
    assert abs(normalized.magnitude() - 1.0) < 1e-10  # debe tener magnitud 1
    
    zero = Point(0, 0)
    normalized_zero = zero.normalize()
    assert normalized_zero.x == 0
    assert normalized_zero.y == 0

def test_dot_product():
    p1 = Point(3, 4)
    p2 = Point(1, 2)
    
    assert p1.dot_product(p2) == 11
    
    v1 = Point(1, 0)
    v2 = Point(0, 1)
    assert v1.dot_product(v2) == 0

def test_cross_product_magnitude():
    p1 = Point(3, 4)
    p2 = Point(1, 2)
    
    assert p1.cross_product_magnitude(p2) == 2
    
    v1 = Point(2, 3)
    v2 = Point(4, 6)
    assert v1.cross_product_magnitude(v2) == 0



def test_repr_and_comparisons():
    # Test __repr__
    p = Point(3.5, 4.2)
    assert repr(p) == "Point(3.5, 4.2)"
    
    p1 = Point(3, 4)    # distancia = 5
    p2 = Point(0, 5)    # distancia = 5  
    p3 = Point(6, 8)    # distancia = 10
    p4 = Point(1, 1)    # distancia ≈ 1.41
    
    # __lt__ (menor que)
    assert p4 < p1      # 1.41 < 5
    assert not (p1 < p2)  # 5 no es < 5
    
    # __le__ (menor o igual)
    assert p4 <= p1     # 1.41 <= 5
    assert p1 <= p2     # 5 <= 5
    
    # __gt__ (mayor que)
    assert p3 > p1      # 10 > 5
    assert not (p1 > p2)  # 5 no es > 5
    
    # __ge__ (mayor o igual)
    assert p3 >= p1     # 10 >= 5
    assert p1 >= p2     # 5 >= 5
    
    # Test con tipos no Point (debe retornar NotImplemented)
    # Esto no causará error, Python manejará el NotImplemented
    result = p1.__lt__("not a point")
    assert result is NotImplemented

 

def test_arithmetic_operations():
    p = Point(6, 8)
    
    # __mul__ (multiplicación normal)
    assert p * 2 == Point(12, 16)
    assert p * 0.5 == Point(3, 4)
    
    # __rmul__ (multiplicación inversa: escalar * punto)
    assert 2 * p == Point(12, 16)
    assert 0.5 * p == Point(3, 4)
    
    # __truediv__ (división)
    assert p / 2 == Point(3, 4)
    assert p / 0.5 == Point(12, 16)
    
    # __sub__ (resta)
    p1 = Point(5, 7)
    p2 = Point(2, 3)
    assert p1 - p2 == Point(3, 4)
    
    # Test casos NotImplemented
    # __mul__ con tipo inválido
    result = p.__mul__("invalid")
    assert result is NotImplemented
    
    # __truediv__ con tipo inválido  
    result = p.__truediv__("invalid")
    assert result is NotImplemented
    
    # __sub__ con tipo inválido
    result = p.__sub__("invalid")
    assert result is NotImplemented
 

def test_truediv():
    p = Point(6, 8)
    
    # División normal por entero
    result = p / 2
    assert result == Point(3, 4)
    
    # División normal por float
    result = p / 2.0
    assert result == Point(3.0, 4.0)
    
    # Test NotImplemented - cuando no es int ni float
    result = p.__truediv__("not a number")
    assert result is NotImplemented
    
    result = p.__truediv__(None)
    assert result is NotImplemented
    
    result = p.__truediv__([1, 2])
    assert result is NotImplemented
    
    # Test ZeroDivisionError usando try/except
    try:
        p / 0
        assert False, "Debería haber lanzado ZeroDivisionError"
    except ZeroDivisionError as e:
        assert "No se puede dividir un punto por cero" in str(e)
    
    try:
        p / 0.0
        assert False, "Debería haber lanzado ZeroDivisionError"
    except ZeroDivisionError as e:
        assert "No se puede dividir un punto por cero" in str(e)


def test_uncovered_lines():
    p = Point(3, 4)
    
    # Cubrir __gt__ NotImplemented (línea roja)
    result = p.__gt__("not a point")
    assert result is NotImplemented
    
    # Cubrir __ge__ NotImplemented (línea roja)  
    result = p.__ge__("not a point")
    assert result is NotImplemented
    
    # Cubrir __eq__ NotImplemented (línea roja)
    result = p.__eq__("not a point")
    assert result is NotImplemented
    
    # Cubrir __ne__ (línea roja) - este usa __eq__ internamente
    result = p.__ne__(Point(1, 2))  # diferentes puntos
    assert result == True
    
    # Cubrir __hash__ (línea roja)
    p1 = Point(3.0, 4.0)
    p2 = Point(3.0, 4.0)
    # Verificar que puntos iguales tienen mismo hash
    assert hash(p1) == hash(p2)
    # Usar en un set para activar __hash__
    point_set = {p1, p2}
    assert len(point_set) == 1  # Solo un punto porque son iguales
    
    # Cubrir __add__ NotImplemented (línea roja)
    result = p.__add__("not a point")
    assert result is NotImplemented

 
def test_remaining_uncovered_lines():
    """Test para cubrir las 4 líneas rojas que faltan"""
    
    # 1. Cubrir distance_to() con TypeError  
    p = Point(3, 4)
    try:
        p.distance_to("not a point")  # Esto debe lanzar TypeError
        assert False, "Debería haber lanzado TypeError"
    except TypeError as e:
        assert "Se requiere otro objeto Point" in str(e)
    
    # 2. Cubrir dot_product() con TypeError 
    try:
        p.dot_product("not a point")  # Esto debe lanzar TypeError
        assert False, "Debería haber lanzado TypeError"
    except TypeError as e:
        assert "Se requiere otro objeto Point" in str(e)
    
    # 3. Cubrir cross_product_magnitude() con TypeError  
    try:
        p.cross_product_magnitude("not a point")  # Esto debe lanzar TypeError
        assert False, "Debería haber lanzado TypeError"
    except TypeError as e:
        assert "Se requiere otro objeto Point" in str(e)
    
    # 4. Cubrir to_tuple() 
    p = Point(3, 4)
    tuple_result = p.to_tuple()
    assert tuple_result == (3.0, 4.0)
    
    # 5. Cubrir to_list()  
    list_result = p.to_list()
    assert list_result == [3.0, 4.0]

