from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
import os
import time
import json
from sql import SQLTableManager
from tabla import TableStorageManager
from estructuras.point_class import Point


class SQLRequest(BaseModel):
    sql: str


app = FastAPI(title="BD2", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sql_manager = None


def point_serializer(obj):
    """
    Serializador personalizado para objetos Point y otros tipos no serializables por defecto.
    """
    if isinstance(obj, Point):
        return {"type": "POINT", "x": obj.x, "y": obj.y, "string_representation": str(obj)}
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    else:
        return str(obj)


def serialize_records_data(records_data):
    """
    Serializa una lista de registros, manejando objetos Point correctamente.

    Args:
        records_data: Lista de registros (diccionarios)

    Returns:
        Lista de registros serializados
    """
    serialized_records = []

    for record in records_data:
        serialized_record = {}
        for key, value in record.items():
            if isinstance(value, Point):
                serialized_record[key] = point_serializer(value)
            else:
                serialized_record[key] = value
        serialized_records.append(serialized_record)

    return serialized_records


@app.on_event("startup")
async def startup():
    """Inicializa el sistema al arrancar"""
    global sql_manager
    os.makedirs("tablas", exist_ok=True)
    os.makedirs("indices", exist_ok=True)
    sql_manager = SQLTableManager(storage_class=TableStorageManager, base_dir="tablas")


@app.get("/")
async def root():
    """Info básica de la API"""
    return {"message": "BD2 ", "status": "active", "docs": "/docs"}


@app.post("/sql")
async def execute_sql(request: SQLRequest):
    """
    Ejecuta instrucciones SQL con soporte completo para tipo POINT.
    """

    if not sql_manager:
        raise HTTPException(status_code=500, detail="Sistema no inicializado")

    try:
        start_time = time.time()

        operations = sql_manager.execute_sql(request.sql)

        execution_time = time.time() - start_time

        result = {
            "sql": request.sql,
            "execution_time": round(execution_time, 4),
            "success": True,
            "results": [],
        }

        for op_type, op_result in operations:
            if op_type == "CREATE":
                result["results"].append(
                    {
                        "operation": "CREATE",
                        "table_created": op_result,
                        "message": f"Tabla '{op_result}' creada exitosamente",
                    }
                )

            elif op_type == "INSERT":
                if isinstance(op_result, dict):
                    if op_result.get("error", False):
                        # Caso de error en INSERT
                        result["results"].append(
                            {
                                "operation": "INSERT",
                                "error": True,
                                "message": op_result.get("message", "Error en INSERT"),
                                "details": op_result.get("details", ""),
                                "failed_record": op_result.get("failed_record", {}),
                                "record_index": op_result.get("record_index", 0),
                            }
                        )
                    elif "records" in op_result:
                        # Caso exitoso
                        inserted_count = len(op_result["records"])
                        inserted_records = serialize_records_data(op_result["records"])

                        result["results"].append(
                            {
                                "operation": "INSERT",
                                "records_inserted": inserted_count,
                                "inserted_ids": op_result.get("inserted_ids", []),
                                "inserted_records": inserted_records,
                                "message": f"{inserted_count} registro(s) insertado(s) exitosamente",
                            }
                        )
                    else:
                        # Caso donde no hay 'records' ni 'error'
                        result["results"].append(
                            {
                                "operation": "INSERT",
                                "error": True,
                                "message": "Respuesta de INSERT inválida",
                            }
                        )
            elif op_type == "SELECT":
                if not op_result.get("error", False):
                    resultado = op_result.get("resultado", {})
                    if not resultado.get("error", False):
                        found_records = resultado.get("numeros_registro", [])
                        table_name = op_result.get("table_name", "")
                        requested_attributes = resultado.get("requested_attributes", [])

                        records_data = []
                        if found_records and table_name:
                            storage_manager = sql_manager.get_storage_manager(table_name)
                            if storage_manager:
                                for record_num in found_records:
                                    record = storage_manager.get(record_num)
                                    if record:
                                        if requested_attributes:
                                            filtered_record = {k: v for k, v in record.items() if k in requested_attributes}
                                            # records_data.append({"record_id": record_num, **filtered_record})
                                            records_data.append(filtered_record)
                                        else:
                                            records_data.append(record)

                        serialized_records = serialize_records_data(records_data)

                        result["results"].append(
                            {
                                "operation": "SELECT",
                                "table": table_name,
                                "records_found": len(found_records),
                                "records": serialized_records,
                                "message": f"Se encontraron {len(found_records)} registro(s)",
                            }
                        )
                    else:
                        result["results"].append(
                            {
                                "operation": "SELECT",
                                "error": True,
                                "message": resultado.get("message", "Error en SELECT"),
                            }
                        )
                else:
                    result["results"].append(
                        {
                            "operation": "SELECT",
                            "error": True,
                            "message": op_result.get("message", "Error en SELECT"),
                        }
                    )

            elif op_type == "DELETE":
                if not op_result.get("error", False):
                    deleted_count = op_result.get("count", 0)
                    result["results"].append(
                        {
                            "operation": "DELETE",
                            "records_deleted": deleted_count,
                            "deleted_ids": op_result.get("records_deleted", []),
                            "message": f"{deleted_count} registro(s) eliminado(s)",
                        }
                    )
                else:
                    result["results"].append(
                        {
                            "operation": "DELETE",
                            "error": True,
                            "message": op_result.get("message", "Error en DELETE"),
                        }
                    )

            elif op_type == "IMPORT_CSV":
                if not op_result.get("error", False):
                    successful = op_result.get("successful_inserts", 0)
                    failed = op_result.get("failed_inserts_count", 0)
                    csv_file = op_result.get("csv_file", "")
                    result["results"].append(
                        {
                            "operation": "IMPORT_CSV",
                            "csv_file": os.path.basename(csv_file),
                            "records_imported": successful,
                            "records_failed": failed,
                            "message": f"Importados {successful} registros desde CSV",
                        }
                    )
                else:
                    result["results"].append(
                        {
                            "operation": "IMPORT_CSV",
                            "error": True,
                            "message": op_result.get("message", "Error en importación CSV"),
                        }
                    )

        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error SQL: {str(e)}")


@app.get("/tables")
async def get_tables():
    """
    Obtiene la lista de todas las tablas disponibles con información detallada.
    """
    if not sql_manager:
        raise HTTPException(status_code=500, detail="Sistema no inicializado")

    try:
        tables_info = {}

        for table_name, table_info in sql_manager.get_all_tables().items():
            storage_manager = sql_manager.get_storage_manager(table_name)
            record_count = 0
            active_count = 0

            if storage_manager:
                try:
                    record_count = storage_manager._get_record_count()
                    active_records = storage_manager._get_all_active_record_numbers()
                    active_count = len(active_records)
                except:
                    pass

            # Extraer información de los atributos
            columns = []
            indexes = []
            primary_key = table_info.get("primary_key", "")

            for attr in table_info.get("attributes", []):
                column_info = {
                    "name": attr["name"],
                    "data_type": attr["data_type"],
                    "is_key": attr.get("is_key", False),
                }
                columns.append(column_info)

                if attr.get("index"):
                    indexes.append({"attribute": attr["name"], "type": attr["index"]})

            tables_info[table_name] = {
                "total_records": record_count,
                "active_records": active_count,
                "record_count": active_count,
                "primary_key": primary_key,
                "columns": columns,
                "indexes": indexes,
            }
        return {"success": True, "tables": tables_info, "total_tables": len(tables_info)}

    except Exception as e:
        return {
            "success": False,
            "error": f"Error obteniendo tablas: {str(e)}",
            "tables": {},
            "total_tables": 0,
        }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/tables/{table_name}")
async def get_table_info(table_name: str):
    """
    Obtiene información detallada de una tabla específica.
    """
    if not sql_manager:
        raise HTTPException(status_code=500, detail="Sistema no inicializado")

    table_info = sql_manager.get_table(table_name)
    if not table_info:
        raise HTTPException(status_code=404, detail=f"Tabla '{table_name}' no encontrada")

    storage_manager = sql_manager.get_storage_manager(table_name)

    # Obtener algunos registros de ejemplo
    sample_records = []
    if storage_manager:
        try:
            all_records = storage_manager.get_all_records()
            # Tomar los primeros 5 registros como muestra
            sample_records = serialize_records_data(all_records[:5])
        except:
            pass

    return {
        "table_name": table_name,
        "table_info": table_info,
        "sample_records": sample_records,
        "total_records": len(storage_manager.get_all_records()) if storage_manager else 0,
    }


# Necesitarás instalar: pip install bcrypt pyjwt
from pydantic import BaseModel
import bcrypt
import jwt
from datetime import datetime, timedelta, timezone


# Modelos para requests
class RegisterRequest(BaseModel):
    user: str
    password: str


class LoginRequest(BaseModel):
    user: str
    password: str


# Configuración JWT (cambiar SECRET_KEY en producción)
SECRET_KEY = "tu_clave_secreta_super_segura_cambiala"
ALGORITHM = "HS256"


@app.post("/register")
async def register(request: RegisterRequest):
    """Registra un nuevo usuario"""
    # Verificar que la tabla existe
    if "auth_usuario_xa" not in sql_manager.tables:
        raise HTTPException(status_code=500, detail="Tabla de usuarios no configurada")

    storage = sql_manager.get_storage_manager("auth_usuario_xa")

    # Verificar si el usuario ya existe
    resultado = storage.select(lista_busquedas=[["user", request.user]])
    if resultado.get("numeros_registro"):
        raise HTTPException(status_code=400, detail="Usuario ya existe")

    # Hash de la password
    hashed = bcrypt.hashpw(request.password.encode("utf-8"), bcrypt.gensalt())

    # Obtener próximo ID
    all_records = storage.get_all_records()
    next_id = len(all_records) + 1

    # Insertar usuario
    record = {
        "id": next_id,
        "user": request.user,
        "password": hashed.decode("utf-8"),  # Guardar como string
    }

    storage.insert(record)

    return {"success": True, "message": "Usuario registrado exitosamente"}


@app.post("/login")
async def login(request: LoginRequest):
    """Inicia sesión y retorna token JWT"""
    if "auth_usuario_xa" not in sql_manager.tables:
        raise HTTPException(status_code=500, detail="Tabla de usuarios no configurada")

    storage = sql_manager.get_storage_manager("auth_usuario_xa")

    # Buscar usuario
    resultado = storage.select(lista_busquedas=[["user", request.user]])

    if not resultado.get("numeros_registro"):
        raise HTTPException(status_code=401, detail="Usuario o contraseña incorrectos")

    # Obtener el registro del usuario
    record_num = resultado["numeros_registro"][0]
    user_data = storage.get(record_num)

    # Verificar password
    if not bcrypt.checkpw(request.password.encode("utf-8"), user_data["password"].encode("utf-8")):
        raise HTTPException(status_code=401, detail="Usuario o contraseña incorrectos")

    # Crear token JWT
    payload = {
        "user_id": user_data["id"],
        "username": user_data["user"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=24),  # Expira en 24 horas
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

    return {
        "success": True,
        "token": token,
        "user": {"id": user_data["id"], "username": user_data["user"]},
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
