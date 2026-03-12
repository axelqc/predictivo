"""
MSB León — API de Mantenimiento Inteligente
FastAPI + DB2 (ibm_db) endpoints para watsonx Orchestrate

Autor: MSB León — Data & Automation
"""

import os
import ibm_db
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import get_db2_connection, execute_query, SCHEMA, DB2_CONFIG
from plan_predictivo import router as plan_router

# ============================================================
# APP
# ============================================================
app = FastAPI(
    title="MSB Mantenimiento Inteligente API",
    description="API para análisis predictivo de fallas y mantenimiento basado en condición. Diseñada para ser consumida por watsonx Orchestrate como skills.",
    version="1.0.0",
    servers=[
        {"url": os.getenv("API_BASE_URL", "http://localhost:8000"), "description": "Servidor principal"}
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(plan_router)


# ============================================================
# MODELOS PYDANTIC
# ============================================================
class EquipoPerfil(BaseModel):
    numero_serie: str
    numero_economico: Optional[str] = None
    modelo: Optional[str] = None
    cliente: Optional[str] = None
    fecha_inicio_op: Optional[str] = None
    total_correctivos: int = 0
    total_preventivos: int = 0
    ratio_corr_prev: Optional[float] = None
    ultimo_horometro: Optional[float] = None
    promedio_uso_dia: Optional[float] = None
    categoria_dominante: Optional[str] = None
    risk_score: Optional[float] = None
    nivel_riesgo: Optional[str] = None
    banda_horometro: Optional[str] = None
    ultima_falla: Optional[str] = None
    ultimo_preventivo: Optional[str] = None


class RiskScoreResponse(BaseModel):
    numero_serie: str
    risk_score: float
    nivel_riesgo: str
    total_correctivos: int
    categoria_dominante: Optional[str]
    ultimo_horometro: Optional[float]
    promedio_uso_dia: Optional[float]
    ratio_corr_prev: Optional[float]


class HistorialItem(BaseModel):
    tipo: str
    fecha: Optional[str]
    horometro: Optional[float]
    detalle: Optional[str]
    descripcion: Optional[str]
    tecnico: Optional[str]


class CategoriaFalla(BaseModel):
    categoria_falla: str
    total_fallas: int
    porcentaje: float
    equipos_afectados: int
    primera_falla: Optional[str]
    ultima_falla: Optional[str]


class BandaHorometro(BaseModel):
    banda_horometro: str
    total_correctivos: int
    equipos: int
    promedio_por_equipo: float


class RecomendacionResponse(BaseModel):
    numero_serie: str
    recomendacion: str


class EquipoCritico(BaseModel):
    numero_serie: str
    numero_economico: Optional[str]
    modelo: Optional[str]
    cliente: Optional[str]
    risk_score: float
    nivel_riesgo: str
    total_correctivos: int
    categoria_dominante: Optional[str]
    ultimo_horometro: Optional[float]
    promedio_uso_dia: Optional[float]


# ============================================================
# ENDPOINTS
# ============================================================

# ----------------------------------------------------------
# 1. PERFIL COMPLETO DE EQUIPO POR N° SERIE
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}",
    response_model=EquipoPerfil,
    summary="Perfil completo de equipo",
    description="Retorna toda la información del equipo incluyendo métricas de riesgo, horómetro, uso diario y categoría de falla dominante. Usar el número de serie como identificador único.",
    tags=["Equipos"],
)
def get_equipo_perfil(numero_serie: str):
    sql = f"""
        SELECT * FROM {SCHEMA}.V_PERFIL_EQUIPO
        WHERE NUMERO_SERIE = ?
    """
    results = execute_query(sql, (numero_serie,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Equipo {numero_serie} no encontrado")
    return results[0]


# ----------------------------------------------------------
# 2. SCORE DE RIESGO DE UN EQUIPO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/riesgo",
    response_model=RiskScoreResponse,
    summary="Score de riesgo del equipo",
    description="Calcula y retorna el score de riesgo basado en: (correctivos × 3) + (uso_diario × 0.5) + (horómetro/1000 × 0.3). Niveles: CRITICO (≥25), ALTO (≥20), MEDIO (≥15), BAJO (<15).",
    tags=["Riesgo"],
)
def get_risk_score(numero_serie: str):
    sql = f"""
        SELECT NUMERO_SERIE, RISK_SCORE, NIVEL_RIESGO,
               TOTAL_CORRECTIVOS, CATEGORIA_DOMINANTE,
               ULTIMO_HOROMETRO, PROMEDIO_USO_DIA, RATIO_CORR_PREV
        FROM {SCHEMA}.RISK_SCORE
        WHERE NUMERO_SERIE = ?
    """
    results = execute_query(sql, (numero_serie,))
    if not results:
        raise HTTPException(status_code=404, detail=f"No hay datos de riesgo para {numero_serie}")
    return results[0]


# ----------------------------------------------------------
# 3. HISTORIAL COMPLETO DE UN EQUIPO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/historial",
    response_model=List[HistorialItem],
    summary="Historial unificado correctivo + preventivo",
    description="Retorna el historial completo de mantenimientos (correctivos y preventivos) del equipo, ordenado por fecha descendente.",
    tags=["Equipos"],
)
def get_historial(
    numero_serie: str,
    tipo: Optional[str] = Query(None, description="Filtrar por tipo: CORRECTIVO o PREVENTIVO"),
    limit: int = Query(50, ge=1, le=500, description="Máximo de registros"),
):
    where = "WHERE NUMERO_SERIE = ?"
    params = [numero_serie]
    if tipo:
        where += " AND TIPO = ?"
        params.append(tipo.upper())

    sql = f"""
        SELECT * FROM {SCHEMA}.V_HISTORIAL_UNIFICADO
        {where}
        ORDER BY FECHA DESC
        FETCH FIRST {limit} ROWS ONLY
    """
    return execute_query(sql, tuple(params))


# ----------------------------------------------------------
# 4. FALLAS CORRECTIVAS DE UN EQUIPO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/correctivos",
    summary="Detalle de fallas correctivas",
    description="Lista todas las fallas correctivas del equipo con categoría, horómetro al momento de falla, refacciones usadas y observaciones.",
    tags=["Correctivos"],
)
def get_correctivos(numero_serie: str):
    sql = f"""
        SELECT FECHA_SERVICIO, HOROMETRO, CATEGORIA_FALLA,
               FALLA_REPORTADA, REFACCIONES, OBSERVACIONES, TECNICO
        FROM {SCHEMA}.CORRECTIVOS
        WHERE NUMERO_SERIE = ?
        ORDER BY FECHA_SERVICIO DESC
    """
    return execute_query(sql, (numero_serie,))


# ----------------------------------------------------------
# 5. EQUIPOS CRÍTICOS (TOP N POR RIESGO)
# ----------------------------------------------------------
@app.get(
    "/equipos/criticos",
    response_model=List[EquipoCritico],
    summary="Equipos con mayor riesgo de falla",
    description="Retorna los equipos ordenados por score de riesgo descendente. Permite filtrar por nivel y cliente.",
    tags=["Riesgo"],
)
def get_equipos_criticos(
    nivel: Optional[str] = Query(None, description="Filtrar por nivel: CRITICO, ALTO, MEDIO, BAJO"),
    cliente: Optional[str] = Query(None, description="Filtrar por cliente"),
    modelo: Optional[str] = Query(None, description="Filtrar por modelo"),
    top: int = Query(20, ge=1, le=100, description="Cantidad de resultados"),
):
    conditions = []
    params = []

    if nivel:
        conditions.append("r.NIVEL_RIESGO = ?")
        params.append(nivel.upper())
    if cliente:
        conditions.append("UPPER(e.CLIENTE) LIKE UPPER(?)")
        params.append(f"%{cliente}%")
    if modelo:
        conditions.append("UPPER(e.MODELO) LIKE UPPER(?)")
        params.append(f"%{modelo}%")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
               r.RISK_SCORE, r.NIVEL_RIESGO, r.TOTAL_CORRECTIVOS,
               r.CATEGORIA_DOMINANTE, r.ULTIMO_HOROMETRO, r.PROMEDIO_USO_DIA
        FROM {SCHEMA}.EQUIPOS e
        JOIN {SCHEMA}.RISK_SCORE r ON e.NUMERO_SERIE = r.NUMERO_SERIE
        {where}
        ORDER BY r.RISK_SCORE DESC
        FETCH FIRST {top} ROWS ONLY
    """
    return execute_query(sql, tuple(params))


# ----------------------------------------------------------
# 6. PATRONES DE FALLA POR CATEGORÍA
# ----------------------------------------------------------
@app.get(
    "/patrones/categorias",
    response_model=List[CategoriaFalla],
    summary="Distribución de fallas por categoría",
    description="Análisis de las categorías de falla con totales, porcentajes y cantidad de equipos afectados.",
    tags=["Patrones"],
)
def get_patrones_categoria():
    sql = f"SELECT * FROM {SCHEMA}.V_PATRONES_CATEGORIA ORDER BY TOTAL_FALLAS DESC"
    return execute_query(sql)


# ----------------------------------------------------------
# 7. ANÁLISIS POR BANDA DE HORÓMETRO
# ----------------------------------------------------------
@app.get(
    "/patrones/horometro",
    response_model=List[BandaHorometro],
    summary="Fallas por banda de horómetro",
    description="Agrupa los correctivos por rangos de horómetro (0-1K, 1K-2.5K, 2.5K-5K, 5K-10K, 10K+) para identificar zonas de desgaste acelerado.",
    tags=["Patrones"],
)
def get_patrones_horometro():
    sql = f"SELECT * FROM {SCHEMA}.V_ANALISIS_HOROMETRO ORDER BY BANDA_HOROMETRO"
    return execute_query(sql)


# ----------------------------------------------------------
# 8. RECOMENDACIÓN INTELIGENTE POR EQUIPO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/recomendacion",
    response_model=RecomendacionResponse,
    summary="Recomendación de mantenimiento inteligente",
    description="Genera una recomendación personalizada basada en el historial del equipo, su horómetro, intensidad de uso, categoría de falla dominante y ratio correctivo/preventivo.",
    tags=["Recomendaciones"],
)
def get_recomendacion(numero_serie: str):
    conn = get_db2_connection()
    try:
        sql = f"CALL {SCHEMA}.SP_RECOMENDACION(?, ?)"
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(stmt, 1, numero_serie)
        out_recomendacion = " " * 2000
        ibm_db.bind_param(stmt, 2, out_recomendacion, ibm_db.SQL_PARAM_OUTPUT)
        ibm_db.execute(stmt)
        result = ibm_db.result(stmt, 1)
        return {
            "numero_serie": numero_serie,
            "recomendacion": result.strip() if result else "Sin datos suficientes para generar recomendación.",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 9. RECALCULAR SCORES (admin/cron)
# ----------------------------------------------------------
@app.post(
    "/admin/recalcular-scores",
    summary="Recalcular todos los scores de riesgo",
    description="Ejecuta el procedimiento SP_RECALCULAR_RISK_SCORE que actualiza la tabla RISK_SCORE con los datos más recientes de correctivos y preventivos.",
    tags=["Admin"],
)
def recalcular_scores():
    conn = get_db2_connection()
    try:
        ibm_db.exec_immediate(conn, f"CALL {SCHEMA}.SP_RECALCULAR_RISK_SCORE()")
        ibm_db.commit(conn)
        return {"status": "ok", "message": "Risk scores recalculados exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 10. BUSCAR EQUIPO POR # ECONÓMICO O SERIE PARCIAL
# ----------------------------------------------------------
@app.get(
    "/equipos/buscar",
    summary="Buscar equipo por número económico o serie parcial",
    description="Busca equipos que coincidan parcialmente con el término de búsqueda en número de serie o número económico.",
    tags=["Equipos"],
)
def buscar_equipo(
    q: str = Query(..., min_length=2, description="Término de búsqueda"),
):
    sql = f"""
        SELECT e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
               COALESCE(r.RISK_SCORE, 0) AS RISK_SCORE,
               COALESCE(r.NIVEL_RIESGO, 'SIN DATOS') AS NIVEL_RIESGO,
               COALESCE(r.TOTAL_CORRECTIVOS, 0) AS TOTAL_CORRECTIVOS
        FROM {SCHEMA}.EQUIPOS e
        LEFT JOIN {SCHEMA}.RISK_SCORE r ON e.NUMERO_SERIE = r.NUMERO_SERIE
        WHERE UPPER(e.NUMERO_SERIE) LIKE UPPER(?)
           OR UPPER(e.NUMERO_ECONOMICO) LIKE UPPER(?)
        ORDER BY COALESCE(r.RISK_SCORE, 0) DESC
        FETCH FIRST 20 ROWS ONLY
    """
    term = f"%{q}%"
    return execute_query(sql, (term, term))


# ----------------------------------------------------------
# 11. RESUMEN EJECUTIVO GLOBAL
# ----------------------------------------------------------
@app.get(
    "/resumen",
    summary="Resumen ejecutivo del estado de la flota",
    description="Métricas globales: total correctivos/preventivos, equipos críticos, categoría más frecuente, promedio de uso de la flota.",
    tags=["Resumen"],
)
def get_resumen():
    total_corr  = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {SCHEMA}.CORRECTIVOS")[0]["TOTAL"]
    total_prev  = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {SCHEMA}.PREVENTIVOS")[0]["TOTAL"]
    criticos    = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {SCHEMA}.RISK_SCORE WHERE NIVEL_RIESGO = 'CRITICO'")[0]["TOTAL"]
    altos       = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {SCHEMA}.RISK_SCORE WHERE NIVEL_RIESGO = 'ALTO'")[0]["TOTAL"]
    cat_top     = execute_query(f"""
        SELECT CATEGORIA_FALLA, COUNT(*) AS TOTAL
        FROM {SCHEMA}.CORRECTIVOS
        WHERE CATEGORIA_FALLA IS NOT NULL
        GROUP BY CATEGORIA_FALLA
        ORDER BY COUNT(*) DESC
        FETCH FIRST 1 ROW ONLY
    """)
    flota = execute_query(f"""
        SELECT COUNT(*) AS EQUIPOS,
               AVG(PROMEDIO_USO_DIA) AS AVG_USO,
               AVG(ULTIMO_HOROMETRO) AS AVG_HORO
        FROM {SCHEMA}.RISK_SCORE
        WHERE PROMEDIO_USO_DIA IS NOT NULL
    """)

    return {
        "total_correctivos":        total_corr,
        "total_preventivos":        total_prev,
        "ratio_global":             round(total_corr / max(total_prev, 1) * 100, 1),
        "equipos_criticos":         criticos,
        "equipos_alto_riesgo":      altos,
        "categoria_mas_frecuente":  cat_top[0] if cat_top else None,
        "flota":                    flota[0] if flota else None,
    }


# ----------------------------------------------------------
# 12. EQUIPOS POR VENCER PREVENTIVO
# ----------------------------------------------------------
@app.get(
    "/alertas/preventivos-vencidos",
    summary="Equipos con preventivo próximo o vencido",
    description="Lista equipos cuya fecha sugerida de próximo PMM está vencida o próxima a vencer, priorizados por riesgo.",
    tags=["Alertas"],
)
def get_preventivos_vencidos(
    dias: int = Query(7, description="Días de anticipación para considerar 'próximo a vencer'"),
):
    sql = f"""
        SELECT p.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
               p.FECHA_SUGERIDA_PMM,
               DAYS(p.FECHA_SUGERIDA_PMM) - DAYS(CURRENT DATE) AS DIAS_RESTANTES,
               COALESCE(r.RISK_SCORE, 0) AS RISK_SCORE,
               COALESCE(r.NIVEL_RIESGO, 'SIN DATOS') AS NIVEL_RIESGO,
               p.HOROMETRO_ACTUAL
        FROM {SCHEMA}.PREVENTIVOS p
        JOIN {SCHEMA}.EQUIPOS e ON p.NUMERO_SERIE = e.NUMERO_SERIE
        LEFT JOIN {SCHEMA}.RISK_SCORE r ON p.NUMERO_SERIE = r.NUMERO_SERIE
        WHERE p.FECHA_SUGERIDA_PMM IS NOT NULL
          AND p.FECHA_SUGERIDA_PMM <= CURRENT DATE + ? DAYS
          AND p.ID_PREVENTIVO = (
              SELECT MAX(p2.ID_PREVENTIVO)
              FROM {SCHEMA}.PREVENTIVOS p2
              WHERE p2.NUMERO_SERIE = p.NUMERO_SERIE
          )
        ORDER BY COALESCE(r.RISK_SCORE, 0) DESC
    """
    return execute_query(sql, (dias,))


# ============================================================
# HEALTH CHECK
# ============================================================
@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "service": "MSB Mantenimiento Inteligente API", "version": "1.0.0"}


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
