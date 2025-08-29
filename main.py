from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import ee
import os
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def init_ee():
    """Inicializar Earth Engine"""
    try:
        creds = os.getenv('GOOGLE_CREDENTIALS')
        if not creds:
            return False
            
        creds_dict = json.loads(creds)
        credentials = ee.ServiceAccountCredentials(
            email=creds_dict['client_email'],
            key_data=creds
        )
        ee.Initialize(credentials)
        return True
    except Exception as e:
        print(f"Error inicializando EE: {e}")
        return False

@app.on_event("startup")
async def startup_event():
    success = init_ee()
    print(f"EE Initialization: {'Success' if success else 'Failed'}")

@app.get("/")
async def root():
    return {"message": "API NDVI Ecuador", "status": "ok"}

@app.get("/test-ee")
async def test_ee():
    """Test básico de Earth Engine"""
    try:
        img = ee.Image(1)
        info = img.getInfo()
        return {"success": True, "message": "Earth Engine funcionando"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/ndvi")
async def get_ndvi():
    """Obtener capa NDVI de Ecuador (recortado exacto)"""
    try:
        # Usar límite administrativo exacto de Ecuador (más preciso que rectángulo)
        ecuador = ee.FeatureCollection("FAO/GAUL/2015/level0") \
            .filter(ee.Filter.eq("ADM0_NAME", "Ecuador")) \
            .geometry()
        
        # Obtener NDVI más reciente de MODIS
        ndvi_collection = ee.ImageCollection('MODIS/061/MOD13A2') \
            .select('NDVI') \
            .filterBounds(ecuador) \
            .filterDate('2024-01-01', '2024-12-31') \
            .sort('system:time_start', False)
        
        # Tomar la imagen más reciente
        ndvi_latest = ndvi_collection.first().multiply(0.0001)
        
        # Recortar EXACTAMENTE a los límites de Ecuador
        ndvi_ecuador = ndvi_latest.clip(ecuador)
        
        # Aplicar máscara para mostrar solo Ecuador
        ndvi_masked = ndvi_ecuador.updateMask(ndvi_ecuador.gte(-1))
        
        # Generar visualización mejorada
        vis_params = {
            'min': 0,
            'max': 1,
            'palette': [
                '#8B0000',  # Rojo oscuro (sin vegetación)
                '#CD5C5C',  # Rojo claro
                '#F0E68C',  # Amarillo (vegetación baja)
                '#9ACD32',  # Verde amarillento
                '#32CD32',  # Verde lima
                '#228B22',  # Verde bosque
                '#006400'   # Verde oscuro (vegetación densa)
            ]
        }
        
        # Obtener URL de tiles
        map_id = ndvi_masked.getMapId(vis_params)
        
        return {
            "success": True,
            "tile_url": map_id['tile_fetcher'].url_format,
            "mapid": map_id['mapid'],
            "token": map_id['token'],
            "message": "NDVI recortado exactamente para Ecuador",
            "date_range": "2024-01-01 a 2024-12-31",
            "description": "NDVI más reciente de MODIS recortado con límites administrativos de Ecuador",
            "boundary_source": "FAO GAUL 2015"
        }
        
    except Exception as e:
        # Si falla, intentar reinicializar
        if "not initialized" in str(e).lower():
            if init_ee():
                return await get_ndvi()  # Reintentar
        
        return {"success": False, "error": str(e)}

@app.get("/ndvi-info")
async def get_ndvi_info():
    """Información sobre el dataset NDVI"""
    try:
        ecuador = ee.Geometry.Rectangle([-82, -5, -75, 2])
        
        collection = ee.ImageCollection('MODIS/061/MOD13A2') \
            .select('NDVI') \
            .filterBounds(ecuador) \
            .filterDate('2024-01-01', '2024-12-31')
        
        # Obtener información de la colección
        size = collection.size().getInfo()
        
        if size > 0:
            latest = collection.sort('system:time_start', False).first()
            date_info = latest.get('system:time_start').getInfo()
            date_readable = ee.Date(date_info).format('YYYY-MM-dd').getInfo()
            
            return {
                "success": True,
                "total_images": size,
                "latest_date": date_readable,
                "dataset": "MODIS/061/MOD13A2",
                "spatial_resolution": "500m",
                "temporal_resolution": "16 days"
            }
        else:
            return {"success": False, "error": "No hay imágenes disponibles"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/indice-sequedad")
async def get_indice_sequedad():
    """Índice de Sequedad Combinado (ISC) - Tu algoritmo completo"""
    try:
        # ROI de Ecuador usando límites administrativos
        roi = ee.FeatureCollection("FAO/GAUL/2015/level0").filter(ee.Filter.eq("ADM0_NAME","Ecuador"))

        # Fechas
        fechaInicio = '2024-01-01'
        fechaFin = '2025-12-31'
        fecha = fechaFin

        # Máscara de recorte
        mascaracut = ee.Image(1).clip(roi)

        def cortarcoleccion(imagen):
            mascara = mascaracut.mask()
            return imagen.updateMask(mascara)

        # 1. PRECIPITACIÓN GPM
        gpmColeccion = ee.ImageCollection('NASA/GPM_L3/IMERG_V06') \
            .select('precipitationCal') \
            .filterBounds(roi) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .limit(48) \
            .map(cortarcoleccion)

        # Duración de precipitación
        umbral = 0.1
        conprecipitacion = gpmColeccion.map(lambda image: image.gt(umbral))
        duracionPrecipitacion = gpmColeccion.sum().divide(2).rename('duracion')

        # 2. TEMPERATURA ERA5
        templast = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR') \
            .select('temperature_2m') \
            .filterBounds(roi) \
            .map(cortarcoleccion) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .first()

        # 3. PUNTO DE ROCÍO
        dewpoint = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR') \
            .select('dewpoint_temperature_2m') \
            .filterBounds(roi) \
            .map(cortarcoleccion) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .first()

        # 4. CÁLCULO HUMEDAD RELATIVA
        temperaK = templast.subtract(273.15)
        dewpointK = dewpoint.subtract(273.15)
        pvse = dewpointK.multiply(17.27).divide(dewpointK.add(237.3)).exp().multiply(6.1078)
        pvses = temperaK.multiply(17.27).divide(temperaK.add(237.3)).exp().multiply(6.1078)
        relativehumidity = pvse.divide(pvses).multiply(100).rename('relahumi')

        datos = relativehumidity.addBands(templast).clip(roi)

        # 5. NDVI MODIS
        coleccionNDVI = ee.ImageCollection("MODIS/061/MOD13A2") \
            .select('NDVI') \
            .filterDate(fechaInicio, fechaFin) \
            .filterBounds(roi) \
            .map(cortarcoleccion)

        ndvilast = coleccionNDVI.sort('system:time_end', False).first().multiply(0.0001)

        # 6. NDVI MIN/MAX - Usar datos históricos públicos en lugar de assets privados
        # Calcular min/max de la colección histórica
        ndviHistorico = ee.ImageCollection("MODIS/061/MOD13A2") \
            .select('NDVI') \
            .filterDate('2020-01-01', '2024-12-31') \
            .filterBounds(roi) \
            .map(cortarcoleccion)

        ndviStats = ndviHistorico.reduce(ee.Reducer.minMax())
        minNDVI = ndviStats.select('NDVI_min').multiply(0.0001)
        maxNDVI = ndviStats.select('NDVI_max').multiply(0.0001)

        # 7. EMC (Equilibrium Moisture Content)
        EMC = datos.expression(
            "(b('relahumi') < 10) ? 0.032229+0.281073*b('relahumi')-0.000578*b('relahumi')*b('temperature_2m')" +
            ": (b('relahumi') < 50) ? 2.22749+0.160107*b('relahumi')-0.014784*b('temperature_2m')" +
            ": 21.0606+0.005565*(b('relahumi')**2)-0.00035*b('relahumi')*b('temperature_2m')-0.483199*b('relahumi')"
        ).rename('EMC')

        # 8. H100
        h100inputs = EMC.addBands(duracionPrecipitacion).clip(roi)
        h100 = h100inputs.expression(
            "(24 - b('duracion')) * b('EMC') + b('duracion') * (0.5 * b('duracion') + 41)"
        ).divide(24).rename('H100')

        # 9. LRmax
        imagenLRmax = maxNDVI.expression(
            '0.30 + 0.30 * ((NDVImax + 0.19) / (0.95 + 0.19))', {
                'NDVImax': maxNDVI
            }
        ).rename('LRmax')

        # 10. RG (Relative Greenness)
        imagenRG = ndvilast.expression(
            '((NDVI - NDVImin) / (NDVImax - NDVImin)) * 100', {
                'NDVI': ndvilast.select('NDVI'),
                'NDVImin': minNDVI,
                'NDVImax': maxNDVI
            }
        ).rename('RG')

        # 11. LR (Live Fuel Moisture)
        imagenLR = imagenRG.expression(
            'RG * LRmax / 100', {
                'RG': imagenRG,
                'LRmax': imagenLRmax
            }
        ).rename('LR')

        # 12. MR - Usar estadísticas de H100 en lugar de assets privados
        h100Stats = h100.reduceRegion(
            reducer=ee.Reducer.minMax(),
            geometry=roi,
            scale=1000,
            maxPixels=1e9
        )
        
        # Valores aproximados para Ecuador (puedes ajustar)
        H100min = ee.Image.constant(h100Stats.getNumber('H100_min').getInfo() if h100Stats.getNumber('H100_min').getInfo() else 10)
        H100max = ee.Image.constant(h100Stats.getNumber('H100_max').getInfo() if h100Stats.getNumber('H100_max').getInfo() else 50)

        imagenMR = h100.expression(
            '((H100 - H100min) / (H100max - H100min))', {
                'H100': h100,
                'H100min': H100min,
                'H100max': H100max
            }
        ).rename('MR')

        # 13. FDI (Fire Danger Index)
        imagenFDIsc = imagenLR.expression(
            '((1 - LR) * (1 - MR)) * 100', {
                'LR': imagenLR,
                'MR': imagenMR
            }
        ).rename('FDI')

        # 14. CLASIFICACIÓN FINAL
        imagenFDI = ee.Image(0) \
            .where(imagenFDIsc.lt(50), 1) \
            .where(imagenFDIsc.gte(50).And(imagenFDIsc.lt(60)), 2) \
            .where(imagenFDIsc.gte(60).And(imagenFDIsc.lt(70)), 3) \
            .where(imagenFDIsc.gte(70).And(imagenFDIsc.lt(80)), 4) \
            .where(imagenFDIsc.gte(80).And(imagenFDIsc.lt(91)), 5) \
            .where(imagenFDIsc.gte(91), 6).clip(roi)

        # Paleta de colores (tu simbología original)
        Simbologia = ['267E00','56E200','FFFC00','FE7400','FF0000','9E00FF']
        
        # Etiquetas originales
        Etiquetas = [
            'Muy baja (<50)',
            'Baja (50-60)',
            'Media (60-70)',
            'Alta (70-80)',
            'Muy alta (80-91)',
            'Extrema (>91)'
        ]

        # Parámetros de visualización
        imagenFDIVis = {'min': 1, 'max': 6, 'palette': Simbologia, 'opacity': 0.70}

        # Generar tiles
        map_id = imagenFDI.getMapId(imagenFDIVis)

        return {
            "success": True,
            "tile_url": map_id['tile_fetcher'].url_format,
            "mapid": map_id['mapid'],
            "token": map_id['token'],
            "message": "Índice de Sequedad Combinado (ISC) generado exitosamente",
            "algorithm": "Tu algoritmo original completo",
            "date_range": f"{fechaInicio} a {fechaFin}",
            "legend": {
                "title": "Nivel de Sequedad",
                "labels": Etiquetas,
                "colors": Simbologia
            },
            "data_sources": {
                "precipitation": "NASA GPM_L3/IMERG_V06",
                "temperature": "ECMWF ERA5_LAND/DAILY_AGGR",
                "ndvi": "MODIS/061/MOD13A2",
                "boundaries": "FAO/GAUL/2015/level0"
            }
        }

    except Exception as e:
        if "not initialized" in str(e).lower():
            if init_ee():
                return await get_indice_sequedad()
        
        return {"success": False, "error": str(e), "message": "Error procesando índice de sequedad"}

import time
from datetime import datetime

# Variable global para cache
cache_data = {
    "sequedad": None,
    "timestamp": None,
    "processing": False
}

@app.get("/sequedad-cache")
async def get_sequedad_cache():
    """Cargar índice de sequedad desde cache (rápido)"""
    try:
        # Si hay cache válido, devolverlo
        if cache_data["sequedad"] and cache_data["timestamp"]:
            age_minutes = (time.time() - cache_data["timestamp"]) / 60
            
            return {
                "success": True,
                "from_cache": True,
                "cache_age_minutes": round(age_minutes, 1),
                **cache_data["sequedad"]
            }
        else:
            return {
                "success": False, 
                "error": "No hay cache disponible", 
                "message": "Ejecuta /actualizar-sequedad primero"
            }
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/actualizar-sequedad")
async def actualizar_sequedad():
    """Procesar y actualizar cache del índice de sequedad"""
    try:
        # Verificar si ya se está procesando
        if cache_data["processing"]:
            return {
                "success": False, 
                "message": "Ya se está procesando. Espera unos minutos.",
                "processing": True
            }
        
        # Marcar como procesando
        cache_data["processing"] = True
        
        # Tu algoritmo completo (mismo código de antes)
        roi = ee.FeatureCollection("FAO/GAUL/2015/level0").filter(ee.Filter.eq("ADM0_NAME","Ecuador"))
        fechaInicio = '2024-01-01'
        fechaFin = '2025-12-31'
        
        mascaracut = ee.Image(1).clip(roi)
        def cortarcoleccion(imagen):
            mascara = mascaracut.mask()
            return imagen.updateMask(mascara)

        # GPM Precipitación
        gpmColeccion = ee.ImageCollection('NASA/GPM_L3/IMERG_V06') \
            .select('precipitationCal') \
            .filterBounds(roi) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .limit(48) \
            .map(cortarcoleccion)

        duracionPrecipitacion = gpmColeccion.sum().divide(2).rename('duracion')

        # ERA5 Temperatura y punto de rocío
        templast = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR') \
            .select('temperature_2m') \
            .filterBounds(roi) \
            .map(cortarcoleccion) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .first()

        dewpoint = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR') \
            .select('dewpoint_temperature_2m') \
            .filterBounds(roi) \
            .map(cortarcoleccion) \
            .filterDate(fechaInicio, fechaFin) \
            .sort('system:time_end', False) \
            .first()

        # Humedad relativa
        temperaK = templast.subtract(273.15)
        dewpointK = dewpoint.subtract(273.15)
        pvse = dewpointK.multiply(17.27).divide(dewpointK.add(237.3)).exp().multiply(6.1078)
        pvses = temperaK.multiply(17.27).divide(temperaK.add(237.3)).exp().multiply(6.1078)
        relativehumidity = pvse.divide(pvses).multiply(100).rename('relahumi')
        datos = relativehumidity.addBands(templast).clip(roi)

        # NDVI
        coleccionNDVI = ee.ImageCollection("MODIS/061/MOD13A2") \
            .select('NDVI') \
            .filterDate(fechaInicio, fechaFin) \
            .filterBounds(roi) \
            .map(cortarcoleccion)
        ndvilast = coleccionNDVI.sort('system:time_end', False).first().multiply(0.0001)

        # NDVI min/max histórico
        ndviHistorico = ee.ImageCollection("MODIS/061/MOD13A2") \
            .select('NDVI') \
            .filterDate('2020-01-01', '2024-12-31') \
            .filterBounds(roi) \
            .map(cortarcoleccion)
        ndviStats = ndviHistorico.reduce(ee.Reducer.minMax())
        minNDVI = ndviStats.select('NDVI_min').multiply(0.0001)
        maxNDVI = ndviStats.select('NDVI_max').multiply(0.0001)

        # EMC
        EMC = datos.expression(
            "(b('relahumi') < 10) ? 0.032229+0.281073*b('relahumi')-0.000578*b('relahumi')*b('temperature_2m')" +
            ": (b('relahumi') < 50) ? 2.22749+0.160107*b('relahumi')-0.014784*b('temperature_2m')" +
            ": 21.0606+0.005565*(b('relahumi')**2)-0.00035*b('relahumi')*b('temperature_2m')-0.483199*b('relahumi')"
        ).rename('EMC')

        # H100
        h100inputs = EMC.addBands(duracionPrecipitacion).clip(roi)
        h100 = h100inputs.expression(
            "(24 - b('duracion')) * b('EMC') + b('duracion') * (0.5 * b('duracion') + 41)"
        ).divide(24).rename('H100')

        # LRmax
        imagenLRmax = maxNDVI.expression(
            '0.30 + 0.30 * ((NDVImax + 0.19) / (0.95 + 0.19))', {
                'NDVImax': maxNDVI
            }
        ).rename('LRmax')

        # RG
        imagenRG = ndvilast.expression(
            '((NDVI - NDVImin) / (NDVImax - NDVImin)) * 100', {
                'NDVI': ndvilast.select('NDVI'),
                'NDVImin': minNDVI,
                'NDVImax': maxNDVI
            }
        ).rename('RG')

        # LR
        imagenLR = imagenRG.expression(
            'RG * LRmax / 100', {
                'RG': imagenRG,
                'LRmax': imagenLRmax
            }
        ).rename('LR')

        # MR (simplificado para cache)
        h100Stats = h100.reduceRegion(
            reducer=ee.Reducer.minMax(),
            geometry=roi,
            scale=5000,  # Escala más grande para ser más rápido
            maxPixels=1e8
        )
        
        H100min = ee.Image.constant(10)  # Valores fijos para ser más rápido
        H100max = ee.Image.constant(50)

        imagenMR = h100.expression(
            '((H100 - H100min) / (H100max - H100min))', {
                'H100': h100,
                'H100min': H100min,
                'H100max': H100max
            }
        ).rename('MR')

        # FDI
        imagenFDIsc = imagenLR.expression(
            '((1 - LR) * (1 - MR)) * 100', {
                'LR': imagenLR,
                'MR': imagenMR
            }
        ).rename('FDI')

        # Clasificación final
        imagenFDI = ee.Image(0) \
            .where(imagenFDIsc.lt(50), 1) \
            .where(imagenFDIsc.gte(50).And(imagenFDIsc.lt(60)), 2) \
            .where(imagenFDIsc.gte(60).And(imagenFDIsc.lt(70)), 3) \
            .where(imagenFDIsc.gte(70).And(imagenFDIsc.lt(80)), 4) \
            .where(imagenFDIsc.gte(80).And(imagenFDIsc.lt(91)), 5) \
            .where(imagenFDIsc.gte(91), 6).clip(roi)

        Simbologia = ['267E00','56E200','FFFC00','FE7400','FF0000','9E00FF']
        Etiquetas = ['Muy baja (<50)', 'Baja (50-60)', 'Media (60-70)', 'Alta (70-80)', 'Muy alta (80-91)', 'Extrema (>91)']
        imagenFDIVis = {'min': 1, 'max': 6, 'palette': Simbologia, 'opacity': 0.70}

        # Generar tiles
        map_id = imagenFDI.getMapId(imagenFDIVis)

        # Guardar en cache
        result_data = {
            "tile_url": map_id['tile_fetcher'].url_format,
            "mapid": map_id['mapid'],
            "token": map_id['token'],
            "message": "Índice de Sequedad actualizado y almacenado en cache",
            "legend": {
                "title": "Nivel de Sequedad",
                "labels": Etiquetas,
                "colors": Simbologia
            },
            "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        }

        cache_data["sequedad"] = result_data
        cache_data["timestamp"] = time.time()
        cache_data["processing"] = False

        return {
            "success": True,
            "processing_completed": True,
            **result_data
        }

    except Exception as e:
        cache_data["processing"] = False
        return {"success": False, "error": str(e)}

@app.get("/cache-status")
async def cache_status():
    """Ver estado del cache"""
    if cache_data["timestamp"]:
        age_minutes = (time.time() - cache_data["timestamp"]) / 60
        return {
            "cache_available": bool(cache_data["sequedad"]),
            "cache_age_minutes": round(age_minutes, 1),
            "processing": cache_data["processing"],
            "last_update": datetime.fromtimestamp(cache_data["timestamp"]).strftime("%Y-%m-%d %H:%M:%S") if cache_data["timestamp"] else None
        }
    else:
        return {
            "cache_available": False,
            "processing": cache_data["processing"],
            "message": "No hay cache disponible"
        }

# Agregar estas líneas AL FINAL de tu main.py (antes del if __name__)
from fire_processor import FireProcessor

fire_cache = {"data": None, "timestamp": None, "processing": False}

@app.get("/process-fires")
async def process_fires():
    if fire_cache["processing"]:
        return {"success": False, "message": "Ya procesando incendios..."}
    
    fire_cache["processing"] = True
    try:
        processor = FireProcessor()
        result = processor.process_all()
        fire_cache["data"] = result
        fire_cache["timestamp"] = time.time()
        fire_cache["processing"] = False
        return result
    except Exception as e:
        fire_cache["processing"] = False
        return {"success": False, "error": str(e)}

@app.get("/fires-status") 
async def fires_status():
    if fire_cache["timestamp"]:
        age_minutes = (time.time() - fire_cache["timestamp"]) / 60
        return {
            "cache_available": bool(fire_cache["data"]),
            "cache_age_minutes": round(age_minutes, 1),
            "processing": fire_cache["processing"]
        }
    return {"cache_available": False, "processing": fire_cache["processing"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
