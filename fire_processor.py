import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from scipy.spatial import Delaunay
import os
from tqdm import tqdm
import warnings
import json
import tempfile
warnings.filterwarnings('ignore')

class FireProcessor:
    def __init__(self):
        self.provinces_path = os.path.join("data", "ORGANIZACION_TERRITORIAL_PARROQUIAL.shp")
        self.area_coords = [-92.0, -5.0, -75.2, 1.7]
        self.main_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
        self.map_key = os.getenv('NASA_FIRMS_KEY', '9c57ff9dd1fb752c9c1dc9da87bce875')
        self.sources = ["VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT", "VIIRS_SNPP_NRT"]
        self.day_range = 10
        self.distance_threshold = 1000
        self.time_lag = 3
        
        self.supabase_url = 'https://neixcsnkwtgdxkucfcnb.supabase.co'
        self.supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5laXhjc25rd3RnZHhrdWNmY25iIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDk1NzQ0OTQsImV4cCI6MjA2NTE1MDQ5NH0.OLcE9XYvYL6vzuXqcgp3dMowDZblvQo8qR21Cj39nyY'
        
    def download_fire_data(self, source, date):
        area = ",".join(map(str, self.area_coords))
        date_str = date.strftime("%Y-%m-%d")
        url = f"{self.main_url}/{self.map_key}/{source}/{area}/{self.day_range}/{date_str}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if response.text.strip():
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
                
                if not df.empty:
                    gdf = gpd.GeoDataFrame(
                        df, 
                        geometry=gpd.points_from_xy(df.longitude, df.latitude),
                        crs='EPSG:4326'
                    )
                    gdf = gdf.to_crs('EPSG:32717')
                    return gdf
            
            return gpd.GeoDataFrame()
        except Exception as e:
            print(f"Error descargando {source}: {e}")
            return gpd.GeoDataFrame()
    
    def generate_unique_id(self, fecha, geometry):
        """Genera ID único: juliano(3) + lng(3) + lat(3) = 9 dígitos"""
        try:
            # Día juliano (3 dígitos)
            juliano = fecha.timetuple().tm_yday
            
            # Centroide de la geometría
            centroid = geometry.centroid
            lng = abs(centroid.x)
            lat = abs(centroid.y)
            
            # Tomar 3 primeros dígitos sin importar signo ni decimal
            lng_str = str(lng).replace('.', '')[:3].ljust(3, '0')
            lat_str = str(lat).replace('.', '')[:3].ljust(3, '0')
            
            # Formato: juliano(3) + lng(3) + lat(3)
            unique_id = int(f"{juliano:03d}{lng_str}{lat_str}")
            
            return unique_id
        except:
            # Fallback simple
            return int(f"{fecha.strftime('%j')}000000")
    
    def load_existing_ids_from_supabase(self):
        """Carga evento_ids existentes de Supabase"""
        try:
            url = f"{self.supabase_url}/rest/v1/incendios_grandes?select=evento_id"
            headers = {
                'apikey': self.supabase_key,
                'Authorization': f'Bearer {self.supabase_key}'
            }
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                existing_ids = {item['evento_id'] for item in data if item['evento_id']}
                print(f"📋 evento_ids existentes en Supabase: {len(existing_ids)}")
                return existing_ids
            return set()
        except Exception as e:
            print(f"Error cargando evento_ids existentes: {e}")
            return set()
    
    def update_fire_data(self):
        print("Paso 1: Actualizando datos de incendios (últimos 10 días)...")
        
        # Últimos 10 días
        date = datetime.now() - timedelta(days=10)
        all_data = []
        
        print("Descargando datos de incendios...")
        for source in self.sources:
            data = self.download_fire_data(source, date)
            if not data.empty:
                all_data.append(data)
        
        if not all_data:
            print("No hay datos nuevos")
            return gpd.GeoDataFrame()
        
        combined = pd.concat(all_data, ignore_index=True)
        
        column_mapping = {
            'bright_ti4': 'BRIGHTNESS',
            'scan': 'SCAN', 
            'track': 'TRACK',
            'acq_date': 'ACQ_DATE',
            'acq_time': 'ACQ_TIME',
            'satellite': 'SATELLITE',
            'instrument': 'INSTRUMENT',
            'confidence': 'CONFIDENCE',
            'version': 'VERSION',
            'bright_ti5': 'BRIGHT_T31',
            'frp': 'FRP',
            'daynight': 'DAYNIGHT'
        }
        
        combined = combined.rename(columns=column_mapping)
        combined['evento_id'] = None
        combined['ACQ_DATE'] = pd.to_datetime(combined['ACQ_DATE'])
        
        print(f"Descargados {len(combined)} registros de FIRMS (últimos 10 días)")
        return combined
    
    def assign_event_ids(self, incendios):
        print("Paso 2: Asignando IDs de eventos...")
        
        incendios = incendios[
            (incendios['ACQ_DATE'] >= '2025-04-01') & 
            (incendios['ACQ_DATE'] <= '2025-12-31')
        ].copy()
        
        if incendios.empty:
            return incendios
        
        incendios = incendios.sort_values('ACQ_DATE').reset_index(drop=True)
        incendios['evento_id'] = None
        evento_id = 1
        
        print("Procesando clustering espacial-temporal...")
        for i in tqdm(range(len(incendios))):
            if pd.isna(incendios.loc[i, 'evento_id']):
                incendios.loc[i, 'evento_id'] = evento_id
                puntos_evento = [i]
                
                while True:
                    nuevos_puntos = []
                    
                    for punto_idx in puntos_evento:
                        punto_base = incendios.iloc[punto_idx]
                        sin_clasificar = incendios[incendios['evento_id'].isna()]
                        
                        if sin_clasificar.empty:
                            continue
                        
                        diferencia_tiempo = (sin_clasificar['ACQ_DATE'] - punto_base['ACQ_DATE']).dt.days
                        tiempo_valido = (diferencia_tiempo >= 0) & (diferencia_tiempo <= self.time_lag)
                        candidatos_temporales = sin_clasificar[tiempo_valido]
                        
                        if candidatos_temporales.empty:
                            continue
                        
                        distancias = candidatos_temporales.geometry.distance(punto_base.geometry)
                        distancia_valida = distancias <= self.distance_threshold
                        candidatos_finales = candidatos_temporales[distancia_valida]
                        
                        for idx in candidatos_finales.index:
                            incendios.loc[idx, 'evento_id'] = evento_id
                            nuevos_puntos.append(idx)
                    
                    if not nuevos_puntos:
                        break
                    
                    puntos_evento = nuevos_puntos
                
                evento_id += 1
        
        return incendios
    
    def create_polygons(self, incendios):
        print("Paso 3: Creando polígonos de incendios...")
        
        eventos_validos = incendios.groupby('evento_id').size()
        eventos_validos = eventos_validos[eventos_validos >= 5].index
        incendios_filtrados = incendios[incendios['evento_id'].isin(eventos_validos)].copy()
        
        print(f"Eventos con ≥5 puntos: {len(eventos_validos)} de {incendios['evento_id'].nunique()} totales")
        
        if incendios_filtrados.empty:
            return gpd.GeoDataFrame()
        
        incendios_filtrados['datetime'] = incendios_filtrados['ACQ_DATE'].dt.strftime('%Y-%m-%d')
        resultados_finales = []
        eventos_unicos = incendios_filtrados['evento_id'].unique()
        
        for evento in tqdm(eventos_unicos, desc="Procesando eventos"):
            incendio_actual = incendios_filtrados[incendios_filtrados['evento_id'] == evento].copy()
            incendio_actual = incendio_actual.sort_values('datetime')
            
            fechas = sorted(incendio_actual['datetime'].unique())
            puntos_acumulados = []
            poligono_anterior = None
            
            for fecha_actual in fechas:
                puntos_dia = incendio_actual[incendio_actual['datetime'] == fecha_actual]
                
                for _, punto in puntos_dia.iterrows():
                    puntos_acumulados.append([punto.geometry.x, punto.geometry.y])
                
                poligono_actual = None
                
                if len(puntos_acumulados) >= 3:
                    try:
                        tri = Delaunay(np.array(puntos_acumulados))
                        triangulos = []
                        
                        for simplex in tri.simplices:
                            triangle_coords = [puntos_acumulados[i] for i in simplex]
                            triangle = Polygon(triangle_coords)
                            
                            coords = list(triangle.exterior.coords)
                            max_lado = max([
                                Point(coords[i]).distance(Point(coords[i+1])) 
                                for i in range(len(coords)-1)
                            ])
                            
                            area_ha = triangle.area / 10000
                            
                            if max_lado <= 2000 and area_ha <= 500:
                                triangulos.append(triangle)
                        
                        if triangulos:
                            poligono_actual = unary_union(triangulos)
                            
                            if poligono_anterior is not None:
                                poligono_actual = unary_union([poligono_actual, poligono_anterior])
                            
                            poligono_anterior = poligono_actual
                        else:
                            poligono_actual = poligono_anterior
                            
                    except Exception as e:
                        poligono_actual = poligono_anterior
                        
                elif len(puntos_acumulados) == 2:
                    points = [Point(p) for p in puntos_acumulados]
                    poligono_actual = unary_union(points).convex_hull
                else:
                    poligono_actual = Point(puntos_acumulados[0]) if puntos_acumulados else None
                
                if poligono_actual is not None and not poligono_actual.is_empty:
                    resultado = {
                        'evento_id': evento,
                        'fecha': pd.to_datetime(fecha_actual),
                        'geometry': poligono_actual
                    }
                    resultados_finales.append(resultado)
        
        if not resultados_finales:
            return gpd.GeoDataFrame()
        
        resultado_gdf = gpd.GeoDataFrame(resultados_finales, crs='EPSG:32717')
        return resultado_gdf
    
    def remove_overlaps(self, incendios):
        print("Paso 4: Eliminando sobreposiciones...")
        
        incendios = incendios.sort_values(['evento_id', 'fecha']).reset_index(drop=True)
        nuevos_poligonos = []
        eventos_unicos = incendios['evento_id'].unique()
        
        for evento in tqdm(eventos_unicos, desc="Eliminando sobreposiciones"):
            poligonos_evento = incendios[incendios['evento_id'] == evento].copy()
            geometria_acumulada = None
            
            for idx, row in poligonos_evento.iterrows():
                geom_actual = row.geometry
                
                if geom_actual.is_empty:
                    continue
                
                if geometria_acumulada is None:
                    geom_unica = geom_actual
                else:
                    try:
                        geom_unica = geom_actual.difference(geometria_acumulada)
                    except:
                        continue
                
                if not geom_unica.is_empty:
                    nuevo_row = row.copy()
                    nuevo_row.geometry = geom_unica
                    nuevos_poligonos.append(nuevo_row)
                    
                    if geometria_acumulada is None:
                        geometria_acumulada = geom_unica
                    else:
                        geometria_acumulada = unary_union([geometria_acumulada, geom_unica])
        
        if not nuevos_poligonos:
            return gpd.GeoDataFrame()
        
        datos_finales = gpd.GeoDataFrame(nuevos_poligonos, crs='EPSG:32717')
        return datos_finales
    
    def assign_location_and_calculate(self, incendios):
        print("Paso 5: Asignando ubicación y calculando métricas...")
        
        try:
            provincias = gpd.read_file(self.provinces_path)
        except Exception as e:
            print(f"Error cargando archivo de provincias: {e}")
            return gpd.GeoDataFrame()
        
        if provincias.crs != incendios.crs:
            provincias = provincias.to_crs(incendios.crs)
        
        # Primero hacer el join espacial con los evento_id originales
        incendios_inicio = (incendios.sort_values(['evento_id', 'fecha'])
                           .groupby('evento_id')
                           .first()
                           .reset_index())
        
        info_ubicacion = gpd.sjoin(incendios_inicio, provincias, how='left', predicate='intersects')
        info_ubicacion = (info_ubicacion.groupby('evento_id').first().reset_index())
        
        ubicacion_cols = ['evento_id', 'DPA_DESPRO', 'DPA_DESCAN', 'DPA_DESPAR']
        info_ubicacion = info_ubicacion[ubicacion_cols]
        
        info_ubicacion = info_ubicacion.rename(columns={
            'DPA_DESPRO': 'dpa_despro',
            'DPA_DESCAN': 'dpa_descan',
            'DPA_DESPAR': 'dpa_despar'
        })
        
        # Merge con información de ubicación usando evento_id original
        incendios_con_ubicacion = incendios.merge(info_ubicacion, on='evento_id', how='left')
        
        # Filtrar solo los que tienen ubicación en Ecuador
        incendios_limpios = incendios_con_ubicacion.dropna(subset=['evento_id', 'fecha', 'dpa_despro'])
        
        if incendios_limpios.empty:
            print("No hay datos válidos después de la limpieza")
            return gpd.GeoDataFrame()
        
        print("Calculando superficies y métricas...")
        incendios_limpios['superficie_ha_individual'] = incendios_limpios.geometry.area / 10000
        incendios_calculados = incendios_limpios.copy()
        
        def calcular_metricas_evento(grupo):
            grupo = grupo.sort_values('fecha').reset_index(drop=True)
            grupo['dia_del_incendio'] = range(1, len(grupo) + 1)
            grupo['superficie_ha_total'] = grupo['superficie_ha_individual'].sum()
            grupo['fecha_inicio'] = grupo['fecha'].min()
            grupo['fecha_fin'] = grupo['fecha'].max()
            grupo['duracion_dias'] = (grupo['fecha_fin'] - grupo['fecha_inicio']).dt.days + 1
            return grupo
        
        incendios_calculados = (incendios_calculados.groupby('evento_id')
                               .apply(calcular_metricas_evento)
                               .reset_index(drop=True))
        
        # AHORA generar IDs únicos por evento después de todos los cálculos
        print("Generando IDs únicos por evento...")
        
        # Para cada evento, tomar el primer polígono para generar ID único
        eventos_unicos = incendios_calculados.groupby('evento_id').first().reset_index()
        eventos_unicos['evento_id_unico'] = eventos_unicos.apply(
            lambda row: self.generate_unique_id(row['fecha'], row['geometry']), 
            axis=1
        )
        
        # Crear mapeo evento_id original → evento_id único
        mapeo_ids = dict(zip(eventos_unicos['evento_id'], eventos_unicos['evento_id_unico']))
        
        # Aplicar el mapeo a todos los registros
        incendios_calculados['evento_id'] = incendios_calculados['evento_id'].map(mapeo_ids)
        
        eventos_grandes = incendios_calculados[incendios_calculados['superficie_ha_total'] >= 10].copy()
        
        print(f"Eventos totales procesados: {incendios_calculados['evento_id'].nunique()}")
        print(f"Eventos grandes (>=10 ha): {eventos_grandes['evento_id'].nunique()}")
        print(f"Polígonos totales: {len(incendios_calculados)}")
        print(f"Polígonos de eventos grandes: {len(eventos_grandes)}")
        
        return incendios_calculados
    
    def save_to_supabase(self, data):
        try:
            eventos_grandes = data[data['superficie_ha_total'] >= 10].copy()
            eventos_grandes = eventos_grandes[eventos_grandes.geometry.geom_type == 'Polygon'].copy()
            
            if eventos_grandes.empty:
                print("No hay polígonos grandes (>=10 ha) para subir")
                return True
            
            # Cargar evento_ids existentes
            existing_ids = self.load_existing_ids_from_supabase()
            
            # Los evento_id ya están generados correctamente en assign_location_and_calculate
            # Solo filtrar eventos que no existan en Supabase
            eventos_nuevos = eventos_grandes[~eventos_grandes['evento_id'].isin(existing_ids)].copy()
            
            if eventos_nuevos.empty:
                print("✅ No hay eventos nuevos para subir")
                return True
            
            print(f"📦 Eventos nuevos a subir: {len(eventos_nuevos)}")
            print(f"📦 Eventos únicos nuevos: {eventos_nuevos['evento_id'].nunique()}")
            
            # Preparar datos para Supabase
            data_copy = eventos_nuevos.copy()
            data_copy = data_copy.to_crs('EPSG:4326')
            data_copy['geom'] = data_copy['geometry'].apply(lambda x: x.wkt)
            data_copy = data_copy.drop('geometry', axis=1)
            
            for col in data_copy.select_dtypes(include=['datetime64']).columns:
                data_copy[col] = data_copy[col].dt.strftime('%Y-%m-%d')
            
            data_copy = data_copy.fillna('')
            
            # Asegurar que evento_id sea integer sin decimales
            data_copy['evento_id'] = data_copy['evento_id'].astype(int)
            
            records = data_copy.to_dict('records')
            
            url = f"{self.supabase_url}/rest/v1/incendios_grandes"
            headers = {
                'apikey': self.supabase_key,
                'Authorization': f'Bearer {self.supabase_key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            # Solo INSERT (no DELETE) - procesamiento incremental
            for i in range(0, len(records), 1000):
                batch = records[i:i+1000]
                response = requests.post(url, json=batch, headers=headers)
                if response.status_code not in [200, 201]:
                    print(f"Error subiendo batch {i//1000 + 1}: {response.status_code}")
                    print(f"Response: {response.text}")
                    return False
            
            print(f"✅ Subidos {len(records)} polígonos nuevos a Supabase")
            return True
            
        except Exception as e:
            print(f"Error subiendo a Supabase: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_all(self):
        print("=== INICIANDO PROCESAMIENTO COMPLETO DE INCENDIOS ===\n")
        
        try:
            fire_data = self.update_fire_data()
            if fire_data.empty:
                print("No hay datos de incendios para procesar")
                return {"success": False, "error": "No hay datos de incendios"}
            
            fire_with_ids = self.assign_event_ids(fire_data)
            if fire_with_ids.empty:
                print("No se pudieron asignar IDs de eventos")
                return {"success": False, "error": "No se pudieron asignar IDs de eventos"}
            
            polygons = self.create_polygons(fire_with_ids)
            if polygons.empty:
                print("No se pudieron crear polígonos")
                return {"success": False, "error": "No se pudieron crear polígonos"}
            
            no_overlaps = self.remove_overlaps(polygons)
            if no_overlaps.empty:
                print("Error eliminando sobreposiciones")
                return {"success": False, "error": "Error eliminando sobreposiciones"}
            
            todos_eventos = self.assign_location_and_calculate(no_overlaps)
            if todos_eventos is None or todos_eventos.empty:
                print("Error en cálculos finales")
                return {"success": False, "error": "Error en cálculos finales"}
            
            eventos_grandes = todos_eventos[todos_eventos['superficie_ha_total'] >= 10]
            
            success = self.save_to_supabase(todos_eventos)
            
            result = {
                "success": True,
                "message": "Procesamiento completado exitosamente",
                "stats": {
                    "total_poligonos": len(todos_eventos),
                    "eventos_unicos": todos_eventos['evento_id'].nunique(),
                    "eventos_grandes": len(eventos_grandes),
                    "superficie_total": todos_eventos['superficie_ha_individual'].sum(),
                    "uploaded": success
                },
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            
            print(f"\n=== PROCESAMIENTO COMPLETADO ===")
            print(f"✅ Total polígonos: {len(todos_eventos)}")
            print(f"✅ Eventos únicos: {todos_eventos['evento_id'].nunique()}")
            print(f"✅ Eventos grandes: {len(eventos_grandes)}")
            print(f"✅ Subida a Supabase: {'OK' if success else 'ERROR'}")
            
            return result
            
        except Exception as e:
            print(f"Error en el procesamiento: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
