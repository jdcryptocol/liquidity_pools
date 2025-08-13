# lambda_function.py

import json
import os
import yaml  # Necesitarás la librería PyYAML
import requests # Necesitarás la librería requests
import pandas as pd
from binance.client import Client
from binance.error import BinanceAPIException

# --- FUNCIONES AUXILIARES ---

def cargar_configuracion():
    """Carga la configuración desde config.yaml o config.json."""
    if os.path.exists('config.yaml'):
        with open('config.yaml', 'r') as f:
            return yaml.safe_load(f)
    elif os.path.exists('config.json'):
        with open('config.json', 'r') as f:
            return json.load(f)
    else:
        raise FileNotFoundError("No se encontró el archivo de configuración (config.yaml o config.json)")

def enviar_mensaje_telegram(mensaje, token, chat_id):
    """Envía un mensaje formateado a un chat de Telegram."""
    # Escapar caracteres especiales para MarkdownV2 de Telegram
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        mensaje = mensaje.replace(char, f'\\{char}')
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': mensaje,
        'parse_mode': 'MarkdownV2'
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Lanza un error si la petición falla (ej. 400, 403)
        print("Mensaje enviado a Telegram exitosamente.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error al enviar mensaje a Telegram: {e}")
        # Imprime la respuesta de Telegram para depuración
        if e.response is not None:
            print(f"Respuesta de Telegram: {e.response.text}")
        return False

# --- FUNCIÓN PRINCIPAL DE ANÁLISIS ---

def analizar_mercado(config):
    """Realiza el análisis y devuelve un reporte de texto."""
    symbol = config['symbol']
    aggregation_level = config['aggregation_level']
    lookback_period = config['lookback_period']
    leverages = [10, 20, 50]
    
    client = Client()

    # 1. Análisis de Pools de Liquidez (Libro de Órdenes SPOT)
    depth = client.get_order_book(symbol=symbol, limit=1000)
    current_price = float(depth['bids'][0][0])
    
    bids = pd.DataFrame(depth['bids'], columns=['price', 'quantity'], dtype=float)
    bids['price_level'] = (bids['price'] // aggregation_level) * aggregation_level
    liquidity_bids = bids.groupby('price_level')['quantity'].sum().nlargest(5)
    
    asks = pd.DataFrame(depth['asks'], columns=['price', 'quantity'], dtype=float)
    asks['price_level'] = (asks['price'] // aggregation_level) * aggregation_level
    liquidity_asks = asks.groupby('price_level')['quantity'].sum().nlargest(5)

    # 2. Estimación de Zonas de Liquidación (FUTUROS)
    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1HOUR, lookback_period)
    df_futures = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_vol', 'trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
    df_futures[['high', 'low']] = df_futures[['high', 'low']].apply(pd.to_numeric)
    
    punto_alto = df_futures['high'].max()
    punto_bajo = df_futures['low'].min()
    
    # 3. Construir el Reporte de Texto para Telegram
    reporte = f"*🔔 Informe de Mercado: ${symbol}*\n"
    reporte += f"_Fecha: {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_\n\n"
    reporte += f"*Precio Actual:* `${current_price:,.2f}`\n\n"
    reporte += "*--- Muros de Liquidez (Spot) ---\n*"
    reporte += "*Soportes (Compras)*\n"
    for level, qty in liquidity_bids.items():
        reporte += f"  - `${int(level):,}`: `{qty:.2f}` BTC\n"
    reporte += "*Resistencias (Ventas)*\n"
    for level, qty in liquidity_asks.items():
        reporte += f"  - `${int(level):,}`: `{qty:.2f}` BTC\n\n"
    
    reporte += f"*--- Zonas de Liquidación (Futuros) ---\n*"
    reporte += f"_Basado en rango de {lookback_period}_\n"
    reporte += f"Entrada Shorts (Alto): `${punto_alto:,.2f}`\n"
    reporte += f"Entrada Longs (Bajo): `${punto_bajo:,.2f}`\n\n"

    reporte += "*🎯 Liquidaciones de SHORTS (Imanes)*\n"
    for lev in leverages:
        liq_price = punto_alto * (1 + (1 / lev))
        reporte += f"  - `{lev:>2}x`: ~`${liq_price:,.2f}`\n"
        
    reporte += "\n*☠️ Liquidaciones de LONGS (Peligro)*\n"
    for lev in leverages:
        liq_price = punto_bajo * (1 - (1 / lev))
        reporte += f"  - `{lev:>2}x`: ~`${liq_price:,.2f}`\n"
        
    return reporte

# --- HANDLER DE AWS LAMBDA ---

def lambda_handler(event, context):
    """Punto de entrada para AWS Lambda."""
    try:
        config = cargar_configuracion()
        
        reporte_texto = analizar_mercado(config)
        
        # Enviar el reporte a Telegram
        enviado = enviar_mensaje_telegram(
            mensaje=reporte_texto,
            token=config['telegram']['bot_token'],
            chat_id=config['telegram']['chat_id']
        )
        
        if not enviado:
            raise Exception("El envío a Telegram falló.")
            
        return {'statusCode': 200, 'body': json.dumps('Reporte generado y enviado a Telegram.')}

    except Exception as e:
        print(f"ERROR en la ejecución principal: {e}")
        # Opcional: Enviar un mensaje de error a Telegram
        try:
            config = cargar_configuracion()
            enviar_mensaje_telegram(f"ERROR en el bot analizador: {e}", config['telegram']['bot_token'], config['telegram']['chat_id'])
        except:
            pass # Falla silenciosamente si no puede enviar el error
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
