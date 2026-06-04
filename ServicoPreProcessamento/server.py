import grpc
from concurrent import futures
import datetime
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import preprocessamento_pb2
import preprocessamento_pb2_grpc

# Aliases de tipo para normalização de nomes
TIPO_ALIASES = {
    'TEMPERATURA': 'TEMP',
    'TEMP':        'TEMP',
    'HUM':         'HUMIDADE',
    'HUMIDADE':    'HUMIDADE',
    'PM25':        'PM2.5',
    'PM2_5':       'PM2.5',
    'PM2.5':       'PM2.5',
    'NO2':         'NO2',
    'DIOXIDO_NITROGENIO': 'NO2',
    'RUIDO':       'RUIDO',
    'NOISE':       'RUIDO',
    'SOUND':       'RUIDO',
}

# Intervalos de valores válidos por tipo (min, max)
RANGES = {
    'TEMP':     (-40.0,  80.0),
    'HUMIDADE': (  0.0, 100.0),
    'PM2.5':    (  0.0, 999.0),
    'NO2':      (  0.0, 2000.0),
    'RUIDO':    (  0.0, 200.0),
}

# Unidades padrão por tipo
UNIDADES = {
    'TEMP':     '°C',
    'HUMIDADE': '%',
    'PM2.5':    'µg/m³',
    'NO2':      'µg/m³',
    'RUIDO':    'dB',
}


def normalizar_tipo(tipo: str) -> str:
    return TIPO_ALIASES.get(tipo.upper().strip(), tipo.upper().strip())


def converter_valor(tipo: str, valor: float, unidade: str) -> float:
    """Converte o valor para a unidade padrão do tipo."""
    if tipo == 'TEMP':
        # Heurística: Fahrenheit se unidade indica 'F' ou valor > 60 sem indicação de Celsius
        e_fahrenheit = (unidade and 'F' in unidade.upper() and '°C' not in unidade) or \
                       (valor > 60.0 and not (unidade and '°C' in unidade))
        if e_fahrenheit:
            valor = (valor - 32.0) * 5.0 / 9.0
    elif tipo == 'PM2.5':
        # mg/m³ → µg/m³ (multiplicar por 1000)
        if unidade and 'mg' in unidade.lower() and 'µg' not in unidade.lower():
            valor = valor * 1000.0
    return round(valor, 4)


def validar_range(tipo: str, valor: float):
    if tipo in RANGES:
        lo, hi = RANGES[tipo]
        if not (lo <= valor <= hi):
            return False, f"Valor {valor:.2f} fora do intervalo [{lo}, {hi}] para {tipo}"
    return True, ""


class ServicoPreProcessamentoServicer(preprocessamento_pb2_grpc.ServicoPreProcessamentoServicer):

    def PreProcessar(self, request, context):
        try:
            tipo_norm  = normalizar_tipo(request.tipo)
            valor_conv = converter_valor(tipo_norm, request.valor, request.unidade)

            valido, msg_erro = validar_range(tipo_norm, valor_conv)
            if not valido:
                print(f"[PRÉ-PROC] REJEITADO {request.sensor_id}|{request.tipo}={request.valor}: {msg_erro}")
                return preprocessamento_pb2.DadoProcessado(
                    sucesso=False,
                    sensor_id=request.sensor_id,
                    zona=request.zona,
                    tipo=tipo_norm,
                    erro=msg_erro
                )

            unidade_norm = UNIDADES.get(tipo_norm, '')
            ts = request.timestamp or datetime.datetime.now().isoformat(timespec='seconds')

            print(f"[PRÉ-PROC] {request.sensor_id}|{request.tipo}={request.valor:.4f}"
                  f" → {tipo_norm}={valor_conv} {unidade_norm}")

            return preprocessamento_pb2.DadoProcessado(
                sucesso=True,
                sensor_id=request.sensor_id,
                zona=request.zona,
                tipo=tipo_norm,
                valor_normalizado=valor_conv,
                unidade_normalizada=unidade_norm,
                timestamp=ts,
                erro=""
            )

        except Exception as e:
            print(f"[PRÉ-PROC] ERRO interno: {e}")
            return preprocessamento_pb2.DadoProcessado(
                sucesso=False,
                sensor_id=request.sensor_id,
                zona=request.zona,
                tipo=request.tipo,
                erro=str(e)
            )


def serve(port: int = 50052):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    preprocessamento_pb2_grpc.add_ServicoPreProcessamentoServicer_to_server(
        ServicoPreProcessamentoServicer(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[PRÉ-PROCESSAMENTO] gRPC a escutar na porta {port}")
    print(f"[PRÉ-PROCESSAMENTO] Normaliza tipos, converte unidades, valida intervalos.")
    server.wait_for_termination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Serviço de Pré-processamento gRPC')
    parser.add_argument('--port', type=int, default=50052, help='Porta gRPC (padrão: 50052)')
    args = parser.parse_args()
    serve(args.port)
