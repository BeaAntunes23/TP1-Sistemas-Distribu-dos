import grpc
from concurrent import futures
import sqlite3
import statistics
import datetime
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analise_pb2
import analise_pb2_grpc

DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'ServerApp', 'dados', 'sensor_data.db'
)

# Limiares de risco baseados nas diretivas da OMS
LIMITES = {
    'TEMP': [
        (35, 'MUITO_ALTO', 'Temperatura extrema — risco de golpe de calor'),
        (30, 'ALTO',       'Temperatura elevada — desconforto térmico'),
        (24, 'MODERADO',   'Temperatura quente'),
        (0,  'BAIXO',      'Temperatura confortável'),
    ],
    'HUMIDADE': [
        (80, 'ALTO',     'Humidade muito elevada — risco de bolores e doenças respiratórias'),
        (70, 'MODERADO', 'Humidade elevada'),
        (30, 'BAIXO',    'Humidade normal'),
        (0,  'MODERADO', 'Ar demasiado seco — irritações respiratórias'),
    ],
    'PM2.5': [
        (75, 'MUITO_ALTO', 'PM2.5 perigoso — risco grave para a saúde'),
        (35, 'ALTO',       'PM2.5 elevado — grupos sensíveis afetados'),
        (12, 'MODERADO',   'PM2.5 moderado'),
        (0,  'BAIXO',      'Qualidade do ar boa'),
    ],
    'NO2': [
        (200, 'MUITO_ALTO', 'NO2 perigoso — risco grave respiratório'),
        (100, 'ALTO',       'NO2 elevado — grupos sensíveis afetados'),
        (40,  'MODERADO',   'NO2 moderado'),
        (0,   'BAIXO',      'Qualidade do ar boa (NO2)'),
    ],
    'RUIDO': [
        (85, 'MUITO_ALTO', 'Ruído perigoso — risco de perda auditiva'),
        (70, 'ALTO',       'Ruído elevado — stresse e perturbação do sono'),
        (55, 'MODERADO',   'Ruído moderado'),
        (0,  'BAIXO',      'Nível de ruído confortável'),
    ],
}


def calcular_risco(tipo, media):
    limites = LIMITES.get(tipo.upper())
    if not limites:
        return 'DESCONHECIDO', f'Sem limiares definidos para {tipo}'
    for limite, nivel, descricao in sorted(limites, key=lambda x: x[0], reverse=True):
        if media >= limite:
            return nivel, descricao
    return 'BAIXO', 'Dentro dos parâmetros normais'


class ServicoAnaliseServicer(analise_pb2_grpc.ServicoAnaliseServicer):

    def __init__(self, db_path):
        self.db_path = db_path

    def AnalisarDados(self, request, context):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            query = "SELECT valor FROM medicoes WHERE UPPER(tipo) = UPPER(?)"
            params = [request.tipo]

            if request.zona:
                query += " AND zona = ?"
                params.append(request.zona)

            query += " ORDER BY id DESC"

            if request.ultimas_n > 0:
                query += " LIMIT ?"
                params.append(request.ultimas_n)

            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return analise_pb2.ResultadoAnalise(
                    sucesso=False,
                    tipo=request.tipo,
                    zona=request.zona,
                    erro=f"Sem dados para tipo={request.tipo}, zona={request.zona or 'todas'}"
                )

            valores = [float(r[0]) for r in rows]
            media  = statistics.mean(valores)
            minimo = min(valores)
            maximo = max(valores)
            desvio = statistics.stdev(valores) if len(valores) > 1 else 0.0
            nivel_risco, descricao = calcular_risco(request.tipo, media)
            ts = datetime.datetime.now().isoformat(timespec='seconds')

            print(f"[ANÁLISE] {request.tipo}/{request.zona or 'TODAS'}: "
                  f"n={len(valores)} média={media:.2f} risco={nivel_risco}")

            return analise_pb2.ResultadoAnalise(
                sucesso=True,
                tipo=request.tipo,
                zona=request.zona,
                media=media,
                minimo=minimo,
                maximo=maximo,
                desvio_padrao=desvio,
                nivel_risco=nivel_risco,
                descricao_risco=descricao,
                timestamp_analise=ts,
                num_medicoes=len(valores)
            )

        except Exception as e:
            return analise_pb2.ResultadoAnalise(
                sucesso=False,
                tipo=request.tipo,
                zona=request.zona,
                erro=str(e)
            )


def serve(db_path, port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analise_pb2_grpc.add_ServicoAnaliseServicer_to_server(
        ServicoAnaliseServicer(db_path), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"[SERVIÇO ANÁLISE] gRPC a escutar na porta {port}")
    print(f"[SERVIÇO ANÁLISE] Base de dados: {os.path.abspath(db_path)}")
    server.wait_for_termination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Serviço de Análise gRPC')
    parser.add_argument('--port', type=int,    default=50051,     help='Porta gRPC')
    parser.add_argument('--db',   type=str,    default=DEFAULT_DB, help='Caminho para a base de dados SQLite')
    args = parser.parse_args()
    serve(args.db, args.port)
