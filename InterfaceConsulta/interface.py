import sqlite3
import datetime
import os
import sys

# Stubs gRPC gerados em ServicoAnalise/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'ServicoAnalise'))

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'ServerApp', 'dados', 'sensor_data.db'
)
GRPC_URL = 'localhost:50051'

GRPC_OK = False
try:
    import grpc
    import analise_pb2
    import analise_pb2_grpc
    GRPC_OK = True
except ImportError:
    print("[AVISO] Modulos gRPC nao encontrados.")
    print("  Execute primeiro: cd ServicoAnalise && setup.bat")


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def db():
    if not os.path.exists(DB_PATH):
        print(f"\n[ERRO] Base de dados nao encontrada: {os.path.abspath(DB_PATH)}")
        print("  Inicie o servidor e aguarde dados dos sensores.")
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def tabela(colunas, linhas, larguras):
    if not linhas:
        print("  (sem resultados)\n")
        return
    sep  = "+-" + "-+-".join("-" * w for w in larguras) + "-+"
    hdr  = "| " + " | ".join(str(c).ljust(w) for c, w in zip(colunas, larguras)) + " |"
    print(sep); print(hdr); print(sep)
    for l in linhas:
        row = "| " + " | ".join(
            str(l[i] if i < len(l) else '').ljust(larguras[i])
            for i in range(len(colunas))
        ) + " |"
        print(row)
    print(sep)


def menu():
    print("\n" + "=" * 62)
    print("   MONITORIZACAO AMBIENTAL URBANA  —  Interface de Consulta")
    print("=" * 62)
    print("  1. Ver medicoes recentes")
    print("  2. Pesquisar medicoes")
    print("  3. Ver analises guardadas")
    print("  4. Solicitar nova analise  (gRPC)")
    print("  5. Resumo de riscos de saude publica")
    print("  0. Sair")
    print("-" * 62)
    return input("  Opcao: ").strip()


# ---------------------------------------------------------------------------
# Opções do menu
# ---------------------------------------------------------------------------

def ver_recentes():
    conn = db()
    if conn is None: return
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, sensor_id, zona, tipo, ROUND(valor,2) "
        "FROM medicoes ORDER BY id DESC LIMIT 20"
    )
    linhas = cur.fetchall()
    conn.close()
    print("\n--- Ultimas 20 Medicoes ---")
    tabela(['Timestamp', 'Sensor', 'Zona', 'Tipo', 'Valor'],
           linhas, [19, 8, 15, 8, 8])


def pesquisar():
    print("\n--- Pesquisar Medicoes ---")
    tipo   = input("  Tipo  (TEMP/HUMIDADE/PM2.5/NO2/RUIDO, Enter=todos): ").strip().upper()
    zona   = input("  Zona  (Enter=todas): ").strip()
    sensor = input("  Sensor (Enter=todos): ").strip()
    inicio = input("  Data inicio YYYY-MM-DD (Enter=sem limite): ").strip()
    fim    = input("  Data fim    YYYY-MM-DD (Enter=hoje): ").strip()

    q = "SELECT timestamp, sensor_id, zona, tipo, ROUND(valor,2) FROM medicoes WHERE 1=1"
    p = []
    if tipo:   q += " AND UPPER(tipo)=?";     p.append(tipo)
    if zona:   q += " AND zona=?";            p.append(zona)
    if sensor: q += " AND sensor_id=?";       p.append(sensor)
    if inicio: q += " AND timestamp>=?";      p.append(inicio)
    if fim:    q += " AND timestamp<=?";      p.append(fim + "T23:59:59")
    q += " ORDER BY timestamp DESC LIMIT 50"

    conn = db()
    if conn is None: return
    cur = conn.cursor()
    cur.execute(q, p)
    linhas = cur.fetchall()
    conn.close()
    print(f"\n--- {len(linhas)} resultado(s) ---")
    tabela(['Timestamp', 'Sensor', 'Zona', 'Tipo', 'Valor'],
           linhas, [19, 8, 15, 8, 8])


def ver_analises():
    conn = db()
    if conn is None: return
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp_analise, tipo, zona, num_medicoes, "
        "ROUND(media,2), ROUND(minimo,2), ROUND(maximo,2), nivel_risco "
        "FROM analises ORDER BY id DESC LIMIT 20"
    )
    linhas = cur.fetchall()
    conn.close()
    print("\n--- Ultimas 20 Analises ---")
    tabela(['Timestamp', 'Tipo', 'Zona', 'N', 'Media', 'Min', 'Max', 'Risco'],
           linhas, [19, 8, 15, 5, 7, 7, 7, 10])


def nova_analise():
    if not GRPC_OK:
        print("\n[ERRO] gRPC nao disponivel.")
        print("  Execute: cd ServicoAnalise && setup.bat")
        print("  Depois:  python server.py")
        return

    print("\n--- Solicitar Nova Analise ---")
    tipo = input("  Tipo (TEMP/HUMIDADE/PM2.5/NO2/RUIDO): ").strip().upper()
    zona = input("  Zona (Enter=todas): ").strip()
    n_str = input("  Ultimas N medicoes (Enter=100): ").strip()
    ultimas_n = int(n_str) if n_str.isdigit() else 100

    try:
        channel = grpc.insecure_channel(GRPC_URL)
        stub    = analise_pb2_grpc.ServicoAnaliseStub(channel)
        pedido  = analise_pb2.PedidoAnalise(tipo=tipo, zona=zona, ultimas_n=ultimas_n)
        r       = stub.AnalisarDados(pedido, timeout=10)

        if r.sucesso:
            print(f"\n{'=' * 52}")
            print(f"  Tipo  : {r.tipo}   Zona: {r.zona or 'TODAS'}")
            print(f"  N med.: {r.num_medicoes}")
            print(f"  Media : {r.media:.2f}")
            print(f"  Min   : {r.minimo:.2f}   Max: {r.maximo:.2f}")
            print(f"  Desvio: {r.desvio_padrao:.2f}")
            print(f"  Risco : {r.nivel_risco}")
            print(f"  Info  : {r.descricao_risco}")
            print(f"{'=' * 52}")
        else:
            print(f"\n[SEM DADOS] {r.erro}")
            print("  Aguarde o servidor receber medicoes dos sensores.")

    except grpc.RpcError as e:
        print(f"\n[ERRO gRPC] {e.code()}: {e.details()}")
        print("  Verifique se ServicoAnalise esta em execucao (python server.py).")


def resumo_riscos():
    conn = db()
    if conn is None: return
    cur = conn.cursor()
    cur.execute("""
        SELECT a.tipo, a.zona, a.nivel_risco, a.descricao_risco, a.timestamp_analise
        FROM analises a
        INNER JOIN (
            SELECT tipo, zona, MAX(id) as mid FROM analises GROUP BY tipo, zona
        ) latest ON a.tipo=latest.tipo AND a.zona=latest.zona AND a.id=latest.mid
        ORDER BY CASE a.nivel_risco
            WHEN 'MUITO_ALTO' THEN 1 WHEN 'ALTO' THEN 2
            WHEN 'MODERADO'   THEN 3 WHEN 'BAIXO' THEN 4 ELSE 5 END
    """)
    linhas = cur.fetchall()
    conn.close()

    if not linhas:
        print("\n  Sem analises disponiveis.")
        print("  Use a opcao 4 para solicitar uma analise primeiro.\n")
        return

    print("\n--- Resumo de Riscos de Saude Publica ---")
    print("  (analise mais recente por tipo/zona)\n")
    tabela(['Tipo', 'Zona', 'Risco', 'Descricao', 'Ultima Analise'],
           linhas, [8, 15, 10, 42, 19])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n  A iniciar interface de consulta...")
    if not os.path.exists(DB_PATH):
        print(f"  [AVISO] Base de dados ainda nao existe: {os.path.abspath(DB_PATH)}")
        print("  O servidor precisa de estar em execucao e receber dados.\n")

    opcoes = {'1': ver_recentes, '2': pesquisar, '3': ver_analises,
              '4': nova_analise, '5': resumo_riscos}

    while True:
        try:
            op = menu()
            if op == '0':
                print("\n  Ate logo!\n")
                break
            elif op in opcoes:
                opcoes[op]()
            else:
                print("\n  Opcao invalida.")
        except KeyboardInterrupt:
            print("\n\n  Interrompido. Ate logo!")
            break
        except Exception as e:
            print(f"\n[ERRO] {e}")


if __name__ == '__main__':
    main()
