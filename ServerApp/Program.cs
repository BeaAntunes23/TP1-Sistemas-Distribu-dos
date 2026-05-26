using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Grpc.Net.Client;
using GrpcAnalise;

namespace Servidor
{
    class Program
    {
        private const int PortaTcp = 6000;
        private const string GrpcUrl = "http://localhost:50051";
        private static readonly string DirDados = "dados";
        private static readonly string DbPath = Path.Combine(DirDados, "sensor_data.db");

        private static readonly Dictionary<string, Mutex> fileMutexes = new();
        private static readonly object fileMutexesMeta = new();
        private static readonly Dictionary<string, string> sessoesVideo = new();
        private static readonly object videoLock = new();
        private static ServicoAnalise.ServicoAnaliseClient? grpcClient;

        static async Task Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            Directory.CreateDirectory(DirDados);
            InicializarBD();
            InicializarGrpc();

            _ = Task.Run(() => AnalisePeriodicaAsync(CancellationToken.None));

            var listener = new TcpListener(IPAddress.Any, PortaTcp);
            listener.Start();

            Console.WriteLine($"[SERVIDOR] À escuta na porta {PortaTcp}");
            Console.WriteLine($"[SERVIDOR] Base de dados: {Path.GetFullPath(DbPath)}");
            Console.WriteLine($"[SERVIDOR] Análise gRPC: {GrpcUrl}");

            while (true)
            {
                var cliente = await listener.AcceptTcpClientAsync();
                Console.WriteLine($"[SERVIDOR] Gateway ligado: {cliente.Client.RemoteEndPoint}");
                _ = Task.Run(() => TratarGateway(cliente));
            }
        }

        static void InicializarBD()
        {
            using var conn = new SqliteConnection($"Data Source={DbPath}");
            conn.Open();

            using var pragmaCmd = conn.CreateCommand();
            pragmaCmd.CommandText = "PRAGMA journal_mode=WAL;";
            pragmaCmd.ExecuteNonQuery();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS medicoes (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp  TEXT NOT NULL,
                    sensor_id  TEXT NOT NULL,
                    zona       TEXT NOT NULL,
                    tipo       TEXT NOT NULL,
                    valor      REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS analises (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_analise TEXT NOT NULL,
                    tipo             TEXT NOT NULL,
                    zona             TEXT NOT NULL,
                    num_medicoes     INTEGER NOT NULL,
                    media            REAL NOT NULL,
                    minimo           REAL NOT NULL,
                    maximo           REAL NOT NULL,
                    desvio_padrao    REAL NOT NULL,
                    nivel_risco      TEXT NOT NULL,
                    descricao_risco  TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_med_tipo_zona  ON medicoes(tipo, zona);
                CREATE INDEX IF NOT EXISTS idx_med_timestamp  ON medicoes(timestamp);
                CREATE INDEX IF NOT EXISTS idx_anal_tipo_zona ON analises(tipo, zona);
            ";
            cmd.ExecuteNonQuery();

            Console.WriteLine("[SERVIDOR] Base de dados inicializada.");
        }

        static void InicializarGrpc()
        {
            try
            {
                var channel = GrpcChannel.ForAddress(GrpcUrl);
                grpcClient = new ServicoAnalise.ServicoAnaliseClient(channel);
                Console.WriteLine($"[SERVIDOR] Cliente gRPC pronto ({GrpcUrl})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SERVIDOR] Aviso: gRPC não inicializado — {ex.Message}");
            }
        }

        static async Task TratarGateway(TcpClient clienteGateway)
        {
            string gatewayId = "?";

            using (clienteGateway)
            using (var stream = clienteGateway.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            {
                try
                {
                    string? mensagem;
                    while ((mensagem = await reader.ReadLineAsync()) != null)
                    {
                        var partes = mensagem.Split('|');
                        var cmd = partes[0].ToUpperInvariant();

                        switch (cmd)
                        {
                            case "HELLO_GATEWAY":
                                await writer.WriteLineAsync("ACK|HELLO_GATEWAY");
                                Console.WriteLine($"[SERVIDOR] HELLO_GATEWAY recebido.");
                                break;

                            case "GATEWAY_REGISTER":
                                if (partes.Length >= 2) gatewayId = partes[1];
                                await writer.WriteLineAsync("ACK|GATEWAY_REGISTER");
                                Console.WriteLine($"[SERVIDOR] Gateway registado: {gatewayId}");
                                break;

                            case "SESSION_START":
                                if (partes.Length >= 2) gatewayId = partes[1];
                                await writer.WriteLineAsync("ACK|SESSION_START");
                                Console.WriteLine($"[SERVIDOR] Sessão iniciada: {gatewayId}");
                                break;

                            case "STORE":
                                if (partes.Length < 5) { await writer.WriteLineAsync("ERROR|STORE_FORMAT"); break; }
                                string sId = partes[1], zona = partes[2], tipo = partes[3], valor = partes[4];
                                string ts = DateTime.Now.ToString("s");
                                GuardarMedicaoCSV(Path.Combine(DirDados, $"{tipo.ToUpper()}.csv"), sId, zona, tipo, valor, ts);
                                GuardarMedicaoDB(sId, zona, tipo, valor, ts);
                                Console.WriteLine($"[SERVIDOR] STORE: {sId}|{zona}|{tipo}={valor}");
                                await writer.WriteLineAsync("ACK|STORE");
                                break;

                            case "VIDEO_START":
                                if (partes.Length < 3) { await writer.WriteLineAsync("ERROR|VIDEO_START_FORMAT"); break; }
                                lock (videoLock) { sessoesVideo[partes[1]] = partes[2]; }
                                await writer.WriteLineAsync("ACK|VIDEO_START");
                                Console.WriteLine($"[SERVIDOR] VIDEO_START: sensor={partes[1]} zona={partes[2]}");
                                break;

                            case "VIDEO_END":
                                if (partes.Length < 2) { await writer.WriteLineAsync("ERROR|VIDEO_END_FORMAT"); break; }
                                lock (videoLock) { sessoesVideo.Remove(partes[1]); }
                                await writer.WriteLineAsync("ACK|VIDEO_END");
                                Console.WriteLine($"[SERVIDOR] VIDEO_END: sensor={partes[1]}");
                                break;

                            case "VIDEO_FRAME":
                                if (partes.Length < 4) { await writer.WriteLineAsync("ERROR|VIDEO_FRAME_FORMAT"); break; }
                                bool ativa;
                                lock (videoLock) { ativa = sessoesVideo.ContainsKey(partes[1]); }
                                if (!ativa) { await writer.WriteLineAsync("ERROR|NO_VIDEO_SESSION"); break; }
                                GuardarFrameCSV(Path.Combine(DirDados, "VIDEO_FRAMES.csv"), partes[1], partes[2], partes[3], DateTime.Now.ToString("s"));
                                await writer.WriteLineAsync("ACK|VIDEO_FRAME");
                                break;

                            default:
                                Console.WriteLine($"[SERVIDOR] Comando desconhecido: {cmd}");
                                await writer.WriteLineAsync("ERROR|UNKNOWN_COMMAND");
                                break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] Erro [{gatewayId}]: {ex.Message}");
                }
            }

            Console.WriteLine($"[SERVIDOR] Gateway desligado: {gatewayId}");
        }

        static void GuardarMedicaoDB(string sensorId, string zona, string tipo, string valor, string timestamp)
        {
            if (!double.TryParse(valor,
                    System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out double valorDouble)) return;

            using var conn = new SqliteConnection($"Data Source={DbPath}");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO medicoes (timestamp,sensor_id,zona,tipo,valor) VALUES ($ts,$sid,$zona,$tipo,$val)";
            cmd.Parameters.AddWithValue("$ts",   timestamp);
            cmd.Parameters.AddWithValue("$sid",  sensorId);
            cmd.Parameters.AddWithValue("$zona", zona);
            cmd.Parameters.AddWithValue("$tipo", tipo);
            cmd.Parameters.AddWithValue("$val",  valorDouble);
            cmd.ExecuteNonQuery();
        }

        static void GuardarResultadoDB(ResultadoAnalise r)
        {
            using var conn = new SqliteConnection($"Data Source={DbPath}");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO analises
                (timestamp_analise,tipo,zona,num_medicoes,media,minimo,maximo,desvio_padrao,nivel_risco,descricao_risco)
                VALUES ($ts,$tipo,$zona,$n,$media,$min,$max,$dp,$risco,$desc)";
            cmd.Parameters.AddWithValue("$ts",    r.TimestampAnalise);
            cmd.Parameters.AddWithValue("$tipo",  r.Tipo);
            cmd.Parameters.AddWithValue("$zona",  r.Zona);
            cmd.Parameters.AddWithValue("$n",     r.NumMedicoes);
            cmd.Parameters.AddWithValue("$media", r.Media);
            cmd.Parameters.AddWithValue("$min",   r.Minimo);
            cmd.Parameters.AddWithValue("$max",   r.Maximo);
            cmd.Parameters.AddWithValue("$dp",    r.DesvioPadrao);
            cmd.Parameters.AddWithValue("$risco", r.NivelRisco);
            cmd.Parameters.AddWithValue("$desc",  r.DescricaoRisco);
            cmd.ExecuteNonQuery();
        }

        static async Task AnalisePeriodicaAsync(CancellationToken ct)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);

            while (!ct.IsCancellationRequested)
            {
                if (grpcClient != null)
                {
                    foreach (var zona in ObterZonasAtivas())
                        foreach (var tipo in new[] { "TEMP", "HUMIDADE", "PM2.5", "NO2", "RUIDO" })
                        {
                            try
                            {
                                var pedido = new PedidoAnalise { Tipo = tipo, Zona = zona, UltimasN = 100 };
                                var resultado = await grpcClient.AnalisarDadosAsync(pedido);
                                if (resultado.Sucesso)
                                {
                                    GuardarResultadoDB(resultado);
                                    Console.WriteLine($"[ANÁLISE] {tipo}/{zona}: média={resultado.Media:F2} risco={resultado.NivelRisco}");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ANÁLISE] Erro {tipo}/{zona}: {ex.Message}");
                            }
                        }
                }

                await Task.Delay(TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
            }
        }

        static string[] ObterZonasAtivas()
        {
            try
            {
                using var conn = new SqliteConnection($"Data Source={DbPath}");
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT DISTINCT zona FROM medicoes";
                using var reader = cmd.ExecuteReader();
                var lista = new List<string>();
                while (reader.Read()) lista.Add(reader.GetString(0));
                return lista.Count > 0 ? lista.ToArray() : new[] { "ZONA_ESCOLAR" };
            }
            catch { return new[] { "ZONA_ESCOLAR" }; }
        }

        static void GuardarMedicaoCSV(string ficheiro, string sensorId, string zona,
                                      string tipo, string valor, string timestamp)
        {
            var mutex = ObterMutex(ficheiro);
            mutex.WaitOne();
            try
            {
                bool existe = File.Exists(ficheiro);
                using var sw = new StreamWriter(ficheiro, append: true, Encoding.UTF8);
                if (!existe) sw.WriteLine("timestamp,sensor_id,zona,tipo,valor");
                sw.WriteLine($"{timestamp},{sensorId},{zona},{tipo},{valor}");
            }
            finally { mutex.ReleaseMutex(); }
        }

        static void GuardarFrameCSV(string ficheiro, string sensorId, string zona,
                                    string conteudo, string timestamp)
        {
            var mutex = ObterMutex(ficheiro);
            mutex.WaitOne();
            try
            {
                bool existe = File.Exists(ficheiro);
                using var sw = new StreamWriter(ficheiro, append: true, Encoding.UTF8);
                if (!existe) sw.WriteLine("timestamp,sensor_id,zona,frame");
                sw.WriteLine($"{timestamp},{sensorId},{zona},{conteudo}");
            }
            finally { mutex.ReleaseMutex(); }
        }

        static Mutex ObterMutex(string ficheiro)
        {
            lock (fileMutexesMeta)
            {
                if (!fileMutexes.ContainsKey(ficheiro))
                    fileMutexes[ficheiro] = new Mutex();
                return fileMutexes[ficheiro];
            }
        }
    }
}
