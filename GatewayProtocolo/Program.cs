using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using GrpcPreProcessamento;
using Grpc.Net.Client;

namespace Gateway
{
    class SensorInfo
    {
        public string Id { get; set; } = "";
        public string Estado { get; set; } = "";
        public string Zona { get; set; } = "";
        public List<string> TiposDados { get; set; } = new();
        public DateTime? LastSync { get; set; }
    }

    class SensorMensagem
    {
        [JsonPropertyName("type")]
        public string Type { get; set; } = "";

        [JsonPropertyName("sensorId")]
        public string SensorId { get; set; } = "";

        [JsonPropertyName("zona")]
        public string Zona { get; set; } = "";

        [JsonPropertyName("tipo")]
        public string Tipo { get; set; } = "";

        [JsonPropertyName("valor")]
        public double Valor { get; set; }

        [JsonPropertyName("frameId")]
        public int FrameId { get; set; }

        [JsonPropertyName("conteudo")]
        public string Conteudo { get; set; } = "";

        [JsonPropertyName("timestamp")]
        public string Timestamp { get; set; } = "";

        [JsonPropertyName("tipos")]
        public List<string>? Tipos { get; set; }
    }

    static class GatewayFileMutex
    {
        public static Mutex SensoresCsvMutex = new Mutex();
    }

    class Program
    {
        private static readonly object sensorLock = new object();
        private static Dictionary<string, SensorInfo> sensores = new();

        private static string ipServidor = "127.0.0.1";
        private static int portaServidor = 6000;
        private static string ficheiroCsv = "sensores.csv";
        private static string gatewayId = "GW01";

        // RabbitMQ
        private static string rabbitHost = "localhost";
        private static string exchangeName = "sensores_topic";
        private static string queueName = "gateway_zona_escolar";
        private static IConnection? rabbitConnection;
        private static IChannel? rabbitChannel;

        // Ligação TCP persistente ao servidor
        private static TcpClient? tcpCliente;
        private static StreamReader? tcpReader;
        private static StreamWriter? tcpWriter;
        private static readonly SemaphoreSlim tcpSemaphore = new SemaphoreSlim(1, 1);

        // Cliente gRPC de pré-processamento (porta 50052)
        private const string GrpcPreProcessUrl = "http://localhost:50052";
        private static ServicoPreProcessamento.ServicoPreProcessamentoClient? preProcessClient;

        static async Task Main(string[] args)
        {
            // Necessário para gRPC sobre HTTP/2 sem TLS
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            try
            {
                CarregarSensores(ficheiroCsv);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERRO AO CARREGAR CSV: {ex.Message}");
                Console.ReadKey();
                return;
            }

            if (!await LigarAoServidor())
            {
                Console.WriteLine("ERRO: não foi possível ligar ao servidor.");
                Console.ReadKey();
                return;
            }

            string respostaInit = await InicializarHandshake();
            if (!respostaInit.StartsWith("ACK"))
            {
                Console.WriteLine($"ERRO NA INICIALIZAÇÃO COM O SERVIDOR: {respostaInit}");
                Console.ReadKey();
                return;
            }

            Console.WriteLine("Ligação persistente ao servidor estabelecida.");

            InicializarGrpcPreProcessamento();

            _ = Task.Run(() => MonitorizarHeartbeats());

            await SubscreverRabbitMqAsync();

            Console.WriteLine("Gateway RabbitMQ ativo. Pressiona ENTER para terminar.");
            Console.ReadLine();

            tcpWriter?.Dispose();
            tcpReader?.Dispose();
            tcpCliente?.Dispose();
            rabbitChannel?.Dispose();
            rabbitConnection?.Dispose();
        }

        // Inicializa o canal gRPC para o serviço de pré-processamento.
        // O canal é lazy — a ligação real só acontece na primeira chamada RPC.
        static void InicializarGrpcPreProcessamento()
        {
            try
            {
                var channel = GrpcChannel.ForAddress(GrpcPreProcessUrl);
                preProcessClient = new ServicoPreProcessamento.ServicoPreProcessamentoClient(channel);
                Console.WriteLine($"[GATEWAY] Cliente gRPC pré-processamento pronto ({GrpcPreProcessUrl})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GATEWAY] Aviso: pré-processamento gRPC não inicializado — {ex.Message}");
            }
        }

        // Estabelece (ou reestabelece) a ligação TCP ao servidor
        static async Task<bool> LigarAoServidor()
        {
            try
            {
                tcpCliente?.Dispose();
                tcpCliente = new TcpClient();
                await tcpCliente.ConnectAsync(IPAddress.Parse(ipServidor), portaServidor);

                var stream = tcpCliente.GetStream();
                tcpReader = new StreamReader(stream, Encoding.UTF8);
                tcpWriter = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"[TCP] Ligado ao servidor {ipServidor}:{portaServidor}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TCP] Falha na ligação ao servidor: {ex.Message}");
                tcpCliente = null;
                return false;
            }
        }

        // Envia o handshake inicial (HELLO → REGISTER → SESSION_START)
        static async Task<string> InicializarHandshake()
        {
            string r1 = await EnviarMensagemDireto("HELLO_GATEWAY");
            if (!r1.StartsWith("ACK")) return r1;

            string r2 = await EnviarMensagemDireto($"GATEWAY_REGISTER|{gatewayId}");
            if (!r2.StartsWith("ACK")) return r2;

            return await EnviarMensagemDireto($"SESSION_START|{gatewayId}");
        }

        static async Task<string> EnviarMensagemDireto(string mensagem)
        {
            if (tcpWriter == null || tcpReader == null)
                return "ERROR|NOT_CONNECTED";
            try
            {
                await tcpWriter.WriteLineAsync(mensagem);
                string? resposta = await tcpReader.ReadLineAsync();
                return string.IsNullOrWhiteSpace(resposta) ? "ERROR|NO_RESPONSE" : resposta;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TCP] Erro ao enviar mensagem direta: {ex.Message}");
                return "ERROR|SEND_FAILED";
            }
        }

        static async Task<string> EnviarParaServidor(string mensagem)
        {
            await tcpSemaphore.WaitAsync();
            try
            {
                if (tcpCliente == null || !tcpCliente.Connected)
                {
                    Console.WriteLine("[TCP] Ligação perdida — a reconectar...");
                    if (!await LigarAoServidor())
                        return "ERROR|SERVER_CONNECTION";

                    string handshake = await InicializarHandshake();
                    if (!handshake.StartsWith("ACK"))
                    {
                        Console.WriteLine($"[TCP] Re-handshake falhou: {handshake}");
                        return "ERROR|HANDSHAKE_FAILED";
                    }
                    Console.WriteLine("[TCP] Reconexão ao servidor bem-sucedida.");
                }

                await tcpWriter!.WriteLineAsync(mensagem);
                string? resposta = await tcpReader!.ReadLineAsync();

                if (string.IsNullOrWhiteSpace(resposta))
                {
                    tcpCliente?.Dispose();
                    tcpCliente = null;
                    return "ERROR|CONNECTION_CLOSED";
                }

                return resposta;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TCP] Erro na comunicação: {ex.Message} — a repor ligação.");
                tcpCliente?.Dispose();
                tcpCliente = null;
                return "ERROR|SERVER_CONNECTION";
            }
            finally
            {
                tcpSemaphore.Release();
            }
        }

        static void CarregarSensores(string ficheiro)
        {
            GatewayFileMutex.SensoresCsvMutex.WaitOne();
            try
            {
                lock (sensorLock)
                {
                    sensores.Clear();

                    if (!File.Exists(ficheiro))
                        throw new FileNotFoundException(
                            "O ficheiro de configuração dos sensores não foi encontrado.", ficheiro);

                    foreach (var linha in File.ReadAllLines(ficheiro).Skip(1))
                    {
                        if (string.IsNullOrWhiteSpace(linha)) continue;

                        string[] partes = linha.Split(':');
                        if (partes.Length < 5) continue;

                        string id          = partes[0].Trim();
                        string estado      = partes[1].Trim();
                        string zona        = partes[2].Trim();
                        string tiposRaw    = partes[3].Trim().Trim('[', ']');
                        string lastSyncRaw = string.Join(":", partes.Skip(4)).Trim();

                        DateTime? lastSync = null;
                        if (lastSyncRaw != "-" && DateTime.TryParse(lastSyncRaw, out DateTime dataLida))
                            lastSync = dataLida;

                        sensores[id] = new SensorInfo
                        {
                            Id         = id,
                            Estado     = estado,
                            Zona       = zona,
                            TiposDados = tiposRaw
                                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                                .Select(t => t.Trim())
                                .ToList(),
                            LastSync = lastSync
                        };
                    }
                }
            }
            finally
            {
                GatewayFileMutex.SensoresCsvMutex.ReleaseMutex();
            }
        }

        static void GuardarSensores(string ficheiro)
        {
            GatewayFileMutex.SensoresCsvMutex.WaitOne();
            try
            {
                lock (sensorLock)
                {
                    var linhas = new List<string> { "sensor_id:estado:zona:[tipos_dados]:last_sync" };

                    foreach (var s in sensores.Values.OrderBy(x => x.Id))
                    {
                        string tipos    = "[" + string.Join(",", s.TiposDados) + "]";
                        string lastSync = s.LastSync.HasValue ? s.LastSync.Value.ToString("s") : "-";
                        linhas.Add($"{s.Id}:{s.Estado}:{s.Zona}:{tipos}:{lastSync}");
                    }

                    File.WriteAllLines(ficheiro, linhas);
                }
            }
            finally
            {
                GatewayFileMutex.SensoresCsvMutex.ReleaseMutex();
            }
        }

        static async Task SubscreverRabbitMqAsync()
        {
            var factory = new ConnectionFactory { HostName = rabbitHost };

            rabbitConnection = await factory.CreateConnectionAsync();
            rabbitChannel    = await rabbitConnection.CreateChannelAsync();

            await rabbitChannel.ExchangeDeclareAsync(
                exchange:   exchangeName,
                type:       ExchangeType.Topic,
                durable:    true,
                autoDelete: false);

            await rabbitChannel.QueueDeclareAsync(
                queue:      queueName,
                durable:    false,
                exclusive:  false,
                autoDelete: false,
                arguments:  null);

            await rabbitChannel.QueueBindAsync(
                queue:      queueName,
                exchange:   exchangeName,
                routingKey: "sensor.#");

            var consumer = new AsyncEventingBasicConsumer(rabbitChannel);

            consumer.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    string mensagem = Encoding.UTF8.GetString(ea.Body.ToArray());
                    Console.WriteLine($"\n[RABBIT] {mensagem}");
                    await ProcessarMensagemAsync(mensagem);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RABBIT] Erro: {ex.Message}");
                }
            };

            await rabbitChannel.BasicConsumeAsync(
                queue:    queueName,
                autoAck:  true,
                consumer: consumer);
        }

        static async Task ProcessarMensagemAsync(string json)
        {
            SensorMensagem? msg;
            try
            {
                msg = JsonSerializer.Deserialize<SensorMensagem>(json);
            }
            catch
            {
                Console.WriteLine("[GATEWAY] JSON inválido — ignorado.");
                return;
            }

            if (msg == null || string.IsNullOrEmpty(msg.SensorId))
            {
                Console.WriteLine("[GATEWAY] Mensagem sem sensorId — ignorada.");
                return;
            }

            string sensorId = msg.SensorId;

            switch (msg.Type.ToUpperInvariant())
            {
                case "DATA":
                    await ProcessarDataAsync(sensorId, msg);
                    break;

                case "REGISTER":
                    RegistarOuAtualizarSensor(sensorId, msg.Zona, msg.Tipos);
                    break;

                case "HEARTBEAT":
                    AtualizarLastSync(sensorId);
                    Console.WriteLine($"[HEARTBEAT] {sensorId}");
                    break;

                case "VIDEO_START":
                    AtualizarLastSync(sensorId);
                    string rVS = await EnviarParaServidor($"VIDEO_START|{sensorId}|{msg.Zona}");
                    Console.WriteLine($"[VIDEO_START] {sensorId} → {rVS}");
                    break;

                case "VIDEO_FRAME":
                    string rVF = await EnviarParaServidor($"VIDEO_FRAME|{sensorId}|{msg.Zona}|{msg.Conteudo}");
                    Console.WriteLine($"[VIDEO_FRAME] {sensorId} frame#{msg.FrameId} → {rVF}");
                    break;

                case "VIDEO_END":
                    string rVE = await EnviarParaServidor($"VIDEO_END|{sensorId}");
                    Console.WriteLine($"[VIDEO_END] {sensorId} → {rVE}");
                    break;

                case "BYE":
                    AtualizarLastSync(sensorId);
                    Console.WriteLine($"[BYE] {sensorId}");
                    break;

                default:
                    Console.WriteLine($"[GATEWAY] Tipo desconhecido: {msg.Type}");
                    break;
            }
        }

        static async Task ProcessarDataAsync(string sensorId, SensorMensagem msg)
        {
            SensorInfo? sensor;
            lock (sensorLock)
                sensores.TryGetValue(sensorId, out sensor);

            if (sensor == null)
            {
                Console.WriteLine($"[DATA] Sensor {sensorId} não registado — ignorado.");
                return;
            }
            if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"[DATA] Sensor {sensorId} não está ativo.");
                return;
            }
            if (!sensor.Zona.Equals(msg.Zona, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"[DATA] Zona inválida para {sensorId}: {msg.Zona}");
                return;
            }
            if (!sensor.TiposDados.Contains(msg.Tipo, StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine($"[DATA] Tipo '{msg.Tipo}' não suportado por {sensorId}.");
                return;
            }

            AtualizarLastSync(sensorId);
            GuardarSensores(ficheiroCsv);

            // Pré-processamento via gRPC: normaliza tipo, converte unidades, valida intervalo.
            // Se o serviço não estiver disponível, usa os valores originais (degradação graciosa).
            double valorFinal = msg.Valor;
            string tipoFinal  = msg.Tipo;

            if (preProcessClient != null)
            {
                try
                {
                    var pedido = new DadoBruto
                    {
                        SensorId  = sensorId,
                        Zona      = msg.Zona,
                        Tipo      = msg.Tipo,
                        Valor     = msg.Valor,
                        Unidade   = "",
                        Timestamp = msg.Timestamp
                    };
                    var resultado = await preProcessClient.PreProcessarAsync(pedido);

                    if (resultado.Sucesso)
                    {
                        valorFinal = resultado.ValorNormalizado;
                        tipoFinal  = resultado.Tipo;
                        Console.WriteLine($"[PRE-PROC] {sensorId}|{msg.Tipo}={msg.Valor:F2} → {tipoFinal}={valorFinal:F2}");
                    }
                    else
                    {
                        Console.WriteLine($"[PRE-PROC] {sensorId} rejeitado: {resultado.Erro}");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[PRE-PROC] Serviço indisponível: {ex.Message} — usando valor original.");
                }
            }

            string valorStr = valorFinal.ToString(System.Globalization.CultureInfo.InvariantCulture);
            string resposta = await EnviarParaServidor($"STORE|{sensorId}|{msg.Zona}|{tipoFinal}|{valorStr}");
            Console.WriteLine($"[DATA] {sensorId}|{tipoFinal}={valorStr} → {resposta}");
        }

        static void RegistarOuAtualizarSensor(string sensorId, string zona, List<string>? tipos)
        {
            lock (sensorLock)
            {
                if (!sensores.ContainsKey(sensorId))
                {
                    sensores[sensorId] = new SensorInfo
                    {
                        Id         = sensorId,
                        Estado     = "ativo",
                        Zona       = zona,
                        TiposDados = tipos ?? new List<string>(),
                        LastSync   = DateTime.Now
                    };
                    Console.WriteLine($"[REGISTER] Novo sensor: {sensorId} ({zona}) tipos={string.Join(",", tipos ?? new())}");
                }
                else
                {
                    sensores[sensorId].LastSync = DateTime.Now;
                    if (tipos != null && tipos.Count > 0)
                        sensores[sensorId].TiposDados = tipos;
                    Console.WriteLine($"[REGISTER] Sensor atualizado: {sensorId}");
                }
            }
            GuardarSensores(ficheiroCsv);
        }

        static void AtualizarLastSync(string sensorId)
        {
            lock (sensorLock)
            {
                if (sensores.ContainsKey(sensorId))
                    sensores[sensorId].LastSync = DateTime.Now;
            }
        }

        static void MonitorizarHeartbeats()
        {
            while (true)
            {
                Thread.Sleep(10000);

                lock (sensorLock)
                {
                    foreach (var sensor in sensores.Values)
                    {
                        if (sensor.LastSync.HasValue &&
                            (DateTime.Now - sensor.LastSync.Value).TotalSeconds > 30)
                        {
                            Console.WriteLine($"[AVISO] Sensor {sensor.Id} pode estar inativo.");
                        }
                    }
                }
            }
        }
    }
}
