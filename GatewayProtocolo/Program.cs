using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

    class VideoSession
    {
        public string SensorId { get; set; } = "";
        public string Zona { get; set; } = "";
        public bool Ativa { get; set; }
    }

    class Program
    {
        private static readonly object fileLock = new object();
        private static readonly object sensorLock = new object();
        private static readonly object videoLock = new object();

        private static Dictionary<string, SensorInfo> sensores = new();
        private static Dictionary<string, VideoSession> sessoesVideo = new();

        private static int portaTcpGateway = 5000;
        private static int portaUdpGateway = 7000;
        private static string ipServidor = "127.0.0.1";
        private static int portaServidor = 6000;
        private static string ficheiroCsv = "sensores.csv";
        private static string gatewayId = "GW01";

        static async Task Main(string[] args)
        {
            try
            {
                CarregarSensores(ficheiroCsv);
            }
            catch (FileNotFoundException ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
                Console.WriteLine($"Ficheiro em falta: {ex.FileName}");
                return;
            }

            string respostaInit = await InicializarLigacaoServidor();

            if (!respostaInit.StartsWith("ACK"))
            {
                Console.WriteLine("Não foi possível inicializar a ligação com o servidor.");
                Console.WriteLine($"Resposta recebida: {respostaInit}");
                return;
            }

            Console.WriteLine("Ligação inicial com o servidor concluída com sucesso.");

            TcpListener listener = new TcpListener(IPAddress.Any, portaTcpGateway);
            listener.Start();

            Console.WriteLine($"Gateway TCP à escuta na porta {portaTcpGateway}...");
            Console.WriteLine($"Gateway UDP à escuta na porta {portaUdpGateway}...");

            _ = Task.Run(() => MonitorizarHeartbeats());
            _ = Task.Run(() => ReceberVideoUdp());

            while (true)
            {
                TcpClient clienteSensor = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => TratarSensor(clienteSensor));
            }
        }

        static async Task<string> InicializarLigacaoServidor()
        {
            try
            {
                string resposta1 = await EnviarParaServidor("HELLO_GATEWAY");
                if (!resposta1.StartsWith("ACK"))
                    return resposta1;

                string resposta2 = await EnviarParaServidor($"GATEWAY_REGISTER|{gatewayId}");
                if (!resposta2.StartsWith("ACK"))
                    return resposta2;

                string resposta3 = await EnviarParaServidor($"SESSION_START|{gatewayId}");
                return resposta3;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro na inicialização com o servidor: {ex.Message}");
                return "ERROR|INIT_SERVER";
            }
        }

        static void CarregarSensores(string ficheiroCsv)
        {
            lock (sensorLock)
            {
                sensores.Clear();

                if (!File.Exists(ficheiroCsv))
                {
                    throw new FileNotFoundException(
                        "O ficheiro de configuração dos sensores não foi encontrado.",
                        ficheiroCsv
                    );
                }

                var linhas = File.ReadAllLines(ficheiroCsv);

                foreach (var linha in linhas.Skip(1))
                {
                    if (string.IsNullOrWhiteSpace(linha))
                        continue;

                    string[] partes = linha.Split(':');
                    if (partes.Length < 5)
                        continue;

                    string id = partes[0].Trim();
                    string estado = partes[1].Trim();
                    string zona = partes[2].Trim();
                    string tiposRaw = partes[3].Trim().Trim('[', ']');
                    string lastSyncRaw = string.Join(":", partes.Skip(4)).Trim();

                    DateTime? lastSync = null;
                    if (lastSyncRaw != "-" && DateTime.TryParse(lastSyncRaw, out DateTime dataLida))
                    {
                        lastSync = dataLida;
                    }

                    sensores[id] = new SensorInfo
                    {
                        Id = id,
                        Estado = estado,
                        Zona = zona,
                        TiposDados = tiposRaw
                            .Split(',', StringSplitOptions.RemoveEmptyEntries)
                            .Select(t => t.Trim())
                            .ToList(),
                        LastSync = lastSync
                    };
                }
            }
        }

        static void GuardarSensores(string ficheiroCsv)
        {
            lock (fileLock)
            {
                lock (sensorLock)
                {
                    var linhas = new List<string>
                    {
                        "sensor_id:estado:zona:[tipos_dados]:last_sync"
                    };

                    foreach (var s in sensores.Values.OrderBy(x => x.Id))
                    {
                        string tipos = "[" + string.Join(",", s.TiposDados) + "]";
                        string lastSync = s.LastSync.HasValue ? s.LastSync.Value.ToString("s") : "-";

                        linhas.Add($"{s.Id}:{s.Estado}:{s.Zona}:{tipos}:{lastSync}");
                    }

                    File.WriteAllLines(ficheiroCsv, linhas);
                }
            }
        }

        static async Task TratarSensor(TcpClient clienteSensor)
        {
            Console.WriteLine("Sensor ligado ao gateway.");

            using (clienteSensor)
            using (NetworkStream stream = clienteSensor.GetStream())
            using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
            using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            {
                bool registado = false;
                string sensorAtual = "";

                while (true)
                {
                    string? mensagem = await reader.ReadLineAsync();
                    if (mensagem == null)
                        break;

                    Console.WriteLine($"Recebido do sensor: {mensagem}");
                    string[] partes = mensagem.Split('|');

                    if (partes.Length == 0)
                    {
                        await writer.WriteLineAsync("ERROR|FORMAT");
                        continue;
                    }

                    string comando = partes[0].ToUpperInvariant();

                    if (comando == "HELLO")
                    {
                        await writer.WriteLineAsync("OK|HELLO");
                    }
                    else if (comando == "REGISTER")
                    {
                        if (partes.Length < 4)
                        {
                            await writer.WriteLineAsync("ERROR|REGISTER");
                            continue;
                        }

                        string sensorId = partes[1];
                        string zona = partes[2];
                        var tipos = partes[3]
                            .Split(',', StringSplitOptions.RemoveEmptyEntries)
                            .Select(t => t.Trim())
                            .ToList();

                        SensorInfo? sensor;
                        lock (sensorLock)
                        {
                            sensores.TryGetValue(sensorId, out sensor);
                        }

                        if (sensor == null)
                        {
                            await writer.WriteLineAsync("ERROR|SENSOR_NOT_FOUND");
                            continue;
                        }

                        if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_STATE");
                            continue;
                        }

                        if (!sensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_ZONE");
                            continue;
                        }

                        bool tiposValidos = tipos.All(t =>
                            sensor.TiposDados.Contains(t, StringComparer.OrdinalIgnoreCase));

                        if (!tiposValidos)
                        {
                            await writer.WriteLineAsync("ERROR|UNSUPPORTED_TYPE");
                            continue;
                        }

                        lock (sensorLock)
                        {
                            sensores[sensorId].LastSync = DateTime.Now;
                        }

                        GuardarSensores(ficheiroCsv);

                        registado = true;
                        sensorAtual = sensorId;

                        await writer.WriteLineAsync("OK|REGISTERED");
                    }
                    else if (comando == "DATA")
                    {
                        if (!registado)
                        {
                            await writer.WriteLineAsync("ERROR|NOT_REGISTERED");
                            continue;
                        }

                        if (partes.Length < 5)
                        {
                            await writer.WriteLineAsync("ERROR|DATA");
                            continue;
                        }

                        string sensorId = partes[1];
                        string zona = partes[2];
                        string tipo = partes[3];
                        string valor = partes[4];

                        SensorInfo? sensor;
                        lock (sensorLock)
                        {
                            sensores.TryGetValue(sensorId, out sensor);
                        }

                        if (sensor == null || sensorId != sensorAtual)
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_SENSOR");
                            continue;
                        }

                        if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_STATE");
                            continue;
                        }

                        if (!sensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_ZONE");
                            continue;
                        }

                        if (!sensor.TiposDados.Contains(tipo, StringComparer.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|UNSUPPORTED_TYPE");
                            continue;
                        }

                        lock (sensorLock)
                        {
                            sensores[sensorId].LastSync = DateTime.Now;
                        }

                        GuardarSensores(ficheiroCsv);

                        string mensagemServidor = $"STORE|{sensorId}|{zona}|{tipo}|{valor}";
                        string respostaServidor = await EnviarParaServidor(mensagemServidor);

                        if (respostaServidor.StartsWith("ACK"))
                            await writer.WriteLineAsync("ACK|DATA");
                        else
                            await writer.WriteLineAsync("ERROR|SERVER");
                    }
                    else if (comando == "VIDEO_START")
                    {
                        if (!registado)
                        {
                            await writer.WriteLineAsync("ERROR|NOT_REGISTERED");
                            continue;
                        }

                        if (partes.Length < 3)
                        {
                            await writer.WriteLineAsync("ERROR|VIDEO_START");
                            continue;
                        }

                        string sensorId = partes[1];
                        string zona = partes[2];

                        SensorInfo? sensor;
                        lock (sensorLock)
                        {
                            sensores.TryGetValue(sensorId, out sensor);
                        }

                        if (sensor == null || sensorId != sensorAtual)
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_SENSOR");
                            continue;
                        }

                        if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_STATE");
                            continue;
                        }

                        if (!sensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_ZONE");
                            continue;
                        }

                        lock (videoLock)
                        {
                            sessoesVideo[sensorId] = new VideoSession
                            {
                                SensorId = sensorId,
                                Zona = zona,
                                Ativa = true
                            };
                        }

                        lock (sensorLock)
                        {
                            sensores[sensorId].LastSync = DateTime.Now;
                        }

                        GuardarSensores(ficheiroCsv);

                        string respostaServidor = await EnviarParaServidor($"VIDEO_START|{sensorId}|{zona}");

                        if (respostaServidor.StartsWith("ACK"))
                            await writer.WriteLineAsync($"ACK|VIDEO_START|UDP_PORT|{portaUdpGateway}");
                        else
                            await writer.WriteLineAsync("ERROR|SERVER");
                    }
                    else if (comando == "VIDEO_END")
                    {
                        if (!registado)
                        {
                            await writer.WriteLineAsync("ERROR|NOT_REGISTERED");
                            continue;
                        }

                        if (partes.Length < 3)
                        {
                            await writer.WriteLineAsync("ERROR|VIDEO_END");
                            continue;
                        }

                        string sensorId = partes[1];
                        string zona = partes[2];

                        lock (videoLock)
                        {
                            if (sessoesVideo.ContainsKey(sensorId))
                                sessoesVideo[sensorId].Ativa = false;
                        }

                        lock (sensorLock)
                        {
                            if (sensores.ContainsKey(sensorId))
                                sensores[sensorId].LastSync = DateTime.Now;
                        }

                        GuardarSensores(ficheiroCsv);

                        string respostaServidor = await EnviarParaServidor($"VIDEO_END|{sensorId}|{zona}");

                        if (respostaServidor.StartsWith("ACK"))
                            await writer.WriteLineAsync("ACK|VIDEO_END");
                        else
                            await writer.WriteLineAsync("ERROR|SERVER");
                    }
                    else if (comando == "HEARTBEAT")
                    {
                        if (!registado || partes.Length < 2)
                        {
                            await writer.WriteLineAsync("ERROR|HEARTBEAT");
                            continue;
                        }

                        string sensorId = partes[1];

                        lock (sensorLock)
                        {
                            if (!sensores.ContainsKey(sensorId))
                            {
                                sensorId = "";
                            }
                            else
                            {
                                sensores[sensorId].LastSync = DateTime.Now;
                            }
                        }

                        if (string.IsNullOrEmpty(sensorId))
                        {
                            await writer.WriteLineAsync("ERROR|SENSOR_NOT_FOUND");
                            continue;
                        }

                        GuardarSensores(ficheiroCsv);
                        await writer.WriteLineAsync("ACK|HEARTBEAT");
                    }
                    else if (comando == "BYE")
                    {
                        if (!string.IsNullOrWhiteSpace(sensorAtual))
                        {
                            lock (videoLock)
                            {
                                if (sessoesVideo.ContainsKey(sensorAtual))
                                    sessoesVideo[sensorAtual].Ativa = false;
                            }
                        }

                        await writer.WriteLineAsync("OK|BYE");
                        break;
                    }
                    else
                    {
                        await writer.WriteLineAsync("ERROR|UNKNOWN_COMMAND");
                    }
                }
            }

            Console.WriteLine("Ligação com sensor terminada.");
        }

        static async Task ReceberVideoUdp()
        {
            using UdpClient udp = new UdpClient(portaUdpGateway);

            while (true)
            {
                try
                {
                    UdpReceiveResult resultado = await udp.ReceiveAsync();
                    string mensagem = Encoding.UTF8.GetString(resultado.Buffer);

                    Console.WriteLine($"Frame UDP recebido: {mensagem}");

                    // formato:
                    // VIDEO_FRAME|S102|ZONA_ESCOLAR|frame001
                    string[] partes = mensagem.Split('|');

                    if (partes.Length < 4)
                    {
                        Console.WriteLine("Datagrama UDP inválido.");
                        continue;
                    }

                    string comando = partes[0].ToUpperInvariant();
                    if (comando != "VIDEO_FRAME")
                        continue;

                    string sensorId = partes[1];
                    string zona = partes[2];
                    string conteudo = partes[3];

                    SensorInfo? sensor;
                    lock (sensorLock)
                    {
                        sensores.TryGetValue(sensorId, out sensor);
                    }

                    if (sensor == null)
                    {
                        Console.WriteLine($"Sensor {sensorId} não registado para vídeo.");
                        continue;
                    }

                    if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"Sensor {sensorId} não está ativo.");
                        continue;
                    }

                    if (!sensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"Zona inválida no frame UDP de {sensorId}.");
                        continue;
                    }

                    bool videoPermitido;
                    lock (videoLock)
                    {
                        videoPermitido = sessoesVideo.ContainsKey(sensorId) && sessoesVideo[sensorId].Ativa;
                    }

                    if (!videoPermitido)
                    {
                        Console.WriteLine($"Sessão de vídeo não ativa para {sensorId}.");
                        continue;
                    }

                    lock (sensorLock)
                    {
                        sensores[sensorId].LastSync = DateTime.Now;
                    }

                    GuardarSensores(ficheiroCsv);

                    string mensagemServidor = $"VIDEO_FRAME|{sensorId}|{zona}|{conteudo}";
                    string resposta = await EnviarParaServidor(mensagemServidor);

                    Console.WriteLine($"Resposta do servidor ao frame UDP: {resposta}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro no UDP do gateway: {ex.Message}");
                }
            }
        }

        static async Task<string> EnviarParaServidor(string mensagem)
        {
            try
            {
                using TcpClient clienteServidor = new TcpClient();
                await clienteServidor.ConnectAsync(IPAddress.Parse(ipServidor), portaServidor);

                using NetworkStream stream = clienteServidor.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"A enviar para o servidor: {mensagem}");
                await writer.WriteLineAsync(mensagem);

                string? resposta = await reader.ReadLineAsync();

                if (string.IsNullOrWhiteSpace(resposta))
                    return "ERROR|NO_RESPONSE";

                Console.WriteLine($"Resposta do servidor: {resposta}");
                return resposta;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao enviar para o servidor: {ex.Message}");
                return "ERROR|SERVER_CONNECTION";
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
                        if (sensor.LastSync.HasValue)
                        {
                            TimeSpan diferenca = DateTime.Now - sensor.LastSync.Value;

                            if (diferenca.TotalSeconds > 30)
                            {
                                Console.WriteLine($"Aviso: o sensor {sensor.Id} pode estar inativo.");
                            }
                        }
                    }
                }
            }
        }
    }
}
