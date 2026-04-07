using System;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Gateway
{
    class SensorInfo
    {
        public string Id { get; set; } = "";
        public string Estado { get; set; } = "";
        public string Zona { get; set; } = "";
        public List<string> TiposDados { get; set; } = new();
        public string LastSync { get; set; } = "-";
    }

    class Program
    {
        private static readonly object fileLock = new object();
        private static Dictionary<string, SensorInfo> sensores = new();

        static async Task Main(string[] args)
        {
            int portaGateway = 5000;
            string ipServidor = "127.0.0.1";
            int portaServidor = 6000;
            string ficheiroCsv = "sensores.csv";

            CarregarSensores(ficheiroCsv);

            TcpListener listener = new TcpListener(IPAddress.Any, portaGateway);
            listener.Start();

            Console.WriteLine($"Gateway à escuta na porta {portaGateway}...");

            while (true)
            {
                TcpClient clienteSensor = await listener.AcceptTcpClientAsync();
                _ = Task.Run(() => TratarSensor(clienteSensor, ipServidor, portaServidor, ficheiroCsv));
            }
        }

        static void CarregarSensores(string ficheiroCsv)
    {
        sensores.Clear();

        if (!File.Exists(ficheiroCsv))
        {
            Console.WriteLine("Ficheiro CSV não encontrado. A criar exemplo...");
            Console.WriteLine("Confirme se o Ficheiro CS existe e se o caminho está correto.");
            //Teste inicial, pode ser removido depois
            /* File.WriteAllLines(ficheiroCsv, new[]
             {
             "sensor_id:estado:zona:[tipos_dados]:last_sync",
             "S101:ativo:ZONA_CENTRO:[TEMP,HUM,RUIDO]:-",
             "S102:ativo:ZONA_ESCOLAR:[PM2.5,TEMP]:-",
             "S103:manutencao:ZONA_INDUSTRIAL:[AR,PM10]:-"
             });*/
        }
    
        var linhas = File.ReadAllLines(ficheiroCsv);

        foreach (var linha in linhas.Skip(1)) //lembrar de colocar skip(0) caso nao tenha header
        {
            if (string.IsNullOrWhiteSpace(linha))
                continue;

            string[] partes = linha.Split(':');
            if (partes.Length < 5)
                continue;
    
            string id = partes[0];
            string estado = partes[1];
            string zona = partes[2];
            string tiposRaw = partes[3].Trim('[', ']');
            string lastSync = string.Join(":", partes.Skip(4));
    
            sensores[id] = new SensorInfo
            {
                Id = id,
                Estado = estado,
                Zona = zona,
                TiposDados = tiposRaw.Split(',', StringSplitOptions.RemoveEmptyEntries)
                                     .Select(t => t.Trim())
                                     .ToList(),
                LastSync = lastSync
            };
        }
    }
        static void GuardarSensores(string ficheiroCsv)
        {
            lock (fileLock)
            {
                var linhas = new List<string>
                {
                    "sensor_id:estado:zona:[tipos_dados]:last_sync" //verificar se o header é necessário ou se deve ser removido 
                };

                foreach (var s in sensores.Values.OrderBy(x => x.Id))
                {
                    string tipos = "[" + string.Join(",", s.TiposDados) + "]";
                    linhas.Add($"{s.Id}:{s.Estado}:{s.Zona}:{tipos}:{s.LastSync}");
                }

                File.WriteAllLines(ficheiroCsv, linhas);
            }
        }

        static async Task TratarSensor(TcpClient clienteSensor, string ipServidor, int portaServidor, string ficheiroCsv)
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

                    string comando = partes[0].ToUpper();

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
                        var tipos = partes[3].Split(',', StringSplitOptions.RemoveEmptyEntries)
                                             .Select(t => t.Trim())
                                             .ToList();

                        if (!sensores.ContainsKey(sensorId))
                        {
                            await writer.WriteLineAsync("ERROR|SENSOR_NOT_FOUND");
                            continue;
                        }

                        var sensor = sensores[sensorId];

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

                        bool tiposValidos = tipos.All(t => sensor.TiposDados.Contains(t, StringComparer.OrdinalIgnoreCase));
                        if (!tiposValidos)
                        {
                            await writer.WriteLineAsync("ERROR|UNSUPPORTED_TYPE");
                            continue;
                        }

                        sensor.LastSync = DateTime.Now.ToString("s");
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
                        string timestamp = DateTime.Now.ToString("s");

                        if (sensorId != sensorAtual || !sensores.ContainsKey(sensorId))
                        {
                            await writer.WriteLineAsync("ERROR|INVALID_SENSOR");
                            continue;
                        }

                        var sensor = sensores[sensorId];

                        if (!sensor.TiposDados.Contains(tipo, StringComparer.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("ERROR|UNSUPPORTED_TYPE");
                            continue;
                        }

                        sensor.LastSync = DateTime.Now.ToString("s");
                        GuardarSensores(ficheiroCsv);

                        // Include timestamp if provided by the sensor (partes[5])
                        string timestamp = partes.Length >= 6 ? partes[5] : DateTime.Now.ToString("s");
                        string mensagemServidor = $"STORE|{sensorId}|{zona}|{tipo}|{valor}|{timestamp}";
                        bool enviado = await EnviarAoServidor(ipServidor, portaServidor, mensagemServidor);

                        if (enviado)
                            await writer.WriteLineAsync("ACK|DATA");
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

                        if (!sensores.ContainsKey(sensorId))
                        {
                            await writer.WriteLineAsync("ERROR|SENSOR_NOT_FOUND");
                            continue;
                        }

                        sensores[sensorId].LastSync = DateTime.Now.ToString("s");
                        GuardarSensores(ficheiroCsv);

                        await writer.WriteLineAsync("ACK|HEARTBEAT");
                    }
                    else if (comando == "BYE")
                    {
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

        static async Task<bool> EnviarAoServidor(string ipServidor, int portaServidor, string mensagem)
        {
            try
            {
                using TcpClient clienteServidor = new TcpClient();
                await clienteServidor.ConnectAsync(IPAddress.Parse(ipServidor), portaServidor);

                using NetworkStream stream = clienteServidor.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"Encaminhado para servidor: {mensagem}");
                await writer.WriteLineAsync(mensagem);

                string? resposta = await reader.ReadLineAsync();
                Console.WriteLine($"Resposta do servidor: {resposta}");

                return resposta != null && resposta.StartsWith("ACK");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao contactar servidor: {ex.Message}");
                return false;
            }
        }
    }
}
