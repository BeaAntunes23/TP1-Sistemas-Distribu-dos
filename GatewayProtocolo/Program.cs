using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

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
        public string sensor_id { get; set; } = "";
        public string zona { get; set; } = "";
        public string tipo { get; set; } = "";
        public double valor { get; set; }
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

        static async Task Main(string[] args)
        {
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

            string respostaInit = await InicializarLigacaoServidor();
            if (!respostaInit.StartsWith("ACK"))
            {
                Console.WriteLine("ERRO NA LIGAÇÃO AO SERVIDOR!");
                Console.WriteLine($"Resposta: {respostaInit}");
                Console.ReadKey();
                return;
            }

            Console.WriteLine("Ligação inicial com o servidor concluída com sucesso.");

            _ = Task.Run(() => MonitorizarHeartbeats());

            SubscreverRabbitMq();

            Console.WriteLine("Gateway RabbitMQ ativo.");
            Console.WriteLine("Pressiona ENTER para terminar.");
            Console.ReadLine();
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
            GatewayFileMutex.SensoresCsvMutex.WaitOne();

            try
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
            finally
            {
                GatewayFileMutex.SensoresCsvMutex.ReleaseMutex();
            }
        }

        static void GuardarSensores(string ficheiroCsv)
        {
            GatewayFileMutex.SensoresCsvMutex.WaitOne();

            try
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
            finally
            {
                GatewayFileMutex.SensoresCsvMutex.ReleaseMutex();
            }
        }

        static void SubscreverRabbitMq()
        {
            var factory = new ConnectionFactory() { HostName = rabbitHost };

            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic);

            channel.QueueDeclare(
                queue: queueName,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );

            // Exemplo: subscrever tudo da ZONA_ESCOLAR
            channel.QueueBind(
                queue: queueName,
                exchange: exchangeName,
                routingKey: "zona.ZONA_ESCOLAR.*"
            );

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    string mensagem = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"\nMensagem recebida do RabbitMQ: {mensagem}");
                    ProcessarMensagemRabbit(mensagem);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao receber mensagem RabbitMQ: {ex.Message}");
                }
            };

            channel.BasicConsume(
                queue: queueName,
                autoAck: true,
                consumer: consumer
            );
        }

        static void ProcessarMensagemRabbit(string json)
        {
            try
            {
                SensorMensagem? msg = JsonSerializer.Deserialize<SensorMensagem>(json);

                if (msg == null)
                {
                    Console.WriteLine("Mensagem inválida.");
                    return;
                }

                Console.WriteLine($"Sensor: {msg.sensor_id} | Zona: {msg.zona} | Tipo: {msg.tipo} | Valor: {msg.valor}");

                SensorInfo? sensor;
                lock (sensorLock)
                {
                    sensores.TryGetValue(msg.sensor_id, out sensor);
                }

                if (sensor == null)
                {
                    Console.WriteLine("Sensor não registado.");
                    return;
                }

                if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Sensor não está ativo.");
                    return;
                }

                if (!sensor.Zona.Equals(msg.zona, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Zona inválida.");
                    return;
                }

                if (!sensor.TiposDados.Contains(msg.tipo, StringComparer.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Tipo de dado não suportado.");
                    return;
                }

                lock (sensorLock)
                {
                    sensores[msg.sensor_id].LastSync = DateTime.Now;
                }

                GuardarSensores(ficheiroCsv);

                string mensagemServidor = $"STORE|{msg.sensor_id}|{msg.zona}|{msg.tipo}|{msg.valor}";
                string respostaServidor = EnviarParaServidor(mensagemServidor).GetAwaiter().GetResult();

                Console.WriteLine($"Resposta do servidor: {respostaServidor}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar JSON: {ex.Message}");
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
