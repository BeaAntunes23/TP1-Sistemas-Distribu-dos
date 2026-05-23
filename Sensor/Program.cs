using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace SensorApp
{
    /// <summary>
    /// Sensor TP2 — publica dados ambientais num broker RabbitMQ (Pub/Sub).
    /// Substitui a comunicação TCP directa do TP1 por publicação em tópicos.
    ///
    /// Exemplo de execução:
    ///   dotnet run -- --id S101 --zona ZONA_ESCOLAR --tipos TEMP,HUMIDADE,PM2.5
    /// </summary>
    class Program
    {
        // === Configuração por defeito (sobreposta pelos argumentos da CLI) ===
        private static string sensorId = "S101";
        private static string zona = "ZONA_ESCOLAR";
        private static List<string> tipos = new List<string> { "TEMP", "HUMIDADE", "PM2.5" };

        // === Configuração do RabbitMQ ===
        private const string RabbitHost = "localhost";
        private const string ExchangeName = "sensors";  // topic exchange partilhado

        // === Intervalos (em ms) ===
        private const int IntervaloDados = 3000;       // 3 segundos entre medições
        private const int IntervaloHeartbeat = 10000;  // 10 segundos entre heartbeats
        private const int IntervaloVideo = 2000;        // 2 segundos entre frames de vídeo

        private static int frameCounter = 0;

        // Flag global para parar as tasks de fundo de forma ordenada
        private static volatile bool isRunning = true;

        // Gerador de números aleatórios para simular valores realistas
        private static readonly Random rng = new Random();

        static async Task Main(string[] args)
        {
            // 1) Ler argumentos da linha de comandos
            ParseArgs(args);

            Console.WriteLine($"--- Sensor {sensorId} ({zona}) ---");
            Console.WriteLine($"Tipos suportados: {string.Join(", ", tipos)}");
            Console.WriteLine($"Broker: {RabbitHost}, exchange: {ExchangeName}");
            Console.WriteLine();

            // 2) Criar ligação ao RabbitMQ (API assíncrona da v7)
            var factory = new ConnectionFactory { HostName = RabbitHost };

            // O 'await using' garante que ligação e canal fecham ao sair do bloco
            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            // 3) Declarar exchange do tipo 'topic' (idempotente — não dá erro se já existir)
            // 'topic' permite routing keys com wildcards (ex: sensor.ZONA_ESCOLAR.*)
            await channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false);

            Console.WriteLine($"[CONECTADO] RabbitMQ em {RabbitHost}");

            // 4) Publicar evento REGISTER no canal de controlo
            await PublicarStatusAsync(channel, "REGISTER", extra: new { tipos });
            Console.WriteLine("[REGISTER] enviado.");

            // Publicar VIDEO_START
            await PublicarStatusAsync(channel, "VIDEO_START");
            Console.WriteLine("[VIDEO_START] enviado.");

            // 5) Arrancar tasks em paralelo: dados periódicos + heartbeat + vídeo
            var cts = new CancellationTokenSource();
            var dadosTask = TarefaPublicarDadosAsync(channel, cts.Token);
            var heartbeatTask = TarefaHeartbeatAsync(channel, cts.Token);
            var videoTask = TarefaPublicarVideoAsync(channel, cts.Token);

            // 6) Esperar comando do utilizador para terminar
            Console.WriteLine("\n[Enter para terminar o sensor]");
            Console.ReadLine();

            // 7) Sinalizar paragem e esperar que as tasks acabem
            isRunning = false;
            cts.Cancel();

            try
            {
                await Task.WhenAll(dadosTask, heartbeatTask, videoTask);
            }
            catch (OperationCanceledException)
            {
                // esperado — cancelámos nós próprios
            }

            // 8) Publicar VIDEO_END e BYE antes de sair
            await PublicarStatusAsync(channel, "VIDEO_END");
            Console.WriteLine("[VIDEO_END] enviado.");
            await PublicarStatusAsync(channel, "BYE");
            Console.WriteLine("[BYE] enviado. Sensor terminado.");
        }

        // ----------------------------------------------------------------
        // Tarefa de fundo: publica medições aleatórias periodicamente
        // ----------------------------------------------------------------
        private static async Task TarefaPublicarDadosAsync(IChannel channel, CancellationToken ct)
        {
            while (isRunning && !ct.IsCancellationRequested)
            {
                try
                {
                    // Escolher um tipo aleatório dos suportados por este sensor
                    string tipo = tipos[rng.Next(tipos.Count)];
                    (double valor, string unidade) = GerarValor(tipo);

                    // Construir payload JSON
                    var payload = new
                    {
                        type = "DATA",
                        sensorId,
                        zona,
                        tipo,
                        valor = Math.Round(valor, 2),
                        unidade,
                        timestamp = DateTime.UtcNow.ToString("o")
                    };

                    // Routing key: substitui '.' por '_' nos tipos (ex: PM2.5 -> PM2_5)
                    // porque o RabbitMQ usa '.' como separador hierárquico
                    string routingKey = $"sensor.{zona}.{tipo.Replace('.', '_')}";

                    await PublicarAsync(channel, routingKey, payload);
                    Console.WriteLine($"[DATA] {routingKey} -> {payload.valor} {payload.unidade}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO DATA] {ex.Message}");
                }

                try { await Task.Delay(IntervaloDados, ct); }
                catch (TaskCanceledException) { break; }
            }
        }

        // ----------------------------------------------------------------
        // Tarefa de fundo: publica frames de vídeo simulados periodicamente
        // ----------------------------------------------------------------
        private static async Task TarefaPublicarVideoAsync(IChannel channel, CancellationToken ct)
        {
            while (isRunning && !ct.IsCancellationRequested)
            {
                try { await Task.Delay(IntervaloVideo, ct); }
                catch (TaskCanceledException) { break; }

                if (!isRunning) break;

                try
                {
                    int id = System.Threading.Interlocked.Increment(ref frameCounter);

                    // Simulação de conteúdo de frame: string base64 de bytes aleatórios
                    byte[] frameBytes = new byte[64];
                    rng.NextBytes(frameBytes);
                    string conteudo = Convert.ToBase64String(frameBytes);

                    var payload = new
                    {
                        type = "VIDEO_FRAME",
                        sensorId,
                        zona,
                        frameId = id,
                        conteudo,
                        timestamp = DateTime.UtcNow.ToString("o")
                    };

                    string routingKey = $"sensor.{zona}.VIDEO";
                    await PublicarAsync(channel, routingKey, payload);
                    Console.WriteLine($"[VIDEO] frame #{id} -> {routingKey}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO VIDEO] {ex.Message}");
                }
            }
        }

        // ----------------------------------------------------------------
        // Tarefa de fundo: publica heartbeats periódicos
        // ----------------------------------------------------------------
        private static async Task TarefaHeartbeatAsync(IChannel channel, CancellationToken ct)
        {
            while (isRunning && !ct.IsCancellationRequested)
            {
                try { await Task.Delay(IntervaloHeartbeat, ct); }
                catch (TaskCanceledException) { break; }

                if (!isRunning) break;

                try
                {
                    await PublicarStatusAsync(channel, "HEARTBEAT");
                    Console.WriteLine("[HEARTBEAT] enviado.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO HEARTBEAT] {ex.Message}");
                }
            }
        }

        // ----------------------------------------------------------------
        // Publica uma mensagem de controlo (REGISTER/HEARTBEAT/BYE)
        // ----------------------------------------------------------------
        private static async Task PublicarStatusAsync(IChannel channel, string tipoMsg, object? extra = null)
        {
            // Construir payload base. Se 'extra' for fornecido (ex: lista de tipos no REGISTER),
            // mistura os seus campos no JSON.
            var baseObj = new Dictionary<string, object?>
            {
                ["type"] = tipoMsg,
                ["sensorId"] = sensorId,
                ["zona"] = zona,
                ["timestamp"] = DateTime.UtcNow.ToString("o")
            };

            if (extra != null)
            {
                foreach (var prop in extra.GetType().GetProperties())
                {
                    baseObj[prop.Name] = prop.GetValue(extra);
                }
            }

            string routingKey = $"sensor.{zona}.STATUS";
            await PublicarAsync(channel, routingKey, baseObj);
        }

        // ----------------------------------------------------------------
        // Publicação genérica no exchange
        // ----------------------------------------------------------------
        private static async Task PublicarAsync(IChannel channel, string routingKey, object payload)
        {
            string json = JsonSerializer.Serialize(payload);
            byte[] body = Encoding.UTF8.GetBytes(json);

            var props = new BasicProperties
            {
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent  // mensagens sobrevivem se o broker reiniciar
            };

            await channel.BasicPublishAsync(
                exchange: ExchangeName,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: props,
                body: body);
        }

        // ----------------------------------------------------------------
        // Geração de valores realistas por tipo de sensor
        // ----------------------------------------------------------------
        private static (double valor, string unidade) GerarValor(string tipo)
        {
            return tipo.ToUpperInvariant() switch
            {
                "TEMP"      => (rng.NextDouble() * 25 + 5, "°C"),       // 5 a 30 °C
                "HUMIDADE"  => (rng.NextDouble() * 60 + 30, "%"),       // 30 a 90 %
                "PM2.5"     => (rng.NextDouble() * 100 + 5, "µg/m³"),   // 5 a 105 µg/m³
                "NO2"       => (rng.NextDouble() * 200 + 10, "µg/m³"),  // 10 a 210 µg/m³
                "RUIDO"     => (rng.NextDouble() * 40 + 40, "dB"),      // 40 a 80 dB
                _           => (rng.NextDouble() * 100, "u.a.")        // fallback genérico
            };
        }

        // ----------------------------------------------------------------
        // Parser simples de argumentos: --id X --zona Y --tipos A,B,C
        // ----------------------------------------------------------------
        private static void ParseArgs(string[] args)
        {
            for (int i = 0; i < args.Length - 1; i++)
            {
                switch (args[i].ToLowerInvariant())
                {
                    case "--id":
                        sensorId = args[++i];
                        break;
                    case "--zona":
                        zona = args[++i];
                        break;
                    case "--tipos":
                        tipos = new List<string>(args[++i].Split(','));
                        break;
                }
            }
        }
    }
}
