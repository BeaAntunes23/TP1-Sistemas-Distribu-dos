using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SensorApp
{
    class Program
    {
        // Variáveis globais da classe
        private static string sensorId = "S102";
        private static string zona = "ZONA_ESCOLAR";
        private static int portaGateway = 5000;
        private static int portaUdpVideo = 5001;
        private static bool isRunning = true;

        // Garante acesso sequencial ao canal TCP
        private static readonly SemaphoreSlim tcpSemaphore = new SemaphoreSlim(1, 1);

        static async Task Main(string[] args)
        {
            Console.WriteLine("--- Inicializando Sensor ---");
            Console.Write("IP do Gateway (ex: 127.0.0.1): ");
            string gatewayIP = Console.ReadLine() ?? "127.0.0.1";

            try
            {
                using TcpClient client = new TcpClient();
                await client.ConnectAsync(gatewayIP, portaGateway);

                using NetworkStream stream = client.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"\n[CONECTADO] Gateway em {gatewayIP}:{portaGateway}");

                // 1. HELLO
                string? resHello = await EnviarEReceberTcpAsync(writer, reader, "HELLO");
                Console.WriteLine($"[GATEWAY]: {resHello}");

                if (resHello == null || !resHello.StartsWith("OK"))
                {
                    Console.WriteLine("[ERRO] HELLO rejeitado pelo gateway.");
                    return;
                }

                // 2. REGISTO
                string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP,RUIDO";
                string? resReg = await EnviarEReceberTcpAsync(writer, reader, regMsg);
                Console.WriteLine($"[GATEWAY]: {resReg}");

                if (resReg == null || !resReg.StartsWith("OK|REGISTERED"))
                {
                    Console.WriteLine("[ERRO] Registo rejeitado pelo gateway.");
                    return;
                }

                // 3. HEARTBEAT (task de fundo)
                _ = Task.Run(async () =>
                {
                    while (isRunning)
                    {
                        try
                        {
                            await Task.Delay(10000);

                            if (!isRunning)
                                break;

                            string heartbeatMsg = $"HEARTBEAT|{sensorId}";
                            string? hbAck = await EnviarEReceberTcpAsync(writer, reader, heartbeatMsg);

                            if (hbAck != null)
                                Console.WriteLine($"[GATEWAY]: {hbAck}");
                        }
                        catch
                        {
                            break;
                        }
                    }
                });

                // 4. INTERFACE
                Console.WriteLine("\n--- Simulação de Sensor (One Health) ---");
                Console.WriteLine("Comandos: TIPO:VALOR | VIDEO | SAIR");
                Console.WriteLine("Exemplos válidos para S102: PM2.5:78 | TEMP:21 | RUIDO:65");

                while (isRunning)
                {
                    string? input = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(input))
                        continue;

                    string cmd = input.ToUpperInvariant();

                    if (cmd == "SAIR")
                    {
                        isRunning = false;

                        string? byeAck = await EnviarEReceberTcpAsync(writer, reader, $"BYE|{sensorId}");
                        Console.WriteLine($"[GATEWAY]: {byeAck}");
                        break;
                    }

                    if (cmd == "VIDEO")
                    {
                        // 4.1 Início do vídeo por TCP
                        string videoStartMsg = $"VIDEO_START|{sensorId}|{zona}";
                        string? videoStartAck = await EnviarEReceberTcpAsync(writer, reader, videoStartMsg);
                        Console.WriteLine($"[GATEWAY]: {videoStartAck}");

                        if (videoStartAck == null || !videoStartAck.StartsWith("ACK|VIDEO_START"))
                        {
                            Console.WriteLine("[ERRO] Gateway não aceitou o início do vídeo.");
                            continue;
                        }

                        // 4.2 Dados do vídeo por UDP
                        try
                        {
                            using UdpClient udpClient = new UdpClient();
                            IPEndPoint remoteEP = new IPEndPoint(IPAddress.Parse(gatewayIP), portaUdpVideo);

                            Console.WriteLine("[UDP] Streaming iniciado...");

                            for (int i = 0; i < 50; i++)
                            {
                                if (!isRunning)
                                    break;

                                string frameData = $"VIDEO_FRAME|{sensorId}|{zona}|frame{i}";
                                byte[] data = Encoding.UTF8.GetBytes(frameData);

                                await udpClient.SendAsync(data, data.Length, remoteEP);
                                await Task.Delay(100);
                            }

                            Console.WriteLine("[UDP] Streaming terminado.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERRO UDP]: {ex.Message}");
                        }

                        // 4.3 Fim do vídeo por TCP
                        string videoEndMsg = $"VIDEO_END|{sensorId}|{zona}";
                        string? videoEndAck = await EnviarEReceberTcpAsync(writer, reader, videoEndMsg);
                        Console.WriteLine($"[GATEWAY]: {videoEndAck}");

                        continue;
                    }

                    // 5. Envio de dados ambientais
                    if (input.Contains(":"))
                    {
                        string[] parts = input.Split(':', 2);

                        if (parts.Length == 2)
                        {
                            string tipo = parts[0].Trim().ToUpperInvariant();
                            string valor = parts[1].Trim();

                            string dataMsg = $"DATA|{sensorId}|{zona}|{tipo}|{valor}";
                            string? ack = await EnviarEReceberTcpAsync(writer, reader, dataMsg);

                            Console.WriteLine($"[ENVIADO]: {dataMsg}");
                            Console.WriteLine($"[GATEWAY]: {ack}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Formato inválido. Use 'TIPO:VALOR', 'VIDEO' ou 'SAIR'.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO]: {ex.Message}");
            }

            Console.WriteLine("Saindo...");
        }

        // Envia uma mensagem TCP e lê a resposta correspondente
        // Usa semáforo para evitar conflitos entre heartbeat e interface principal
        static async Task<string?> EnviarEReceberTcpAsync(StreamWriter writer, StreamReader reader, string mensagem)
        {
            await tcpSemaphore.WaitAsync();

            try
            {
                await writer.WriteLineAsync(mensagem);
                return await reader.ReadLineAsync();
            }
            finally
            {
                tcpSemaphore.Release();
            }
        }
    }
}
