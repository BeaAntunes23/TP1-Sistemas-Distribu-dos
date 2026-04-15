using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace SensorApp
{
    class Program
    {
        // Variáveis globais da classe (precisam de ser static para o Main aceder)
        private static string sensorId = "S102";
        private static string zona = "ZONA_ESCOLAR";
        private static int portaGateway = 5000;
        private static int portaUdpVideo = 5001;
        private static bool isRunning = true;

        static async Task Main(string[] args)
        {
            Console.WriteLine("--- Inicializando Sensor ---");
            Console.Write("IP do Gateway (ex: 127.0.0.1): ");
            string gatewayIP = Console.ReadLine() ?? "127.0.0.1";

            try
            {
                // Configuração do Cliente TCP
                using TcpClient client = new TcpClient();
                await client.ConnectAsync(gatewayIP, portaGateway);

                using NetworkStream stream = client.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"\n[CONECTADO] Gateway em {gatewayIP}:{portaGateway}");

                // 1. REGISTO
                string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP,RUIDO";
                await writer.WriteLineAsync(regMsg);

                string? resReg = await reader.ReadLineAsync();
                Console.WriteLine($"[GATEWAY]: {resReg}");

                // 2. HEARTBEAT (Thread de fundo)
                _ = Task.Run(async () =>
                {
                    while (isRunning)
                    {
                        try
                        {
                            await Task.Delay(10000); // 10 segundos
                            if (isRunning)
                            {
                                await writer.WriteLineAsync($"HEARTBEAT|{sensorId}");
                            }
                        }
                        catch { break; }
                    }
                });

                // 3. INTERFACE
                Console.WriteLine("\n--- Simulação de Sensor (One Health) ---");
                Console.WriteLine("Comandos: TIPO:VALOR | VIDEO | SAIR");

                while (isRunning)
                {
                    string? input = Console.ReadLine();
                    if (string.IsNullOrEmpty(input)) continue;

                    string cmd = input.ToUpper();

                    if (cmd == "SAIR")
                    {
                        isRunning = false;
                        await writer.WriteLineAsync($"BYE|{sensorId}");
                        break;
                    }

                    if (cmd == "VIDEO")
                    {
                        // Controlo por TCP
                        string videoControlMsg = $"STREAM_REQ|{sensorId}|UDP_START|{portaUdpVideo}";
                        await writer.WriteLineAsync(videoControlMsg);
                        Console.WriteLine($"[CONTROLO TCP]: {videoControlMsg}");

                        // Dados por UDP (Thread de fundo)
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                using UdpClient udpClient = new UdpClient();
                                IPEndPoint remoteEP = new IPEndPoint(IPAddress.Parse(gatewayIP), portaUdpVideo);
                                Console.WriteLine("[UDP] Streaming iniciado...");

                                for (int i = 0; i < 50; i++)
                                {
                                    if (!isRunning) break;
                                    string frameData = $"FRAME|{sensorId}|{i}|{DateTime.Now:HH:mm:ss.fff}";
                                    byte[] data = Encoding.UTF8.GetBytes(frameData);
                                    await udpClient.SendAsync(data, data.Length, remoteEP);
                                    await Task.Delay(100);
                                }
                                Console.WriteLine("[UDP] Streaming terminado.");
                            }
                            catch (Exception ex) { Console.WriteLine($"[ERRO UDP]: {ex.Message}"); }
                        });
                        continue;
                    }

                    // Envio de Dados Ambientais
                    if (input.Contains(":"))
                    {
                        string[] parts = input.Split(':');
                        if (parts.Length == 2)
                        {
                            string tipo = parts[0].Trim().ToUpper();
                            string valor = parts[1].Trim();
                            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                            string dataMsg = $"DATA|{sensorId}|{zona}|{tipo}|{valor}|{timestamp}";
                            await writer.WriteLineAsync(dataMsg);
                            Console.WriteLine($"[ENVIADO]: {dataMsg}");

                            string? ack = await reader.ReadLineAsync();
                            Console.WriteLine($"[GATEWAY]: {ack}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Formato inválido. Use 'TIPO:VALOR' ou 'VIDEO'.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO]: {ex.Message}");
            }

            Console.WriteLine("Saindo...");
        }
    }
}
