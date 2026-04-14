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
        // Variáveis de configuração
        private static string sensorId = "S102";
        private static string zona = "ZONA_ESCOLAR";
        private static int portaGateway = 5000;
        private static int portaUdpVideo = 5001;
        private static bool isRunning = true;

        static async Task Main(string[] args)
        {
            Console.Write("IP do Gateway (ex: 127.0.0.1): ");
            string gatewayIP = Console.ReadLine() ?? "127.0.0.1";

            try
            {
                using TcpClient client = new TcpClient(gatewayIP, portaGateway);
                using NetworkStream stream = client.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine($"\n[CONECTADO] Gateway em {gatewayIP}:{portaGateway}");

                // 1. REGISTO: Identificar-se e indicar tipos de dados
                string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP,RUIDO";
                await writer.WriteLineAsync(regMsg);

                string? resReg = await reader.ReadLineAsync();
                Console.WriteLine($"[GATEWAY]: {resReg}");

                // 2. HEARTBEAT: Tarefa em segundo plano (TCP)
                _ = Task.Run(async () =>
                {
                    while (isRunning)
                    {
                        try
                        {
                            await Task.Delay(10000); // Envia a cada 10 segundos
                            if (isRunning)
                            {
                                await writer.WriteLineAsync($"HEARTBEAT|{sensorId}");
                            }
                        }
                        catch { break; }
                    }
                });

                // 3. INTERFACE DE SIMULAÇÃO
                Console.WriteLine("\n--- Simulação de Sensor (One Health) ---");
                Console.WriteLine("Comandos: TIPO:VALOR | VIDEO | SAIR");

                while (isRunning)
                {
                    string? input = Console.ReadLine();
                    if (string.IsNullOrEmpty(input)) continue;

                    string cmd = input.ToUpper();

                    // Verificação de saída
                    if (cmd == "SAIR")
                    {
                        isRunning = false;
                        await writer.WriteLineAsync($"BYE|{sensorId}");
                        break;
                    }

                    // Comando de Vídeo (TCP para controlo, UDP para streaming)
                    if (cmd == "VIDEO")
                    {
                        // Sinalização por TCP
                        string videoControlMsg = $"STREAM_REQ|{sensorId}|UDP_START|{portaUdpVideo}";
                        await writer.WriteLineAsync(videoControlMsg);
                        Console.WriteLine($"[CONTROLO TCP]: {videoControlMsg}");

                        // Simulação de streaming por UDP em background
                        _ = Task.Run(async () =>
                        {
                            using UdpClient udpClient = new UdpClient();
                            IPEndPoint remoteEP = new IPEndPoint(IPAddress.Parse(gatewayIP), portaUdpVideo);

                            Console.WriteLine("[UDP] Streaming de vídeo iniciado...");

                            for (int i = 0; i < 50; i++) // Envia 50 frames
                            {
                                string frameData = $"FRAME|{sensorId}|{i}|{DateTime.Now:HH:mm:ss.fff}";
                                byte[] data = Encoding.UTF8.GetBytes(frameData);
                                await udpClient.SendAsync(data, data.Length, remoteEP);
                                await Task.Delay(100); // Simula 10 FPS
                            }
                            Console.WriteLine("[UDP] Streaming de vídeo terminado.");
                        });
                        continue;
                    }

                    // Envio de medições ambientais (TIPO:VALOR)
                    string[] parts = input.Split(':');
                    if (parts.Length == 2)
                    {
                        string tipo = parts[0].Trim().ToUpper();
                        string valor = parts[1].Trim();
                        string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                        // Formato: DATA|sensorId|zona|tipo|valor|timestamp
                        string dataMsg = $"DATA|{sensorId}|{zona}|{tipo}|{valor}|{timestamp}";
                        await writer.WriteLineAsync(dataMsg);
                        Console.WriteLine($"[ENVIADO]: {dataMsg}");

                        // Ler confirmação (ACK) do gateway
                        try
                        {
                            string? ack = await reader.ReadLineAsync();
                            Console.WriteLine($"[GATEWAY]: {ack}");
                        }
                        catch { }
                    }
                    else
                    {
                        Console.WriteLine("Formato inválido. Use 'TIPO:VALOR' ou 'VIDEO'.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO DE LIGAÇÃO]: {ex.Message}");
                isRunning = false;
            }

            Console.WriteLine("Aplicação Sensor terminada. Prima qualquer tecla para sair.");
            Console.ReadKey();
        }
    }
}
