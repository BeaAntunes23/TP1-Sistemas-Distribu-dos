using System.Net; // Necessário para IPEndPoint e UDP
using System;
using System.Net.Sockets;
using System.Text;

string sensorId = "S102";
string zona = "ZONA_ESCOLAR";
int portaGateway = 5000;
int portaUdpVideo = 5001; // Porta diferente da do Gateway TCP
bool isRunning = true;

Console.Write("IP do Gateway (ex: 127.0.0.1): ");
string? gatewayIP = Console.ReadLine() ?? "127.0.0.1";

try
{
    using TcpClient client = new TcpClient(gatewayIP, portaGateway);
    using NetworkStream stream = client.GetStream();
    using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
    using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

    Console.WriteLine($"\n[CONECTADO] Porta {portaGateway}");

    // 1. REGISTO: Identificar-se e indicar tipos de dados
    string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP,RUIDO";
    await writer.WriteLineAsync(regMsg);

    string? resReg = await reader.ReadLineAsync();
    Console.WriteLine($"[GATEWAY]: {resReg}");

    // 2. HEARTBEAT: Tarefa em segundo plano
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

        // Envio de necessidade de stream de vídeo
        // Envio de necessidade de stream de vídeo (TCP para controlo, UDP para dados)
        if (cmd != "VIDEO")
        {
            // Envio de medições ambientais
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

                // Ler confirmação do gateway
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
        else
        {
            // 1. Sinalização por TCP (Garante que o Gateway sabe que o vídeo vai começar)
            string videoControlMsg = $"STREAM_REQ|{sensorId}|UDP_START|{portaUdpVideo}";
            await writer.WriteLineAsync(videoControlMsg);
            Console.WriteLine($"[CONTROLO TCP]: {videoControlMsg}");

            // 2. Simulação de envio de frames por UDP (Fase 3 - Funcionalidade Extra)
            _ = Task.Run(async () =>
            {
                using UdpClient udpClient = new UdpClient();
                IPEndPoint remoteEP = new IPEndPoint(IPAddress.Parse(gatewayIP), portaUdpVideo);

                Console.WriteLine("[UDP] Streaming de vídeo iniciado...");

                for (int i = 0; i < 50; i++) // Simula o envio de 50 frames
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
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[ERRO DE LIGAÇÃO]: {ex.Message}");
    isRunning = false;
}
