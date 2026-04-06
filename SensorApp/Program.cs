using System.Net.Sockets;
using System.Text;

string sensorId = "S102";
string zona = "ZONA_ESCOLAR";
int portaGateway = 5000;
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

    [cite_start]// 1. REGISTO: Identificar-se e indicar tipos de dados [cite: 45, 46]
    string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP,RUIDO";
    await writer.WriteLineAsync(regMsg);

    string? resReg = await reader.ReadLineAsync();
    Console.WriteLine($"[GATEWAY]: {resReg}");

    [cite_start]// 2. HEARTBEAT: Tarefa em segundo plano [cite: 49, 51]
    _ = Task.Run(async () =>
    {
        while (isRunning)
        {
            try
            {
                [cite_start] await Task.Delay(10000); // Envia a cada 10 segundos [cite: 52]
                if (isRunning)
                {
                    await writer.WriteLineAsync($"HEARTBEAT|{sensorId}");
                }
            }
            catch { break; }
        }
    });

    [cite_start]// 3. INTERFACE DE SIMULAÇÃO [cite: 53]
    Console.WriteLine("\n--- Simulação de Sensor (One Health) ---");
    Console.WriteLine("Comandos: TIPO:VALOR | VIDEO | SAIR");

    while (isRunning)
    {
        string? input = Console.ReadLine();
        if (string.IsNullOrEmpty(input)) continue;

        string cmd = input.ToUpper();

        [cite_start]// Verificação de saída [cite: 50]
        if (cmd == "SAIR")
        {
            isRunning = false;
            await writer.WriteLineAsync($"QUIT|{sensorId}");
            break;
        }

        [cite_start]// Envio de necessidade de stream de vídeo [cite: 48]
        if (cmd == "VIDEO")
        {
            string videoMsg = $"STREAM_REQ|{sensorId}|VIDEO_START|{DateTime.Now:s}";
            await writer.WriteLineAsync(videoMsg);
            Console.WriteLine($"[SOLICITAÇÃO]: {videoMsg}");
            continue;
        }

        [cite_start]// Envio de medições ambientais (FORA do bloco VIDEO) [cite: 47]
        string[] parts = input.Split(':');
        if (parts.Length == 2)
        {
            string tipo = parts[0].Trim().ToUpper();
            string valor = parts[1].Trim();
            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

            [cite_start]// Formatação: DATA|ID|TIPO|VALOR|TIMESTAMP [cite: 26, 32, 33]
            string dataMsg = $"DATA|{sensorId}|{tipo}|{valor}|{timestamp}";
            await writer.WriteLineAsync(dataMsg);
            Console.WriteLine($"[ENVIADO]: {dataMsg}");
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