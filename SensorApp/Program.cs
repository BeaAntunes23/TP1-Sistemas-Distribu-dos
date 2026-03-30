using System.Net.Sockets;
using System.Text;

// --- CONFIGURAÇÕES INICIAIS ---
string sensorId = "S102";
int porta = 12345;
bool isRunning = true;

Console.Write("Introduza o IP do Gateway (ex: 127.0.0.1): ");
string? gatewayIP = Console.ReadLine();

if (string.IsNullOrEmpty(gatewayIP)) return;

try
{
    // 1. ESTABELECER LIGAÇÃO
    using TcpClient client = new TcpClient(gatewayIP, porta);
    using NetworkStream stream = client.GetStream();
    Console.WriteLine($"\n[CONECTADO] Ligado ao Gateway em {gatewayIP}:{porta}");

    // 2. IDENTIFICAÇÃO E REGISTO
    string regMsg = $"REGISTER;{sensorId};PM2.5|RUIDO|TEMP";
    SendMessage(stream, regMsg);

    // 3. THREAD PARA HEARTBEAT (A cada 10s)
    Thread hbThread = new Thread(() =>
    {
        while (isRunning)
        {
            Thread.Sleep(10000);
            if (isRunning) SendMessage(stream, $"HB;{sensorId}");
        }
    });
    hbThread.IsBackground = true;
    hbThread.Start();

    // 4. INTERFACE DE TEXTO (SIMULAÇÃO)
    while (isRunning)
    {
        Console.WriteLine($"\n--- MENU SENSOR ({sensorId}) ---");
        Console.WriteLine("1. Enviar PM2.5 (Ex: 78)");
        Console.WriteLine("2. Enviar Ruído (Ex: 72)");
        Console.WriteLine("3. Enviar Temperatura");
        Console.WriteLine("0. Sair");
        Console.Write("Escolha uma opção: ");

        string? opcao = Console.ReadLine();
        string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

        switch (opcao)
        {
            case "1":
                SendMessage(stream, $"DATA;{sensorId};PM2.5;78;{timestamp}");
                break;
            case "2":
                SendMessage(stream, $"DATA;{sensorId};RUIDO;72;{timestamp}");
                break;
            case "3":
                SendMessage(stream, $"DATA;{sensorId};TEMP;20;{timestamp}");
                break;
            case "0":
                SendMessage(stream, $"QUIT;{sensorId}");
                isRunning = false;
                break;
            default:
                Console.WriteLine("Opção inválida.");
                break;
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[ERRO] Não foi possível ligar ao Gateway: {ex.Message}");
}

void SendMessage(NetworkStream stream, string message)
{
    try
    {
        byte[] buffer = Encoding.UTF8.GetBytes(message);
        stream.Write(buffer, 0, buffer.Length);
        Console.WriteLine($"[ENVIADO] {message}");
    }
    catch { Console.WriteLine("[ERRO] Falha ao enviar mensagem."); }
}