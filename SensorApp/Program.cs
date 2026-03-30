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


    string regMsg = $"REGISTER|{sensorId}|{zona}|PM2.5,TEMP";
    await writer.WriteLineAsync(regMsg);
    string? resReg = await reader.ReadLineAsync();
    Console.WriteLine($"[GATEWAY]: {resReg}");

    _ = Task.Run(async () =>
    {
        while (isRunning)
        {
            await Task.Delay(10000);
            if (isRunning) await writer.WriteLineAsync($"HEARTBEAT|{sensorId}");
        }
    });

    // 3. MENU DE SIMULAÇÃO
    while (isRunning)
    {
        Console.WriteLine("\n1. Enviar PM2.5 (78)\n2. Enviar Temperatura (20)\n0. Sair");
        string? opcao = Console.ReadLine();

        switch (opcao)
        {
            case "1":
                await writer.WriteLineAsync($"DATA|{sensorId}|{zona}|PM2.5|78");
                break;
            case "2":
                await writer.WriteLineAsync($"DATA|{sensorId}|{zona}|TEMP|20");
                break;
            case "0":
                await writer.WriteLineAsync("BYE"); 
                isRunning = false;
                break;
        }

        string? resposta = await reader.ReadLineAsync();
        Console.WriteLine($"[GATEWAY]: {resposta}");
    }
}
catch (Exception ex) { Console.WriteLine($"Erro: {ex.Message}"); }