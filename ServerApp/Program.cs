using System.Net;
using System;
using System.IO;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Concurrent;

int portaServidor = 6000;
TcpListener listener = new(IPAddress.Any, portaServidor);
listener.Start();
Console.WriteLine($"Servidor à escuta na porta {portaServidor}...");

var fileLocks = new ConcurrentDictionary<string, object>();

while (true)
{
    var cliente = await listener.AcceptTcpClientAsync();
    _ = Task.Run(async () =>
    {
        using var client = cliente;
        using var stream = client.GetStream();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

        while (true)
        {
            string? linha;
            try
            {
                linha = await reader.ReadLineAsync();
            }
            catch
            {
                break;
            }

            if (linha == null)
                break;

            Console.WriteLine($"Recebido do gateway: {linha}");

            // Accept both STORE|sensor|zona|tipo|valor|timestamp and FORWARD;sensor;tipo;valor;timestamp
            if (linha.StartsWith("STORE|") || linha.StartsWith("FORWARD;") || linha.StartsWith("FORWARD|"))
            {
                string[] partes;
                if (linha.Contains("|")) partes = linha.Split('|');
                else partes = linha.Split(';');

                // Normalize fields for both formats
                // Expected normalized: partes[0]=STORE/FORWARD, partes[1]=sensorId, partes[2]=zona or tipo, partes[3]=tipo or valor, partes[4]=valor or timestamp, partes[5]=timestamp (optional)
                string sensorId = partsGet(partes, 1);
                string zona = "UNKNOWN";
                string tipo = "UNKNOWN";
                string valor = "";
                string timestamp = DateTime.Now.ToString("s");

                if (partes[0].Equals("STORE", StringComparison.OrdinalIgnoreCase))
                {
                    // STORE|sensorId|zona|tipo|valor|timestamp?
                    sensorId = partsGet(partes, 1);
                    zona = partsGet(partes, 2);
                    tipo = partsGet(partes, 3);
                    valor = partsGet(partes, 4);
                    if (partes.Length >= 6) timestamp = partsGet(partes, 5);
                }
                else
                {
                    // FORWARD;sensorId;tipo;valor;timestamp?  OR FORWARD;sensorId;zona;tipo;valor
                    // try to detect
                    sensorId = partsGet(partes, 1);
                    if (partes.Length >= 5)
                    {
                        // assume FORWARD;sensor;tipo;valor;timestamp
                        tipo = partsGet(partes, 2);
                        valor = partsGet(partes, 3);
                        if (partes.Length >= 5) timestamp = partsGet(partes, 4);
                    }
                }

                if (string.IsNullOrEmpty(tipo) || string.IsNullOrEmpty(valor))
                {
                    await writer.WriteLineAsync("ERROR|STORE");
                }
                else
                {
                    // Validate timestamp (expecting ISO 8601 without offset: "s" / yyyy-MM-ddTHH:mm:ss)
                    if (!DateTime.TryParseExact(timestamp, "s", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedTs))
                    {
                        await writer.WriteLineAsync("ERROR|TIMESTAMP");
                    }
                    else
                    {
                        string dir = "data";
                        Directory.CreateDirectory(dir);
                        string filename = Path.Combine(dir, $"data_{tipo}.csv");

                        var lockObj = fileLocks.GetOrAdd(filename, _ => new object());
                        lock (lockObj)
                        {
                            bool novo = !File.Exists(filename);
                            using var fs = new FileStream(filename, FileMode.Append, FileAccess.Write, FileShare.Read);
                            using var sw = new StreamWriter(fs, Encoding.UTF8);
                            if (novo)
                            {
                                sw.WriteLine("timestamp,sensorId,zona,valor");
                            }
                            sw.WriteLine($"{parsedTs:s},{sensorId},{zona},{valor}");
                        }

                        await writer.WriteLineAsync("ACK_SAVE");
                    }
                }
            }
            else
            {
                await writer.WriteLineAsync("ERROR|UNKNOWN");
            }

            static string partsGet(string[] p, int idx) => (idx < p.Length) ? p[idx] : string.Empty;
        }
    });
}
