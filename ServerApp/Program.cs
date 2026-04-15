using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Servidor
{
    class Program
    {
        // Porta TCP do servidor (usada pelo Gateway para se ligar)
        private static int portaTcp = 6000;

        // Diretório onde os ficheiros de dados são guardados
        private static string dirDados = "dados";

        // Mutex global para acesso concorrente por ficheiro
        // Chave: nome do ficheiro; Valor: mutex desse ficheiro
        private static readonly Dictionary<string, Mutex> fileMutexes = new();
        private static readonly object fileMutexesMeta = new();

        // Sessões de vídeo ativas: sensorId -> zona
        private static readonly Dictionary<string, string> sessoesVideo = new();
        private static readonly object videoLock = new();

        static async Task Main(string[] args)
        {
            // Criar diretório de dados se não existir
            Directory.CreateDirectory(dirDados);

            TcpListener listener = new TcpListener(IPAddress.Any, portaTcp);
            listener.Start();

            Console.WriteLine($"[SERVIDOR] À escuta na porta TCP {portaTcp}...");
            Console.WriteLine($"[SERVIDOR] Dados guardados em: {Path.GetFullPath(dirDados)}");

            while (true)
            {
                TcpClient clienteGateway = await listener.AcceptTcpClientAsync();
                Console.WriteLine($"[SERVIDOR] Gateway ligado: {clienteGateway.Client.RemoteEndPoint}");
                _ = Task.Run(() => TratarGateway(clienteGateway));
            }
        }

        static async Task TratarGateway(TcpClient clienteGateway)
        {
            using (clienteGateway)
            using (NetworkStream stream = clienteGateway.GetStream())
            using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
            using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            {
                try
                {
                    string? mensagem = await reader.ReadLineAsync();
                    if (mensagem == null)
                    {
                        Console.WriteLine("[SERVIDOR] Ligação encerrada sem mensagem.");
                        return;
                    }

                    Console.WriteLine($"[SERVIDOR] Recebido: {mensagem}");

                    string[] partes = mensagem.Split('|');
                    if (partes.Length == 0)
                    {
                        await writer.WriteLineAsync("ERROR|FORMAT");
                        return;
                    }

                    string comando = partes[0].ToUpperInvariant();

                    switch (comando)
                    {
                        // ----------------------------------------------------------------
                        // HELLO_GATEWAY
                        // Primeiro passo do handshake do Gateway
                        // ----------------------------------------------------------------
                        case "HELLO_GATEWAY":
                            Console.WriteLine("[SERVIDOR] HELLO_GATEWAY recebido.");
                            await writer.WriteLineAsync("ACK|HELLO_GATEWAY");
                            break;

                        // ----------------------------------------------------------------
                        // GATEWAY_REGISTER|gatewayId
                        // Registo do Gateway no Servidor
                        // ----------------------------------------------------------------
                        case "GATEWAY_REGISTER":
                            if (partes.Length < 2)
                            {
                                await writer.WriteLineAsync("ERROR|GATEWAY_REGISTER_FORMAT");
                                return;
                            }
                            Console.WriteLine($"[SERVIDOR] Gateway registado: {partes[1]}");
                            await writer.WriteLineAsync("ACK|GATEWAY_REGISTER");
                            break;

                        // ----------------------------------------------------------------
                        // SESSION_START|gatewayId
                        // Início de sessão do Gateway — último passo do handshake
                        // ----------------------------------------------------------------
                        case "SESSION_START":
                            if (partes.Length < 2)
                            {
                                await writer.WriteLineAsync("ERROR|SESSION_START_FORMAT");
                                return;
                            }
                            Console.WriteLine($"[SERVIDOR] Sessão iniciada para gateway: {partes[1]}");
                            await writer.WriteLineAsync("ACK|SESSION_START");
                            break;

                        // ----------------------------------------------------------------
                        // STORE|sensorId|zona|tipo|valor
                        // Guarda medição ambiental em ficheiro CSV por tipo de dado
                        // ----------------------------------------------------------------
                        case "STORE":
                            if (partes.Length < 5)
                            {
                                await writer.WriteLineAsync("ERROR|STORE_FORMAT");
                                return;
                            }

                            string sensorId = partes[1];
                            string zona = partes[2];
                            string tipo = partes[3];
                            string valor = partes[4];
                            string timestamp = DateTime.Now.ToString("s"); // ISO 8601

                            // Guardar em ficheiro: dados/<TIPO>.csv
                            string nomeFicheiro = Path.Combine(dirDados, $"{tipo.ToUpper()}.csv");

                            GuardarMedicao(nomeFicheiro, sensorId, zona, tipo, valor, timestamp);

                            Console.WriteLine($"[SERVIDOR] STORE: {sensorId} | {zona} | {tipo} = {valor} @ {timestamp}");
                            await writer.WriteLineAsync("ACK|STORE");
                            break;

                        // ----------------------------------------------------------------
                        // VIDEO_START|sensorId|zona
                        // Regista início de sessão de vídeo
                        // ----------------------------------------------------------------
                        case "VIDEO_START":
                            if (partes.Length < 3)
                            {
                                await writer.WriteLineAsync("ERROR|VIDEO_START_FORMAT");
                                return;
                            }

                            string vsId = partes[1];
                            string vsZona = partes[2];

                            lock (videoLock)
                            {
                                sessoesVideo[vsId] = vsZona;
                            }

                            Console.WriteLine($"[SERVIDOR] VIDEO_START: sensor={vsId} zona={vsZona}");
                            await writer.WriteLineAsync("ACK|VIDEO_START");
                            break;

                        // ----------------------------------------------------------------
                        // VIDEO_END|sensorId|zona
                        // Regista fim de sessão de vídeo
                        // ----------------------------------------------------------------
                        case "VIDEO_END":
                            if (partes.Length < 3)
                            {
                                await writer.WriteLineAsync("ERROR|VIDEO_END_FORMAT");
                                return;
                            }

                            string veId = partes[1];

                            lock (videoLock)
                            {
                                sessoesVideo.Remove(veId);
                            }

                            Console.WriteLine($"[SERVIDOR] VIDEO_END: sensor={veId}");
                            await writer.WriteLineAsync("ACK|VIDEO_END");
                            break;

                        // ----------------------------------------------------------------
                        // VIDEO_FRAME|sensorId|zona|conteudo
                        // Regista frame de vídeo (simulação — guarda metadados em CSV)
                        // ----------------------------------------------------------------
                        case "VIDEO_FRAME":
                            if (partes.Length < 4)
                            {
                                await writer.WriteLineAsync("ERROR|VIDEO_FRAME_FORMAT");
                                return;
                            }

                            string vfId = partes[1];
                            string vfZona = partes[2];
                            string vfConteudo = partes[3];
                            string vfTimestamp = DateTime.Now.ToString("s");

                            // Verificar se sessão de vídeo está ativa
                            bool sessaoAtiva;
                            lock (videoLock)
                            {
                                sessaoAtiva = sessoesVideo.ContainsKey(vfId);
                            }

                            if (!sessaoAtiva)
                            {
                                Console.WriteLine($"[SERVIDOR] VIDEO_FRAME ignorado: sessão não iniciada para {vfId}");
                                await writer.WriteLineAsync("ERROR|NO_VIDEO_SESSION");
                                return;
                            }

                            // Guardar metadados do frame em ficheiro de log de vídeo
                            string ficheiroVideo = Path.Combine(dirDados, "VIDEO_FRAMES.csv");
                            GuardarFrame(ficheiroVideo, vfId, vfZona, vfConteudo, vfTimestamp);

                            Console.WriteLine($"[SERVIDOR] VIDEO_FRAME: {vfId} | frame={vfConteudo} @ {vfTimestamp}");
                            await writer.WriteLineAsync("ACK|VIDEO_FRAME");
                            break;

                        default:
                            Console.WriteLine($"[SERVIDOR] Comando desconhecido: {comando}");
                            await writer.WriteLineAsync("ERROR|UNKNOWN_COMMAND");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] Erro ao tratar gateway: {ex.Message}");
                }
            }

            Console.WriteLine("[SERVIDOR] Ligação com gateway encerrada.");
        }

        // -----------------------------------------------------------------------
        // Guarda uma medição ambiental em CSV com mutex por ficheiro
        // Formato: timestamp,sensor_id,zona,tipo,valor
        // -----------------------------------------------------------------------
        static void GuardarMedicao(string ficheiro, string sensorId, string zona,
                                   string tipo, string valor, string timestamp)
        {
            Mutex mutex = ObterMutex(ficheiro);
            mutex.WaitOne();

            try
            {
                bool existe = File.Exists(ficheiro);

                using StreamWriter sw = new StreamWriter(ficheiro, append: true, Encoding.UTF8);

                // Cabeçalho apenas na primeira linha
                if (!existe)
                    sw.WriteLine("timestamp,sensor_id,zona,tipo,valor");

                sw.WriteLine($"{timestamp},{sensorId},{zona},{tipo},{valor}");
            }
            finally
            {
                mutex.ReleaseMutex();
            }
        }

        // -----------------------------------------------------------------------
        // Guarda metadados de frame de vídeo em CSV com mutex por ficheiro
        // Formato: timestamp,sensor_id,zona,frame
        // -----------------------------------------------------------------------
        static void GuardarFrame(string ficheiro, string sensorId, string zona,
                                 string conteudo, string timestamp)
        {
            Mutex mutex = ObterMutex(ficheiro);
            mutex.WaitOne();

            try
            {
                bool existe = File.Exists(ficheiro);

                using StreamWriter sw = new StreamWriter(ficheiro, append: true, Encoding.UTF8);

                if (!existe)
                    sw.WriteLine("timestamp,sensor_id,zona,frame");

                sw.WriteLine($"{timestamp},{sensorId},{zona},{conteudo}");
            }
            finally
            {
                mutex.ReleaseMutex();
            }
        }

        // -----------------------------------------------------------------------
        // Obtém (ou cria) um mutex associado a cada ficheiro
        // Garante acesso sequencial por ficheiro mesmo com múltiplos gateways
        // -----------------------------------------------------------------------
        static Mutex ObterMutex(string ficheiro)
        {
            lock (fileMutexesMeta)
            {
                if (!fileMutexes.ContainsKey(ficheiro))
                    fileMutexes[ficheiro] = new Mutex();

                return fileMutexes[ficheiro];
            }
        }
    }
}
