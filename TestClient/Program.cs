/*using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SimpleSocketClient
{
    class Program
    {
        private static Socket? clientSocket;
        private static bool isConnected = false;

        static void Main(string[] args)
        {
            Console.WriteLine("채팅 클라이언트 시작");

            try
            {
                // 서버 정보 설정
                IPAddress serverIP = IPAddress.Parse("127.0.0.1");
                int serverPort = 7777;

                // 소켓 생성
                clientSocket = new Socket(AddressFamily.InterNetwork,
                                        SocketType.Stream,
                                        ProtocolType.Tcp);

                // 서버 연결
                Console.WriteLine($"서버 {serverIP}:{serverPort}에 연결 중...");
                clientSocket.Connect(new IPEndPoint(serverIP, serverPort));

                // 연결 성공
                isConnected = true;
                Console.WriteLine("서버에 연결되었습니다.");

                // 메시지 수신 스레드 시작
                Thread receiveThread = new(ReceiveMessages);
                receiveThread.IsBackground = true;
                receiveThread.Start();

                // 메시지 입력 및 전송
                while (isConnected)
                {
                    string? message = Console.ReadLine();

                    if (string.IsNullOrEmpty(message))
                        continue;

                    if (message.ToLower() == "/exit")
                        break;

                    // 메시지 전송
                    SendMessage(message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"오류 발생: {ex.Message}");
            }
            finally
            {
                // 연결 종료
                CloseConnection();
            }

            Console.WriteLine("프로그램을 종료하려면 아무 키나 누르세요...");
            Console.ReadKey();
        }

        // 메시지 수신 메서드
        static void ReceiveMessages()
        {
            try
            {
                byte[] buffer = new byte[1024];

                while (isConnected && clientSocket != null)
                {
                    try
                    {
                        // 메시지 수신
                        int bytesRead = clientSocket.Receive(buffer);

                        if (bytesRead == 0)
                        {
                            // 서버 연결 종료
                            Console.WriteLine("서버와의 연결이 종료되었습니다.");
                            isConnected = false;
                            break;
                        }

                        // 수신된 메시지 처리
                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        Console.WriteLine(message);
                    }
                    catch (SocketException ex)
                    {
                        Console.WriteLine($"메시지 수신 오류: {ex.Message}");
                        isConnected = false;
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"수신 스레드 오류: {ex.Message}");
            }
            finally
            {
                // 연결 종료
                isConnected = false;
                CloseConnection();
            }
        }

        // 메시지 전송 메서드
        static void SendMessage(string message)
        {
            if (isConnected && clientSocket != null)
            {
                try
                {
                    byte[] data = Encoding.UTF8.GetBytes(message);
                    clientSocket.Send(data);
                }
                catch (SocketException ex)
                {
                    Console.WriteLine($"메시지 전송 오류: {ex.Message}");
                    isConnected = false;
                }
            }
        }

        // 연결 종료 메서드
        static void CloseConnection()
        {
            if (clientSocket != null)
            {
                try
                {
                    if (clientSocket.Connected)
                    {
                        clientSocket.Shutdown(SocketShutdown.Both);
                    }
                }
                catch (SocketException)
                {
                    // 이미 연결이 끊겼을 경우 무시
                }
                finally
                {
                    clientSocket.Close();
                    clientSocket = null;
                }

                Console.WriteLine("서버와의 연결이 종료되었습니다.");
            }

            isConnected = false;
        }
    }
}*/

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace TestClient;

internal static class Program
{
    private const ushort HeaderSize = 4;
    private const ushort PktReqChat = 1000;
    private const ushort PktAckChat = 1001;

    private static Socket? _socket;
    private static volatile bool _running;
    private static string _name = "Client";

    private static void Main()
    {
        Console.Write("name: ");
        string? inputName = Console.ReadLine();
        if (!string.IsNullOrWhiteSpace(inputName))
            _name = inputName;

        try
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 7777));

            _running = true;
            Console.WriteLine("서버 연결 완료. 메시지를 입력하세요. 종료: /exit");

            Thread receiveThread = new Thread(ReceiveLoop)
            {
                IsBackground = true
            };
            receiveThread.Start();

            while (_running)
            {
                string? message = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(message))
                    continue;

                if (message.Equals("/exit", StringComparison.OrdinalIgnoreCase))
                    break;

                byte[] packet = MakeReqChat(_name, message);
                _socket.Send(packet);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"오류: {ex.Message}");
        }
        finally
        {
            Close();
        }
    }

    private static byte[] MakeReqChat(string name, string message)
    {
        int payloadSize = GetStringSize(name) + GetStringSize(message);
        int packetSize = HeaderSize + payloadSize;

        if (packetSize > ushort.MaxValue)
            throw new InvalidOperationException("패킷 크기가 너무 큽니다.");

        byte[] buffer = new byte[packetSize];

        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(0, 2), (ushort)packetSize);
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(2, 2), PktReqChat);

        int offset = HeaderSize;
        WriteString(buffer, ref offset, name);
        WriteString(buffer, ref offset, message);

        return buffer;
    }

    private static void ReceiveLoop()
    {
        byte[] recvBuffer = new byte[4096];
        List<byte> pending = new List<byte>();

        try
        {
            while (_running && _socket != null)
            {
                int recvLen = _socket.Receive(recvBuffer);

                if (recvLen == 0)
                {
                    Console.WriteLine("서버 연결 종료");
                    break;
                }

                pending.AddRange(recvBuffer.AsSpan(0, recvLen).ToArray());
                ProcessPackets(pending);
            }
        }
        catch (Exception ex)
        {
            if (_running)
                Console.WriteLine($"수신 오류: {ex.Message}");
        }
        finally
        {
            _running = false;
            Close();
        }
    }

    private static void ProcessPackets(List<byte> pending)
    {
        int processLen = 0;
        byte[] buffer = pending.ToArray();

        while (true)
        {
            int dataSize = buffer.Length - processLen;

            if (dataSize < HeaderSize)
                break;

            ushort packetSize = BinaryPrimitives.ReadUInt16LittleEndian(buffer.AsSpan(processLen, 2));
            ushort packetId = BinaryPrimitives.ReadUInt16LittleEndian(buffer.AsSpan(processLen + 2, 2));

            if (packetSize < HeaderSize)
            {
                Console.WriteLine("잘못된 패킷 크기");
                _running = false;
                break;
            }

            if (dataSize < packetSize)
                break;

            ReadOnlySpan<byte> payload = buffer.AsSpan(
                processLen + HeaderSize,
                packetSize - HeaderSize);

            HandlePacket(packetId, payload);

            processLen += packetSize;
        }

        if (processLen > 0)
            pending.RemoveRange(0, processLen);
    }

    private static void HandlePacket(ushort packetId, ReadOnlySpan<byte> payload)
    {
        switch (packetId)
        {
            case PktAckChat:
                {
                    if (payload.Length < sizeof(ushort))
                    {
                        Console.WriteLine("ACK_CHAT payload size error");
                        break;
                    }

                    ushort state = BinaryPrimitives.ReadUInt16LittleEndian(payload.Slice(0, 2));

                    Console.WriteLine($"ACK_CHAT State: {state}");
                    break;
                }
            default:
                Console.WriteLine($"알 수 없는 패킷 수신: {packetId}");
                break;
        }
    }

    private static int GetStringSize(string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        return sizeof(ushort) + byteCount;
    }

    private static void WriteString(byte[] buffer, ref int offset, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);

        if (byteCount > ushort.MaxValue)
            throw new InvalidOperationException("문자열이 너무 깁니다.");

        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(offset, 2), (ushort)byteCount);
        offset += 2;

        Encoding.UTF8.GetBytes(value, buffer.AsSpan(offset, byteCount));
        offset += byteCount;
    }

    private static string ReadString(ReadOnlySpan<byte> buffer, ref int offset)
    {
        ushort byteCount = BinaryPrimitives.ReadUInt16LittleEndian(buffer.Slice(offset, 2));
        offset += 2;

        string value = Encoding.UTF8.GetString(buffer.Slice(offset, byteCount));
        offset += byteCount;

        return value;
    }

    private static void Close()
    {
        _running = false;

        try
        {
            _socket?.Shutdown(SocketShutdown.Both);
        }
        catch
        {
        }

        try
        {
            _socket?.Close();
        }
        catch
        {
        }

        _socket = null;
    }
}