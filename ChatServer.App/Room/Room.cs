using ChatServer.App.ChatSession;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static System.Runtime.InteropServices.JavaScript.JSType;

using ChatServer.App.Packet;

namespace ChatServer.App.Room
{
    public class Room
    {
        private readonly ConcurrentDictionary<Guid, GameSession> _userList;
        private readonly string _roomName;
        private readonly int _roomMaxCount;

        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);

        public Room(string roomName, int roomMaxCount)
        {
            _userList = new ConcurrentDictionary<Guid, GameSession>();
            _roomName = roomName;
            _roomMaxCount = roomMaxCount;
        }

       /* public bool RoomJoinAsync(Guid user_id, GameSession client)
        {
            await _sendLock.WaitAsync();

            try
            {
                if (_userList.ContainsKey(user_id))
                {
                    byte[] data = Encoding.UTF8.GetBytes($"이미 입장한 방 입니다.");
                    SendBuffer sendBuffer = new SendBuffer(data.Length);
                    data.CopyTo(sendBuffer.Buffer);
                    client.Send(sendBuffer);
                    return;
                }
                else if (_userList.Count >= _roomMaxCount)
                {
                    byte[] data = Encoding.UTF8.GetBytes($"이미 방이 꽉찼습니다.");
                    SendBuffer sendBuffer = new SendBuffer(data.Length);
                    data.CopyTo(sendBuffer.Buffer);
                    client.Send(sendBuffer);
                    return;
                }

                if (_userList.TryAdd(user_id, client))
                {
                    byte[] data = Encoding.UTF8.GetBytes($"{_roomName} 방에 입장했습니다");
                    SendBuffer sendBuffer = new SendBuffer(data.Length);
                    data.CopyTo(sendBuffer.Buffer);
                    client.Send(sendBuffer);

                }
            }
            finally
            {
                _sendLock.Release();
            }
        }

        public async Task RoomLeaveAsync(Guid user_id)
        {
            await _sendLock.WaitAsync();

            try
            {
                if (_userList.TryRemove(user_id, out var user))
                {
                    byte[] data = Encoding.UTF8.GetBytes($"{_roomName} 방에서 퇴장했습니다.");
                    SendBuffer sendBuffer = new SendBuffer(data.Length);
                    data.CopyTo(sendBuffer.Buffer);
                    user.Send(sendBuffer);

                    //await RoomBroadcastMessageAsync($"{user_id}님이 {_roomName}방에서 퇴장하였습니다.", user_id);
                }
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private void RoomBroadcastMessageAsync(string message, Guid senderId)
        {
            var tasks = new List<Task>();

            foreach (var client in _userList.Values)
            {
                if (client._id == senderId)
                {
                    continue;
                }

                byte[] data = Encoding.UTF8.GetBytes(message);
                SendBuffer sendBuffer = new SendBuffer(data.Length);
                data.CopyTo(sendBuffer.Buffer);
                client.Send(sendBuffer);

                //tasks.Add(client.SendMessageAsync(message));
            }

            //await Task.WhenAll(tasks);
        }*/
    }
}
