using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.App.Packet
{
    public enum CHAT_STATE
    {
        NONE = 0,
        FAIL = 1,
        SUCCESS = 2,
    }

    public enum GET_ROOM_INFO_STATE
    {
        NONE = 0,
        FAIL = 1,
        SUCCESS = 2,
    }
}
