using ChatServer.Core.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Interface
{
    public interface ISessionOwner
    {
        void ReleaseSession(Session session);
    }
}
