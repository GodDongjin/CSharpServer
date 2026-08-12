using System;
using System.Collections.Generic;
using System.Text;

namespace ChatServer.App.DataBase
{
    public sealed record Login(UInt64 accountID);

    public sealed class DBTest
    {
        public int TestValue { get; set; }
    };
}
