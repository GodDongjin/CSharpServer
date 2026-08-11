using System.Net;

using ChatServer.Core.DataBase;

class Program
{
    static async Task Main(string[] args)
    {


        string connectionString =
            "Server=192.168.75.13;" +
            "Port=3306;" +
            "Database=gamedb;" +
            "User ID=admin;" +
            "Password=ngames1@@;" +
            "Pooling=True;" +
            "MinimumPoolSize=0;" +
            "MaximumPoolSize=50;";

        MysqlDataBase _DB = new MysqlDataBase(connectionString);

        if (!await _DB.IsConnect())
            return;


        _DB.Start();

      


    }
}