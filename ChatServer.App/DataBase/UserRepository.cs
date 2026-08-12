using System;
using System.Collections.Generic;
using System.Text;
using ChatServer.Core.DataBase;
using MySqlConnector;

namespace ChatServer.App.DataBase
{
    public sealed class Repository
    {
        private readonly MysqlDataBase _dataBase;

        public Repository(MysqlDataBase dataBase)
        {
            ArgumentNullException.ThrowIfNull(dataBase);

            _dataBase = dataBase;
        }

        public Task<DBTest> DBTestAsync(UInt64 userID, CancellationToken cancellationToken = default)
        {
            return _dataBase.ExecuteAsync(userID,
                async (connection, token) =>
                {
                    await using MySqlCommand command = connection.CreateCommand();

                    command.CommandType = System.Data.CommandType.StoredProcedure;

                    command.CommandText = "p_test";

                    await using MySqlDataReader reader = await command.ExecuteReaderAsync(token);

                   if(!await reader.ReadAsync(token))
                    {
                        throw new InvalidOperationException("p_test 프로시저가 결과를 반환하지 않았습니다.");
                    }

                    return new DBTest
                    {
                        TestValue = reader.GetInt32("TestValue")
                    };
                   
                }, cancellationToken);
           
        }
    }
}
