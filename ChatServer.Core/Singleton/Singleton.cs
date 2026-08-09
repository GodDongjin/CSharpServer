using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Singleton
{
    public abstract class Singleton<T> where T : class
    {
        private static readonly T _instance;
        public static T Instance => _instance;

        static Singleton()
        {
            // 런타임 렉 방지를 위해 서버 켜질 때 '각자' 자식의 생성자를 강제 호출
            _instance = Activator.CreateInstance(typeof(T), true) as T
                ?? throw new InvalidOperationException($"{typeof(T).Name} 생성 실패");
        }

        // static 생성자를 안전하게 깨우기 위한 빈 메서드
        public static void Create() { }
    }
}
