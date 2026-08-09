using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace C__Server_Study
{
    /// ==============================================================================
    ///                                 Thread
    ///   새로운 스레드를 생성하여 메인 스레드에 방해 받지 않고 작업을 진행 할 수 있다.
    ///   장점: 제어가 용이하고 간단함
    ///   단점: 과도한 스레드 생성 시 시스템 리소스 낭비
    /// ==============================================================================
    class ThreadEntity
    {
        public void StartThread()
        {
            // 스레드 생성
            Thread thread = new(WorkerThread);

            // 스레드 실행
            thread.Start();

            // 메인 스레드에서 병렬 작업 진행
            for(int i = 0; i < 5; i++)
            {
                Console.WriteLine($"메인 스레드 : {i}");
                Thread.Sleep(100);
            }

            // 워커 스레드 종료 까지 대기
            thread.Join();

            Console.WriteLine("프로그램 종료");
        }

        public void WorkerThread()
        {
            for(int i = 0; i < 5; i++)
            {
                Console.WriteLine($"워쿼 스레드 : {i}");
                Thread.Sleep(200);
            }
        }
    }

    /// ==============================================================================
    ///                                 ThreadPool
    ///   ThreadPool에서 스레드를 미리 생성하여 관리해주며 생성/파괴 비용을 줄여 성능을 향상 시켜준다.
    ///   QueueUserWorkItem() : 스레드 생성 및 작업 등록 및 Thread 실행.
    ///   장점: 스레드 재사용으로 효율적인 리소스 관리
    ///   단점: 스레드 개수와 생명주기 직접 제어 불가
    /// ==============================================================================

    class ThreadPoolExample
    {
        public void ThreadPoolStart()
        {
            for(int i = 0; i < 5; i++)
            {
                int taskNum = i;    // 클로저 문제 방지
                ThreadPool.QueueUserWorkItem(state => WorkerThread(taskNum));
            }

            // 모든 작업이 완료될 때까지 대기
            Console.WriteLine("작업 대기 중...");
            Thread.Sleep(2000);

            Console.WriteLine("프로그램 종료");
        }

        public void WorkerThread(int taskNum)
        {
            Console.WriteLine($"ThreadPool 스레드 #{Thread.CurrentThread.ManagedThreadId} : 작업 {taskNum} 시작");
            Thread.Sleep(1000);
            Console.WriteLine($"ThreadPool 스레드 #{Thread.CurrentThread.ManagedThreadId} : 작업 {taskNum} 완료");

        }
    }

    /// ==============================================================================
    ///                                 Task
    ///   threadPool에 이미 생성된 thread에 작업을 부여하여 비동기로 진행된다.
    ///   이때 async/await를 사용하여 실행 흐름을 동기화 해줘야 한다.
    ///   작업 완료되면 결과를 반환해 준다.
    ///   장점: 비동기 작업 처리 용이, 결과 반환 쉬움
    ///   단점: 단순 스레드 모델보다 조금 더 복잡함
    /// ==============================================================================

    class TaskExample
    {
        public async Task StartTask()
        {
            Console.WriteLine("작업 시작");

            // 여러 작업 병렬 실행
            Task[] tasks = new Task[5];
            for(int i =0; i < 5; i++)
            {
                int taskNum = i;
                tasks[i] = Task.Run(() => DoWork(taskNum));
            }

            // 모든 작업 완료 대기
            await Task.WhenAll(tasks);

            Console.WriteLine("모든 작업 완료");
        }

        public async Task DoWork(int taskNumber)
        {
            Console.WriteLine($"작업 {taskNumber} 시작");

            // 비동기 작업 시뮬레이션
            await Task.Delay(1000);

            Console.WriteLine($"작업 {taskNumber} 완료");
        }
    }
}
