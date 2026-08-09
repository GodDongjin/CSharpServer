using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChatServer.Core.Network
{
    public sealed class RecvBuffer : IDisposable
    {
        private byte[] _buffer;
        private Int32 _readPos;     // 다음에 읽을 위치
        private Int32 _writePos;    // 다음에 쓸 위치
        private Int32 _capacity;    // 실제로 사용할 버퍼 크기

        private bool _disposed = false;

        // 아직 처리하지 않은 수신 데이터 영역
        public Memory<byte> ReadMemory => _buffer.AsMemory(_readPos, _writePos - _readPos);

        // 소켓 수신 데이터를 새로 써 넣을 수 있는 빈 영역
        public Memory<byte> WriteMemory => _buffer.AsMemory(_writePos, _capacity - _writePos);

        // 처리 대기 중인 수신 데이터 크기
        public Int32 DataSize => _writePos - _readPos;

        // 뒤쪽에 남아 있는 쓰기 가능 공간 크기
        public Int32 FreeSize => _capacity - _writePos;

        public RecvBuffer(int bufferSize = 64 * 1024)
        {
            // 버퍼 사이즈 채크
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            // ArrayPool에서 수신 버퍼로 사용할 배열을 빌린다.
            _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            // Rent는 요청보다 큰 배열을 줄 수 있으므로, 실제 사용 범위를 따로 제한한다.
            _capacity = bufferSize;
        }

        // 처리되지 않은 데이터를 버퍼 앞쪽으로 당겨 쓰기 공간을 확보한다.
        public void Clear()
        {
            int dataSize = DataSize;

            if (dataSize == 0)
            {
                _readPos = 0;
                _writePos = 0;
                return;
            }

            // 아직 처리하지 않은 데이터만 0번 위치로 이동한다.
            ReadMemory.CopyTo(_buffer.AsMemory(0, dataSize));

            _readPos = 0;
            _writePos = dataSize;
        }

        // 처리한 바이트 수만큼 읽기 위치를 이동한다.
        public bool OnRead(Int32 numOfBytes)
        {
            if (numOfBytes < 0 || numOfBytes > DataSize)
                return false;

            _readPos += numOfBytes;
            return true;
        }

        // 수신한 바이트 수만큼 쓰기 위치를 이동한다.
        public bool OnWrite(Int32 numOfBytes)
        {
            if (numOfBytes < 0 || numOfBytes > FreeSize)
                return false;

            _writePos += numOfBytes;
            return true;
        }

        public void Reset()
        {
            _readPos = 0;
            _writePos = 0;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            // 빌린 배열 ArrayPool 반납
            ArrayPool<byte>.Shared.Return(_buffer, clearArray: false);
            _buffer = Array.Empty<byte>();
            _disposed = true;
        }
    }


    /// =============================== SendBuffer =========================================
    /// 
    /// 통신 송신 버퍼를 관리하는 클래스입니다.
    /// ArrayPool에서 byte 배열을 대여해 사용하며, 세션 종료 시 반드시 반환해야 합니다.
    /// 
    /// ====================================================================================

    public sealed class SendBuffer : IDisposable
    {
        private byte[] _buffer;
        private Int32 _writeSize;   // 송신할 데이터 크기
        private Int32 _capacity;    // 실제로 사용할 버퍼 크기

        private bool _disposed = false;

        // 송신 데이터를 작성할 수 있는 버퍼 영역
        public Memory<byte> Buffer => _buffer.AsMemory(0, _capacity);

        // 실제 송신할 데이터 영역
        public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _writeSize);

        // 송신할 데이터 크기
        public Int32 WriteSize => _writeSize;

        // 실제로 사용할 버퍼 크기
        public Int32 Capacity => _capacity;


        public ArraySegment<byte> ToArraySegment()
        {
            return new ArraySegment<byte>(_buffer, 0, WriteSize);
        }

        public SendBuffer(Int32 bufferSize)
        {
            // 버퍼 크기 확인
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            // ArrayPool에서 수신 버퍼로 사용할 배열을 대여한다.
            _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            // Rent는 요청보다 큰 배열을 줄 수 있으므로, 실제 사용 범위를 따로 제한한다.
            _capacity = bufferSize;
        }

        // 완성된 외부 데이터를 복사하고 송신 크기를 확정한다.
        public void CopyData(ReadOnlyMemory<byte> data)
        {
            // 데이터 크기 확인
            if (data.Length > _capacity)
                throw new ArgumentOutOfRangeException(nameof(data));

            // 외부 데이터를 송신 버퍼에 복사한다.
            data.CopyTo(_buffer.AsMemory(0, _capacity));

            // 송신할 데이터 크기를 기록한다.
            _writeSize = data.Length;
        }

        // 직접 작성한 버퍼의 송신 크기를 확정한다.
        public void Close(Int32 writeSize)
        {
            // 직접 작성한 데이터의 크기를 확정한다.
            if (writeSize < 0 || writeSize > _capacity)
                throw new ArgumentOutOfRangeException(nameof(writeSize));

            _writeSize = writeSize;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            // 빌린 배열 ArrayPool에 반납
            ArrayPool<byte>.Shared.Return(_buffer, clearArray: false);
            _buffer = Array.Empty<byte>();
            _disposed = true;
        }
    }
}
