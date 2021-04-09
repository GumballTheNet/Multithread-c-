#ifndef AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H

#include <afina/Storage.h>
#include <afina/execute/Command.h>
#include <cstring>
#include <protocol/Parser.h>
#include <spdlog/logger.h>
#include <sys/epoll.h>

namespace Afina {
namespace Network {
namespace MTnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> ps, std::shared_ptr<spdlog::logger> lg)
        : _socket(s), pStorage(ps), _logger(lg) {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
        is_Alive.store(true, std::memory_order_relaxed);
        data_changed.store(false, std::memory_order_relaxed);
    }

    inline bool isAlive() const { return is_Alive.load(std::memory_order_relaxed); }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class Worker;
    friend class ServerImpl;
    std::atomic<bool> is_Alive;
    std::mutex mutex;
    int _socket;
    struct epoll_event _event;
    char client_buffer[4096] = {0};
    std::shared_ptr<spdlog::logger> _logger;
    std::shared_ptr<Afina::Storage> pStorage;
    std::unique_ptr<Execute::Command> command_to_execute;
    Protocol::Parser parser;
    std::size_t arg_remains = 0;
    std::string argument_for_command;
    std::vector<std::string> replies;
    int current_bytes = 0;
    int rest = 0;
    std::atomic<bool> data_changed;
};

} // namespace MTnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
