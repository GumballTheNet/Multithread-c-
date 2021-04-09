
#ifndef AFINA_NETWORK_ST_COROUTINE_CONNECTION_H
#define AFINA_NETWORK_ST_COROUTINE_CONNECTION_H

#include <afina/execute/Command.h>
#include <cstring>

#include <afina/Storage.h>
#include <afina/coroutine/Engine.h>
#include <protocol/Parser.h>
#include <spdlog/logger.h>
#include <sys/epoll.h>

namespace Afina {
namespace Network {
namespace STcoroutine {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> ps, std::shared_ptr<spdlog::logger> lg)
        : _socket(s), pStorage(ps), _logger(lg) {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
    }

    inline bool isAlive() const { return is_Alive; }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();
    void cor_fun(Afina::Coroutine::Engine &engine);

private:
    friend class ServerImpl;

    bool is_Alive = true;

    Afina::Coroutine::Engine::context *_ctx;
    Afina::Coroutine::Engine::context *maini;
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
    bool no_read = false;
    int rest = 0;
    bool not_yield = false;
};
} // namespace STcoroutine
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_ST_COROUTINE_CONNECTION_H
