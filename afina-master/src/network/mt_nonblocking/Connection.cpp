#include "Connection.h"

#include <iostream>
#include <sys/uio.h>

namespace Afina {
namespace Network {
namespace MTnonblock {

// See Connection.h
void Connection::Start() {
    _logger->debug("Start {} socket", _socket);
    _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
    _event.data.fd = _socket;
    _event.data.ptr = this;
    command_to_execute.reset();
    argument_for_command.resize(0);
    parser.Reset();
    arg_remains = 0;
    rest = 0;
    current_bytes = 0;
    replies.clear();
}

// See Connection.h
void Connection::OnError() {
    _logger->debug("Error on {} socket", _socket);
    is_Alive.store(false, std::memory_order_relaxed);
}

// See Connection.h
void Connection::OnClose() {
    _logger->debug("Close {} socket", _socket);
    is_Alive.store(false, std::memory_order_relaxed);
}

// See Connection.h
void Connection::DoRead() {
    std::atomic_thread_fence(std::memory_order_acquire);
    _logger->debug("Do read on {} socket", _socket);
    try {
        int read_count = 0;
        while ((read_count = read(_socket, client_buffer + current_bytes, sizeof(client_buffer) - current_bytes)) > 0) {
            current_bytes += read_count;
            _logger->debug("Got {} bytes from socket", read_count);
            while (current_bytes > 0) {
                _logger->debug("Process {} bytes", current_bytes);
                // There is no command yet
                if (!command_to_execute) {
                    std::size_t parsed = 0;
                    if (parser.Parse(client_buffer, current_bytes, parsed)) {
                        // There is no command to be launched, continue to parse input stream
                        // Here we are, current chunk finished some command, process it
                        _logger->debug("Found new command: {} in {} bytes", parser.Name(), parsed);
                        command_to_execute = parser.Build(arg_remains);
                        if (arg_remains > 0) {
                            arg_remains += 2;
                        }
                    }
                    // Parsed might fails to consume any bytes from input stream. In real life that could happens,
                    // for example, because we are working with UTF-16 chars and only 1 byte left in stream
                    if (parsed == 0) {
                        break;
                    } else {
                        std::memmove(client_buffer, client_buffer + parsed, current_bytes - parsed);
                        current_bytes -= parsed;
                    }
                }
                // There is command, but we still wait for argument to arrive...
                if (command_to_execute && arg_remains > 0) {
                    _logger->debug("Fill argument: {} bytes of {}", current_bytes, arg_remains);
                    // There is some parsed command, and now we are reading argument
                    std::size_t to_read = std::min(arg_remains, std::size_t(current_bytes));
                    argument_for_command.append(client_buffer, to_read - 2 * (to_read == arg_remains));
                    std::memmove(client_buffer, client_buffer + to_read, current_bytes - to_read);
                    arg_remains -= to_read;
                    current_bytes -= to_read;
                }
                // There is command & argument - RUN!
                if (command_to_execute && arg_remains == 0) {
                    _logger->debug("Start command execution");
                    std::string result;
                    command_to_execute->Execute(*pStorage, argument_for_command, result);
                    // Send response
                    result += "\r\n";
                    replies.push_back(result);
                    if (replies.size() == 1) {
                        _event.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLOUT;
                    }
                    // Prepare for the next command
                    command_to_execute.reset();
                    argument_for_command.resize(0);
                    parser.Reset();
                }
            }
        } // while (read_count)
        if (current_bytes == 0) {
            _logger->debug("End reading");
            data_changed.store(true, std::memory_order_release);
        } else {
            if (!(errno == EAGAIN || errno == EINTR)) {
                throw std::runtime_error("Failed to response");
            }
        }
    } catch (std::runtime_error &ex) {
        OnError();
        std::string ErrorReply("ERROR!Failed to process connection.\r\n");
        write(_socket, ErrorReply.data(), ErrorReply.size());
        data_changed.store(true, std::memory_order_release);
    }
}

// See Connection.h
void Connection::DoWrite() {
    if (!data_changed.load(std::memory_order_acquire)) {
        return;
    }
    try {
        if (replies.empty()) {
            return;
        }
        _logger->debug("Write on {} socket", _socket);
        struct iovec messages[replies.size()];
        messages[0].iov_len = replies[0].size() - rest;
        messages[0].iov_base = &(replies[0][0]) + rest;
        for (int i = 1; i < replies.size(); i++) {
            messages[i].iov_len = replies[i].size();
            messages[i].iov_base = &(replies[i][0]);
        }
        int written;
        if ((written = writev(_socket, messages, replies.size())) <= 0) {
            if (!(errno == EAGAIN || errno == EINTR)) {
                throw std::runtime_error("Failed to response");
            } else {
                std::atomic_thread_fence(std::memory_order_release);
                return;
            }
        }
        rest += written;
        int i = 0;
        for (; !(i >= replies.size() || (rest - messages[i].iov_len) < 0); i++) {
            rest -= messages[i].iov_len;
        }
        replies.erase(replies.begin(), replies.begin() + i);
        if (replies.empty()) {
            _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
            is_Alive = false;
        }
        std::atomic_thread_fence(std::memory_order_release);
    } catch (std::runtime_error &ex) {
        _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
        OnError();
        std::atomic_thread_fence(std::memory_order_release);
    }
}

} // namespace MTnonblock
} // namespace Network
} // namespace Afina
