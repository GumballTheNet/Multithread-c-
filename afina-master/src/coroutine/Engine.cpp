
#include <afina/coroutine/Engine.h>

#include <cassert>
#include <setjmp.h>
#include <stdio.h>
#include <string.h>
namespace Afina {
namespace Coroutine {

void Engine::Store(context &ctx) {
    char current_address;
    char *dest;
    if (inverse) {
        dest = ctx.Low = &current_address;
    } else {
        dest = ctx.Hight = &current_address;
    }
    auto &buf = std::get<0>(ctx.Stack);
    auto &size = std::get<1>(ctx.Stack);
    auto need_size = ctx.Hight - ctx.Low;
    if (size < need_size) {
        delete[] buf;
        buf = new char[need_size];
        size = need_size;
    }

    memcpy(buf, dest, need_size);
    ctx.Stack = std::make_tuple(buf, need_size);
}

void Engine::Restore(context &ctx) {
    char current_address;
    if ((&current_address >= ctx.Low) && (&current_address < ctx.Hight)) {
        Restore(ctx);
    }

    auto &buf = std::get<0>(ctx.Stack);
    auto size = std::get<1>(ctx.Stack);
    memcpy(ctx.Low, buf, size);
    cur_routine = &ctx;

    longjmp(ctx.Environment, 1);
}

void Engine::yield() {
    if (alive == nullptr) {
        return;
    }

    auto routine_todo = alive;
    if (routine_todo == cur_routine) {
        if (alive->next != nullptr) {
            routine_todo = alive->next;
        } else {
            return;
        }
    }
    Enter(*routine_todo);
}

void Engine::sched(void *routine) {
    if (routine == cur_routine) {
        return;
    } else if (routine == nullptr) {
        yield();
    }
    Enter(*(static_cast<context *>(routine)));
}

void Engine::Enter(context &ctx) {
    assert(cur_routine != nullptr);
    if (cur_routine != idle_ctx) {
        if (setjmp(cur_routine->Environment) > 0) {
            return;
        }
        Store(*cur_routine);
    }
    cur_routine = &ctx;
    Restore(ctx);
}

} // namespace Coroutine
} // namespace Afina
