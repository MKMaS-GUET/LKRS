/*
 * @FileName   : macros.hpp 
 * @CreateAt   : 2021/11/4
 * @Author     : Inno Fang
 * @Email      : innofang@yeah.net
 * @Description: utils collections
 */

#ifndef RETRIEVE_SYSTEM_UTILS_HPP
#define RETRIEVE_SYSTEM_UTILS_HPP

// DEPRECATION !!
#define TIMEIT( CODE, RECORD ) do { \
            auto start_time = std::chrono::high_resolution_clock::now(); \
            CODE;  \
            auto stop_time = std::chrono::high_resolution_clock::now(); \
            std::chrono::duration<double, std::milli> used_time = stop_time - start_time; \
            (RECORD) = used_time.count(); \
        } while(0);

namespace inno {

/* RECOMMEND!! timing function */
template<typename Function, typename... Types>
decltype(auto) timeit(Function &&function, Types &&...args) {
        auto start_time = std::chrono::high_resolution_clock::now();
        auto ret = function(args...);
        auto stop_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = stop_time - start_time; \
    return std::make_tuple(std::move(ret), diff.count());
}

} // namespace inno

#endif //RETRIEVE_SYSTEM_UTILS_HPP
