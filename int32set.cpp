#include <cstring>
#include <fcntl.h>
#include <future>
#include <iomanip>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <vector>

using namespace std;
using namespace std::chrono;

constexpr uint64_t DATA_SIZE = uint64_t(UINT32_MAX) + 1;

template<typename F, typename... Args>
pair<size_t, int64_t> time_func_ms(F func, Args&&... args) {
    auto start = steady_clock::now();
    auto result = func(std::forward<Args>(args)...);
    return pair(result, duration_cast<milliseconds>(steady_clock::now() - start).count());
}

template<typename F, typename... Args>
void io_stats(F func, Args&&... args) {
    auto result = time_func_ms(func, std::forward<Args>(args)...);
    auto mb = result.first * 0.000001;
    auto seconds = result.second * 0.001;
    cout << fixed << setprecision(3) << mb << " MB in " << seconds << " seconds, " << (mb / seconds) << " MB/s" << endl;
} 

uint64_t read_file(const char* filename, uint8_t* data, uint8_t pattern, off_t limit = 0, size_t num_readers = 0) {
    auto fd = open(filename, O_RDONLY);
    if(fd < 0) {
        cerr << "error opening file for reading " << filename << endl;
        exit(1);
    }
    struct stat file_stat;
    if(fstat(fd, &file_stat) < 0) {
        cerr << "error getting file stats for " << filename << endl;
        exit(1);
    }
    auto file_size = limit ? min(file_stat.st_size, limit) : file_stat.st_size;
    if(file_stat.st_size % sizeof(uint32_t) != 0) {
        cerr << "error file is not int32 aligned " << filename << endl;
        exit(1);
    }
    auto mapped_ptr = mmap(0, file_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(mapped_ptr == MAP_FAILED) {
        cerr << "mmap failed " << filename << endl;
        exit(1);
    }
    if(num_readers == 0) num_readers = thread::hardware_concurrency();
    auto mapped_file = (const uint32_t*)mapped_ptr;
    auto elements = file_size / sizeof(uint32_t);
    auto elements_per_reader = elements / num_readers;
    uint64_t total = 0;
    vector<future<uint64_t>> readers;
    cout << "reading memory map " << filename << ", file_size=" << file_size << " num_readers=" << num_readers << " elements_per_reader=" << elements_per_reader << endl << flush;
    for(auto reader = 0; reader < num_readers; ++reader) {
        auto reader_offset = reader * elements_per_reader + (reader > 0 ? elements % num_readers : 0);
        auto reader_elements = elements_per_reader + (reader == 0 ? elements % num_readers : 0);
        readers.push_back(async([reader, reader_offset, reader_elements, &pattern, &data, &mapped_file]() -> uint64_t {
            uint64_t result = 0;
            auto ptr = mapped_file + reader_offset;
            auto end = ptr + reader_elements;
            //madvise((void*)ptr, reader_elements * sizeof(uint32_t), MADV_SEQUENTIAL | MADV_WILLNEED);
            while(ptr != end) {
                __sync_fetch_and_or(data + *ptr, pattern);
                ptr++;
                result++;
            }
            return result;
        }));
    }
    for(auto &it : readers) {
        it.wait();
        total += it.get() * sizeof(uint32_t);
    }
    cout << "done " << total << " bytes read" << endl << flush;
    return total;
}

size_t write_set(const char* filename, const uint8_t* data, const uint8_t value) {
    auto fp = fopen(filename, "wb");
    if(!fp) {
        cerr << "error opening file for writing " << filename << endl;
        exit(1);
    }
    cout << "writing " << filename << endl;
    size_t total = 0;
    size_t matches = 0;
    auto ptr = data;
    auto end = data + DATA_SIZE;
    uint32_t i = 0;
    while(ptr != end) {
        if(*ptr == value) {
            total += fwrite(&i, 1, sizeof(uint32_t), fp);
            matches++;
        }
        ptr++;
        i++;
    }
    cout << "done " << total << " bytes written" << endl;
    cout << "found " << matches << " distinct values" << endl;
    fclose(fp);
    return total;
}

int main(int argc, char** argv) {
    auto start = steady_clock::now();
    off_t limit = argc > 1 ? atoi(argv[1]) * 1024 * 1024 * 1024 : 0;
    auto readers = argc > 2 ? atoi(argv[2]) : 0;
    uint8_t* data = new uint8_t[DATA_SIZE];
    if(!data) {
        cerr << "error allocating data" << endl;
        exit(1);
    }
    memset(data, 0, DATA_SIZE);
    auto a = async([&data, limit, readers]() { io_stats(read_file, "1.bin", data, 0x0f, limit, readers); });
    auto b = async([&data, limit, readers]() { io_stats(read_file, "2.bin", data, 0xf0, limit, readers); });
    a.wait();
    b.wait();
    io_stats(write_set, "set.bin", data, 0xff);
    delete[] data;
    cout << "total time " << (duration_cast<milliseconds>(steady_clock::now() - start).count() * 0.001) << " seconds" << endl;
    return 0;
}
