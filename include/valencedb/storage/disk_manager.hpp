#pragma once

#include "valencedb/storage/page_codec.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>

namespace valencedb {

enum class OpenMode { CreateNew, OpenExisting };

struct IoCounters {
    uint64_t database_reads = 0;
    uint64_t database_writes = 0;
    uint64_t database_allocations = 0;
    uint64_t temporary_reads = 0;
    uint64_t temporary_writes = 0;
    uint64_t temporary_allocations = 0;
};

class DiskManager {
public:
    DiskManager(std::filesystem::path database_path, OpenMode database_mode,
                std::filesystem::path temporary_path, OpenMode temporary_mode);
    ~DiskManager() noexcept = default;
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    PageId Allocate(FileKind kind);
    void Read(PageRef page, PageBytes& out); // On failure, out may be partially overwritten.
    void Write(PageRef page, const PageBytes& data);
    uint32_t PageCount(FileKind kind) const;
    void Flush(FileKind kind);
    void FlushAll();
    void Close();
    void ResetTemporary();

    const IoCounters& counters() const noexcept { return counters_; }

private:
    struct File {
        std::filesystem::path path;
        std::fstream stream;
        uint32_t page_count = 0;
    };
    File database_;
    File temporary_;
    IoCounters counters_;
    bool closed_ = false;

    static void OpenFile(File& file, OpenMode mode);
    File& Select(FileKind kind);
    const File& Select(FileKind kind) const;
    static std::streamoff Offset(PageId page_id);
    void CheckPage(PageRef page) const;
};

}  // namespace valencedb
