#pragma once

#include "valencedb/storage/heap_file.hpp"
#include "valencedb/index/bplus_tree.hpp"
#include "valencedb/optimizer/statistics.hpp"

#include <filesystem>
#include <memory>
#include <map>

namespace valencedb {

class Database {
public:
    Database(std::filesystem::path database_path, OpenMode mode,
             std::filesystem::path temporary_path, size_t buffer_frames);
    ~Database() noexcept = default;
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    TableInfo CreateTable(const std::string& name, const Schema& schema);
    TableInfo Table(const std::string& name) const;
    std::vector<std::string> TableNames() const;
    RID Insert(const std::string& name, const Tuple& tuple);
    std::optional<Tuple> Fetch(const std::string& name, RID rid);
    bool Delete(const std::string& name, RID rid);
    HeapScan Scan(const std::string& name, ScanRing* ring = nullptr);
    void CreateIndex(const std::string& name, size_t column);
    // Database owns one stable handle per index; it must outlive returned cursors.
    const BPlusTree& Index(const std::string& name) const;
    BPlusIterator IndexEqualRange(const std::string& name, int64_t key) const;
    BPlusIterator IndexRange(const std::string& name, int64_t low, int64_t high) const;
    void Analyze(const std::string& name);
    const TableStatistics* Statistics(const std::string& name) const;
    void Flush();
    void Close();
    const BufferMetrics& Metrics() const noexcept { return pool_.metrics(); }
    BufferPool& Buffer() noexcept { return pool_; }
    DiskManager& Disk() noexcept { return disk_; }

private:
    friend class QueryContext;
    void CheckOpen() const;
    void CheckMutable() const;
    void BeginQuery();
    void StaleStatistics(const std::string& name);
    BPlusTree& TableIndex(const TableInfo& table) const;
    DiskManager disk_;
    BufferPool pool_;
    std::unique_ptr<Catalog> catalog_;
    std::map<PageId, std::unique_ptr<BPlusTree>> indexes_;
    std::map<std::string, TableStatistics> statistics_; // Deliberately not persisted.
    uint64_t epoch_ = 0;
    bool closed_ = false;
    bool query_active_ = false;
};

}  // namespace valencedb
