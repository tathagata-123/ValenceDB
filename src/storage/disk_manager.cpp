#include "valencedb/storage/disk_manager.hpp"
#include "valencedb/common/error.hpp"

#include <limits>
#include <string>

namespace valencedb {
namespace {

std::string PathMessage(const std::filesystem::path& path, const char* action) {
    return std::string(action) + ": " + path.string();
}

}  // namespace

DiskManager::DiskManager(std::filesystem::path database_path, OpenMode database_mode,
                         std::filesystem::path temporary_path, OpenMode temporary_mode) {
    if (database_path == temporary_path ||
        (std::filesystem::exists(database_path) && std::filesystem::exists(temporary_path) &&
         std::filesystem::equivalent(database_path, temporary_path)))
        throw DbError("database and temporary files must be distinct");
    database_.path = std::move(database_path);
    temporary_.path = std::move(temporary_path);
    OpenFile(database_, database_mode);
    OpenFile(temporary_, temporary_mode);
}

void DiskManager::OpenFile(File& file, OpenMode mode) {
    if (mode == OpenMode::CreateNew) {
        if (std::filesystem::exists(file.path))
            throw DbError(PathMessage(file.path, "file already exists"));
        std::ofstream create(file.path, std::ios::binary | std::ios::trunc);
        if (!create) throw DbError(PathMessage(file.path, "cannot create file"));
        create.close();
        if (!create) throw DbError(PathMessage(file.path, "cannot close new file"));
    } else if (mode != OpenMode::OpenExisting || !std::filesystem::is_regular_file(file.path)) {
        throw DbError(PathMessage(file.path, "page file does not exist"));
    }
    const uintmax_t length = std::filesystem::file_size(file.path);
    if (length % kPageSize != 0 || length / kPageSize > kInvalidPageId ||
        length > uintmax_t(std::numeric_limits<std::streamoff>::max()))
        throw DbError(PathMessage(file.path, "invalid page file length"));
    file.page_count = static_cast<uint32_t>(length / kPageSize);
    file.stream.open(file.path, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.stream) throw DbError(PathMessage(file.path, "cannot open page file"));
}

DiskManager::File& DiskManager::Select(FileKind kind) {
    if (closed_) throw DbError("DiskManager is closed");
    switch (kind) {
        case FileKind::Database: return database_;
        case FileKind::Temporary: return temporary_;
    }
    throw DbError("invalid file kind");
}

const DiskManager::File& DiskManager::Select(FileKind kind) const {
    if (closed_) throw DbError("DiskManager is closed");
    switch (kind) {
        case FileKind::Database: return database_;
        case FileKind::Temporary: return temporary_;
    }
    throw DbError("invalid file kind");
}

std::streamoff DiskManager::Offset(PageId page_id) {
    const uint64_t offset = uint64_t(page_id) * kPageSize;
    if (offset > uint64_t(std::numeric_limits<std::streamoff>::max()))
        throw DbError("page offset exceeds stream range");
    return static_cast<std::streamoff>(offset);
}

void DiskManager::CheckPage(PageRef page) const {
    const File& file = Select(page.file);
    if (page.page_id == kInvalidPageId || page.page_id >= file.page_count)
        throw DbError("page ID is not allocated");
}

uint32_t DiskManager::PageCount(FileKind kind) const {
    return Select(kind).page_count;
}

PageId DiskManager::Allocate(FileKind kind) {
    File& file = Select(kind);
    if (file.page_count == kInvalidPageId) throw DbError("page ID space exhausted");
    const PageId page_id = file.page_count;
    PageBytes zero{};
    file.stream.clear();
    file.stream.seekp(Offset(page_id));
    if (!file.stream) throw DbError(PathMessage(file.path, "page allocation seek failed"));
    file.stream.write(reinterpret_cast<const char*>(zero.data()), zero.size());
    file.stream.flush();
    if (!file.stream) throw DbError(PathMessage(file.path, "page allocation write failed"));
    ++file.page_count;
    if (kind == FileKind::Database) ++counters_.database_allocations;
    else ++counters_.temporary_allocations;
    return page_id;
}

void DiskManager::Read(PageRef page, PageBytes& out) {
    CheckPage(page);
    if (fail_next_ == FailNext::Read) {
        fail_next_ = FailNext::None;
        throw DbError("injected page read failure");
    }
    File& file = Select(page.file);
    file.stream.clear();
    file.stream.seekg(Offset(page.page_id));
    if (!file.stream) throw DbError(PathMessage(file.path, "page read seek failed"));
    file.stream.read(reinterpret_cast<char*>(out.data()), out.size());
    if (file.stream.gcount() != static_cast<std::streamsize>(kPageSize) || !file.stream)
        throw DbError(PathMessage(file.path, "short page read"));
    if (page.file == FileKind::Database) ++counters_.database_reads;
    else ++counters_.temporary_reads;
}

void DiskManager::Write(PageRef page, const PageBytes& data) {
    CheckPage(page);
    if (fail_next_ == FailNext::Write) {
        fail_next_ = FailNext::None;
        throw DbError("injected page write failure");
    }
    File& file = Select(page.file);
    file.stream.clear();
    file.stream.seekp(Offset(page.page_id));
    if (!file.stream) throw DbError(PathMessage(file.path, "page write seek failed"));
    file.stream.write(reinterpret_cast<const char*>(data.data()), data.size());
    if (!file.stream) throw DbError(PathMessage(file.path, "page write failed"));
    if (page.file == FileKind::Database) ++counters_.database_writes;
    else ++counters_.temporary_writes;
}

void DiskManager::Flush(FileKind kind) {
    File& file = Select(kind);
    file.stream.clear();
    file.stream.flush();
    if (!file.stream) throw DbError(PathMessage(file.path, "page file flush failed"));
}

void DiskManager::FlushAll() {
    Flush(FileKind::Database);
    Flush(FileKind::Temporary);
}

void DiskManager::Close() {
    if (closed_) return;
    FlushAll();
    database_.stream.close();
    if (database_.stream.fail()) throw DbError("database file close failed");
    temporary_.stream.close();
    if (temporary_.stream.fail()) throw DbError("temporary file close failed");
    closed_ = true;
}

void DiskManager::ResetTemporary() {
    File& file = Select(FileKind::Temporary);
    if (fail_next_ == FailNext::ResetTemporary) {
        fail_next_ = FailNext::None;
        throw DbError("injected temporary reset failure");
    }
    Flush(FileKind::Temporary);
    file.stream.close();
    if (file.stream.fail()) throw DbError("temporary file close before reset failed");
    std::filesystem::resize_file(file.path, 0);
    OpenFile(file, OpenMode::OpenExisting);
}

}  // namespace valencedb
