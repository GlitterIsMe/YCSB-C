//
// Created by YiwenZhang on 2023/2/15.
//

#ifndef YCSB_FASTER_DB_H
#define YCSB_FASTER_DB_H

#include "core/db.h"
#include "src/core/faster.h"
#include "src/core/utility.h"
#include "src/device/file_system_disk.h"

#include <iostream>
#include <string>
#include <mutex>

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>


    class Slice {
    public:
        // Create an empty slice.
        Slice() : data_(""), size_(0) { }

        // Create a slice that refers to d[0,n-1].
        Slice(const char* d, size_t n) : data_(d), size_(n) { }

        // Create a slice that refers to the contents of "s"
        Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

        // Create a slice that refers to s[0,strlen(s)-1]
        Slice(const char* s) : data_(s), size_(strlen(s)) { }

        // Return a pointer to the beginning of the referenced data
        const char* data() const { return data_; }

        // Return the length (in bytes) of the referenced data
        constexpr size_t size() const { return size_; }

        // Return true iff the length of the referenced data is zero
        bool empty() const { return size_ == 0; }

        // Return the ith byte in the referenced data.
        // REQUIRES: n < size()
        char operator[](size_t n) const {
            assert(n < size());
            return data_[n];
        }

        // Change this slice to refer to an empty array
        void clear() { data_ = ""; size_ = 0; }

        // Drop the first "n" bytes from this slice.
        void remove_prefix(size_t n) {
            assert(n <= size());
            data_ += n;
            size_ -= n;
        }

        // Return a string that contains the copy of the referenced data.
        std::string ToString() const { return std::string(data_, size_); }

        // Three-way comparison.  Returns value:
        //   <  0 iff "*this" <  "b",
        //   == 0 iff "*this" == "b",
        //   >  0 iff "*this" >  "b"
        int compare(const Slice& b) const;

        // Return true iff "x" is a prefix of "*this"
        bool starts_with(const Slice& x) const {
            return ((size_ >= x.size_) &&
                    (memcmp(data_, x.data_, x.size_) == 0));
        }

        FASTER::core::KeyHash GetHash() const {
            return FASTER::core::KeyHash {FASTER::core::Utility::HashBytesUint8((uint8_t*)data_, size_)};
        }

    private:
        const char* data_;
        size_t size_;

        // Intentionally copyable
    };

    inline bool operator==(const Slice& x, const Slice& y) {
        return ((x.size() == y.size()) &&
                (memcmp(x.data(), y.data(), x.size()) == 0));
    }

    inline bool operator!=(const Slice& x, const Slice& y) {
        return !(x == y);
    }

    inline int Slice::compare(const Slice& b) const {
        const int min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

class FixedSizeKey {
public:
    FixedSizeKey(std::string value)
            : key{ value }
    {}

    FixedSizeKey(const FixedSizeKey&) = default;

    inline uint32_t size() const{
        return static_cast<uint32_t>(key.size());
    }

    inline FASTER::core::KeyHash GetHash() const {
        //HashFn hash_fn;
        //return core::KeyHash{ hash_fn(key) };
        return FASTER::core::KeyHash {FASTER::core::Utility::HashBytesUint8((uint8_t*)key.c_str(), key.size())};
    }

    inline bool operator==(const FixedSizeKey& other) const {
        return key == other.key;
    }
    inline bool operator!=(const FixedSizeKey& other) const {
        return key != other.key;
    }

    std::string key;
};

using ycsbc_key_t = FixedSizeKey;
using ycsbc_value_t = std::string;


/// Class passed to store_t::Read().
class ReadContext : public FASTER::core::IAsyncContext {
public:
    typedef ycsbc_key_t key_t;
    typedef ycsbc_value_t value_t;

    ReadContext(key_t key)
            : key_{ key } {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext& other)
            : key_{ other.key_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ycsbc_key_t & key() const {
        return key_;
    }

    // For this benchmark, we don't copy out, so these are no-ops.
    inline void Get(const ycsbc_value_t & value) {
        //printf("Get value size %d\n", value.size());
        //char* tmp = new char[value.size()];
        //memcpy(tmp, value.data(), value.size());
        //value_ = new Slice(tmp, value.size());
    }
    inline void GetAtomic(const ycsbc_value_t & value) {
        printf("GetAtomic value size %d\n", value.size());
        //char* tmp = new char[value.size()];
        //memcpy(tmp, value.data(), value.size());
        //value_ = Slice(tmp, value.size());
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ycsbc_key_t key_;
    ycsbc_value_t value_;
};

class UpsertContext : public FASTER::core::IAsyncContext {
public:
    typedef ycsbc_key_t key_t;
    typedef ycsbc_value_t value_t;

    UpsertContext(ycsbc_key_t key, ycsbc_value_t input)
            : key_{ key }
            , input_{ input } {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ } {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const ycsbc_key_t & key() const {
        return key_;
    }
    inline  uint32_t value_size() const {
        return input_.size();
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(ycsbc_value_t & value) {
        value = input_;
    }
    inline bool PutAtomic(ycsbc_value_t & value) {
        //value.atomic_value_.store(input_);
        value = input_;
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ycsbc_key_t key_;
    ycsbc_value_t input_;
};

extern FASTER::core::FasterKv<ycsbc_key_t, ycsbc_value_t, FASTER::device::FileSystemDisk<FASTER::environment::QueueIoHandler, 1073741824L>>* db;

class FasterDB : public ycsbc::DB {
public:
    void Init();

    void Close();

    int Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result);

    int Scan(const std::string &table, const std::string &key,
             int len, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result);

    int Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Delete(const std::string &table, const std::string &key);

    static int live_sessions;
private:
    FASTER::core::Guid session;
    uint64_t seq;
};


#endif //YCSB_FASTER_DB_H
