//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (depth_ == 0) {
    throw std::invalid_argument("Depth must be > 0");
  }

  if (width_ == 0) {
    throw std::invalid_argument("Width must be > 0");
  }

  hash_matrix_.resize(depth);
  for (size_t i = 0; i < depth; i++) {
    hash_matrix_[i].resize(width, 0UL);  // 0 is the default value
  }

  // Initialize mutexes
  row_mutexes_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    row_mutexes_.push_back(std::make_unique<std::mutex>());
  }

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  hash_matrix_ = std::move(other.hash_matrix_);
  hash_functions_ = std::move(other.hash_functions_);
  row_mutexes_ = std::move(other.row_mutexes_);

  other.hash_functions_.clear();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    hash_matrix_ = std::move(other.hash_matrix_);
    hash_functions_ = std::move(other.hash_functions_);
    row_mutexes_ = (std::move(other.row_mutexes_));
  }

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    std::lock_guard lock(*row_mutexes_[i]);
    const size_t hash_val = hash_functions_[i](item) % width_;
    ++hash_matrix_[i][hash_val];
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      hash_matrix_[i][j] += other.hash_matrix_[i][j];
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  return estimate(item);
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      hash_matrix_[i][j] = 0UL;
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  if (k <= 0 || candidates.empty()) {
    return {};
  }

  // Create vector of (count, key) pairs
  std::vector<std::pair<uint32_t, KeyType>> countPairs;
  countPairs.reserve(candidates.size());

  for (const auto& key : candidates) {
    uint32_t count = Count(key);
    countPairs.push_back({count, key});
  }

  // Sort by count (descending)
  std::sort(countPairs.begin(), countPairs.end(),
       [](const auto& a, const auto& b) {
           return a.first > b.first;  // Higher counts first
       });

  // Extract top k (key, count) pairs
  std::vector<std::pair<KeyType, uint32_t>> result;
  size_t actualK = std::min(static_cast<size_t>(k), countPairs.size());
  result.reserve(actualK);

  for (size_t i = 0; i < actualK; i++) {
    result.push_back({countPairs[i].second, countPairs[i].first});
  }

  return result;
}

template <typename KeyType>
auto CountMinSketch<KeyType>::estimate(const KeyType& key) const -> uint32_t {
  size_t minCount = ULONG_MAX;
  for (size_t i = 0; i < depth_; i++) {
    std::lock_guard lock(*row_mutexes_[i]);
    int col = hash_functions_[i](key) % width_;
    if (hash_matrix_[i][col] < minCount) {
      minCount = hash_matrix_[i][col];
    }
  }
  return minCount;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
