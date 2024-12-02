//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// column.cpp
//
// Identification: src/catalog/column.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column.h"

#include <sstream>
#include <string>

namespace bustub {

auto Column::ToString(bool simplified) const -> std::string {
  if (simplified) {
    std::ostringstream os;
    os << column_name_ << ":" << Type::TypeIdToString(column_type_);
    return (os.str());
  }

  std::ostringstream os;

  os << "Column[" << column_name_ << ", " << Type::TypeIdToString(column_type_) << ", "
     << "Offset:" << column_offset_ << ", ";

  if (IsInlined()) {
    os << "FixedLength:" << fixed_length_;
  } else {
    os << "VarLength:" << variable_length_;
  }
  os << "]";
  return (os.str());
}

void Column::SerializeTo(char *storage) const {
  int column_name_size = column_name_.size();
  memcpy(storage, &column_name_size, sizeof(int));
  memcpy(storage + sizeof(int), column_name_.data(), column_name_.size());
  memcpy(storage + sizeof(int) + column_name_.size(), &column_type_, sizeof(TypeId));
  memcpy(storage + sizeof(int) + column_name_.size() + sizeof(TypeId), &fixed_length_, sizeof(u_int32_t));
  memcpy(storage + sizeof(int) + column_name_.size() + sizeof(TypeId) + sizeof(u_int32_t), &variable_length_,
         sizeof(u_int32_t));
  memcpy(storage + sizeof(int) + column_name_.size() + sizeof(TypeId) + sizeof(u_int32_t) + sizeof(u_int32_t),
         &column_offset_, sizeof(u_int32_t));
}

void Column::DeserializeFrom(const char *storage) {
  int column_name_size = 0;
  memcpy(&column_name_size, storage, sizeof(int));
  this->column_name_.resize(column_name_size);
  memcpy(column_name_.data(), storage + sizeof(int), column_name_size);
  memcpy(&column_type_, storage + sizeof(int) + column_name_.size(), sizeof(TypeId));
  memcpy(&fixed_length_, storage + sizeof(int) + column_name_.size() + sizeof(TypeId), sizeof(u_int32_t));
  memcpy(&variable_length_, storage + sizeof(int) + column_name_.size() + sizeof(TypeId) + sizeof(u_int32_t),
         sizeof(u_int32_t));
  memcpy(&column_offset_,
         storage + sizeof(int) + column_name_.size() + sizeof(TypeId) + sizeof(u_int32_t) + sizeof(u_int32_t),
         sizeof(u_int32_t));
}

}  // namespace bustub
