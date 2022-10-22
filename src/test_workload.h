#pragma once
#include "db.h"
#include "workload.h"

namespace benchmark {
class TestWorkload : public Workload {
 public:
  TestWorkload() = default;
  void Init(DB &db) override;
  bool DoRequest(DB &db) override;
};
}  // namespace benchmark