#include "wt_db.h"
#include "db_factory.h"

#include <chrono>
#include <filesystem>

namespace benchmark {

void WiredTigerDB::Init() {
  const utils::Properties &props = *props_;
  db_path_                       = props.GetProperty("wiredtiger.db_path");
  auto config_str                = props.GetProperty("wiredtiger.config");
  auto session_str               = props.GetProperty("wiredtiger.session_cfg");

  int ret = wiredtiger_open(db_path_.c_str(), NULL, config_str.c_str(), &conn);
  error_check(ret);

  ret = conn->open_session(conn, NULL, session_str.c_str(), &session);
  error_check(ret);

  // create 2 tables if not exist
  if (props.ContainsKey("initialize_db")) {
    to_clean_up_ = true;
    ret          = session->create(session, "table:objects",
                                   "key_format=r,"
                                            "value_format=Qs150,"
                                            "columns=(id,timestamp,value)");
    error_check(ret);

    ret = session->create(session, "table:edges",
                          "key_format=r,"
                          "value_format=rrbQs150,"
                          "columns=(id,id1,id2,type,timestamp,value)");
    error_check(ret);

    ret = session->create(session, "index:edges:relationship", "columns=(id1,id2,type)");
    error_check(ret);
  }
}

void WiredTigerDB::Cleanup() {
  session->close(session, NULL);
  conn->close(conn, NULL);

  if (to_clean_up_) { std::filesystem::remove(db_path_); }
}

// TODO(Duy): Implement all of the below functions
Status WiredTigerDB::Read(DataTable table, const std::vector<Field> &key,
                          std::vector<TimestampValue> &buffer) {
  return Status::kOK;
}

Status WiredTigerDB::Scan(DataTable table, const std::vector<Field> &key, int n,
                          std::vector<TimestampValue> &buffer) {
  return Status::kOK;
}

Status WiredTigerDB::Update(DataTable table, const std::vector<Field> &key,
                            TimestampValue const &value) {
  return Status::kOK;
}

Status WiredTigerDB::Insert(DataTable table, const std::vector<Field> &key,
                            TimestampValue const &value) {
  return Status::kOK;
}

Status WiredTigerDB::Delete(DataTable table, const std::vector<Field> &key,
                            TimestampValue const &value) {
  return Status::kOK;
}

Status WiredTigerDB::Execute(const DB_Operation &operation,
                             std::vector<TimestampValue> &read_buffer,  // for reads
                             bool txn_op) {
  return Status::kOK;
}

Status WiredTigerDB::ExecuteTransaction(const std::vector<DB_Operation> &operations,
                                        std::vector<TimestampValue> &read_buffer,
                                        bool read_only) {
  return Status::kOK;
}

Status WiredTigerDB::BatchInsert(DataTable table,
                                 const std::vector<std::vector<Field>> &keys,
                                 std::vector<TimestampValue> const &values) {
  return Status::kOK;
}

Status WiredTigerDB::BatchRead(DataTable table, const std::vector<Field> &floor_key,
                               const std::vector<Field> &ceiling_key, int n,
                               std::vector<std::vector<Field>> &key_buffer) {
  return Status::kOK;
}

}  // namespace benchmark