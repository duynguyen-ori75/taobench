#pragma once

#include "db.h"
#include "properties.h"

#include <wiredtiger.h>
#include <csignal>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

#define error_check(p)                                \
  if (p) {                                            \
    std::cerr << wiredtiger_strerror(p) << std::endl; \
    raise(SIGTRAP);                                   \
  }

#define WT_SUCCESS 0

namespace benchmark {

class WiredTigerDB : public DB {
 public:
  WiredTigerDB() : to_clean_up_(false) {}

  void Init();

  void Cleanup();

  Status Read(DataTable table, const std::vector<Field> &key,
              std::vector<TimestampValue> &buffer);

  Status Scan(DataTable table, const std::vector<Field> &key, int n,
              std::vector<TimestampValue> &buffer);

  Status Update(DataTable table, const std::vector<Field> &key,
                TimestampValue const &value);

  Status Insert(DataTable table, const std::vector<Field> &key,
                TimestampValue const &value);

  Status Delete(DataTable table, const std::vector<Field> &key,
                TimestampValue const &value);

  Status Execute(const DB_Operation &operation,
                 std::vector<TimestampValue> &read_buffer,  // for reads
                 bool txn_op = false);

  Status ExecuteTransaction(const std::vector<DB_Operation> &operations,
                            std::vector<TimestampValue> &read_buffer, bool read_only);

  Status BatchInsert(DataTable table, const std::vector<std::vector<Field>> &keys,
                     std::vector<TimestampValue> const &values);

  Status BatchRead(DataTable table, const std::vector<Field> &floor_key,
                   const std::vector<Field> &ceiling_key, int n,
                   std::vector<std::vector<Field>> &key_buffer);

 private:
  bool to_clean_up_;
  std::string db_path_;
  static constexpr char object_table_[] = "table:objects";
  static constexpr char edge_table_[]   = "table:edges";
  static constexpr char edge_index_[]   = "index:edges:relationship";
  WT_CONNECTION *conn;
  WT_SESSION *session;

  /**
    pqxx::result
    DoRead(pqxx::transaction_base &tx, const DataTable table,
           const std::vector<Field> &key);

  pqxx::result DoScan(pqxx::transaction_base &tx, const std::string &table,
                      const std::vector<Field> &key, int len,
                      const std::vector<std::string> *fields,
                      const std::vector<Field> &limit);

  pqxx::result DoUpdate(pqxx::transaction_base &tx, DataTable table,
                        const std::vector<Field> &key, TimestampValue const &value);

  pqxx::result DoInsert(pqxx::transaction_base &tx, DataTable table,
                        const std::vector<Field> &key, const TimestampValue &value);

  pqxx::result DoDelete(pqxx::transaction_base &tx, DataTable table,
                        const std::vector<Field> &key, const TimestampValue &value);

  Status BatchInsertObjects(DataTable table, const std::vector<std::vector<Field>> &keys,
                            const std::vector<TimestampValue> &values);

  Status BatchInsertEdges(DataTable table, const std::vector<std::vector<Field>> &keys,
                          const std::vector<TimestampValue> &values);

  Status ExecuteTransactionBatch(const std::vector<DB_Operation> &operations,
                                 std::vector<TimestampValue> &results, bool read_only);

  Status ExecuteTransactionPrepared(const std::vector<DB_Operation> &operations,
                                    std::vector<TimestampValue> &results, bool read_only);

  std::string GenerateMergedReadQuery(const std::vector<DB_Operation> &read_operations);

  std::string GenerateMergedInsertQuery(
    const std::vector<DB_Operation> &insert_operations);

  std::string GenerateMergedUpdateQuery(
    const std::vector<DB_Operation> &update_operations);

  std::string GenerateMergedDeleteQuery(
    const std::vector<DB_Operation> &delete_operations);
  */
};

DB *WiredTigerDB();

}  // namespace benchmark
