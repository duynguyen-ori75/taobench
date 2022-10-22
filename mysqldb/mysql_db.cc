#include "mysql_db.h"

namespace {
const std::string DATABASE_NAME         = "mysqldb.dbname";
const std::string DATABASE_URL          = "mysqldb.url";
const std::string DATABASE_USERNAME     = "mysqldb.username";
const std::string DATABASE_PASSWORD     = "mysqldb.password";
const std::string DATABASE_PORT         = "mysqldb.dbport";
const std::string DATABASE_PORT_DEFAULT = "4000";
}  // namespace

namespace sql = SuperiorMySqlpp;

namespace benchmark {

inline std::string ReadObjectSQL(const DB::DB_Operation &op) {
  auto &key = op.key;
  auto id   = key[0].value;
  std::ostringstream stmt;
  stmt << "SELECT timestamp, value FROM objects WHERE id=" << id;
  return stmt.str();
}

inline std::string ReadEdgeSQL(const DB::DB_Operation &op) {
  auto &key = op.key;
  auto id1  = key[0].value;
  auto id2  = key[1].value;
  auto type = key[2].value;
  std::ostringstream stmt;
  stmt << "SELECT timestamp, value FROM edges WHERE id1=" << id1 << " AND id2=" << id2
       << " AND type=" << type;
  return stmt.str();
}

inline std::string InsertObjectSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id        = key[0].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "INSERT INTO objects (id, timestamp, value) VALUES (" << id << ", " << timestamp
       << ", '" << val << "')";
  return stmt.str();
}

inline std::string InsertOtherSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "INSERT INTO edges (id1, id2, type, timestamp, value) SELECT " << id1 << ", "
       << id2 << ", " << type << ", " << timestamp << ", '" << val
       << "' WHERE NOT EXISTS  (SELECT 1 FROM edges WHERE id1=" << id1
       << " AND type=0 OR id1=" << id1 << " AND type=2 OR id1=" << id1
       << " AND id2=" << id2 << " AND type=1 OR id1=" << id2 << " AND id2=" << id1 << ")";
  return stmt.str();
}

inline std::string InsertUniqueSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "INSERT INTO edges (id1, id2, type, timestamp, value) SELECT " << id1 << ", "
       << id2 << ", " << type << ", " << timestamp << ", '" << val
       << "' WHERE NOT EXISTS (SELECT 1 FROM edges WHERE id1=" << id1 << " OR id1=" << id2
       << " AND id2=" << id1 << ")";
  return stmt.str();
}

inline std::string InsertBidrectionalSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "INSERT INTO edges (id1, id2, type, timestamp, value) SELECT " << id1 << ", "
       << id2 << ", " << type << ", " << timestamp << ", '" << val
       << "' WHERE NOT EXISTS (SELECT 1 FROM edges WHERE id1=" << id1
       << " AND type=0 OR id1=" << id1 << " AND type=2 OR id1=" << id1
       << " AND id2=" << id2 << " AND type=3 OR id1=" << id2 << " AND id2=" << id1
       << " AND type=3 OR id1=" << id1 << " AND id2=" << id2 << " AND type=0)";
  return stmt.str();
}

inline std::string InsertUniqueAndBidirectionalSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "INSERT INTO edges (id1, id2, type, timestamp, value) SELECT " << id1 << ", "
       << id2 << ", " << type << ", " << timestamp << ", '" << val
       << "' WHERE NOT EXISTS (SELECT 1 FROM edges WHERE id1=" << id1 << " OR id1=" << id2
       << " AND id2=" << id1 << " AND type=3 OR id1=" << id2 << " AND id2=" << id1
       << " AND type=0)";
  return stmt.str();
}

inline std::string DeleteObjectSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id        = key[0].value;
  auto timestamp = op.time_and_value.timestamp;
  std::ostringstream stmt;
  stmt << "DELETE FROM objects where timestamp < " << timestamp << " AND id=" << id;
  return stmt.str();
}

inline std::string DeleteEdgeSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  std::ostringstream stmt;
  stmt << "DELETE FROM edges where timestamp<" << timestamp << " AND id1=" << id1
       << " AND id2=" << id2 << " AND type=" << type;
  return stmt.str();
}

inline std::string UpdateObjectSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id        = std::to_string(key[0].value);
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "UPDATE objects SET timestamp=" << timestamp << ", value='" << val
       << "' WHERE timestamp<" << timestamp << " AND id=" << id;
  return stmt.str();
}

inline std::string UpdateEdgeSQL(const DB::DB_Operation &op) {
  auto &key      = op.key;
  auto id1       = key[0].value;
  auto id2       = key[1].value;
  auto type      = key[2].value;
  auto timestamp = op.time_and_value.timestamp;
  auto val       = op.time_and_value.value;
  std::ostringstream stmt;
  stmt << "UPDATE edges SET timestamp=" << timestamp << ", value='" << val
       << "' WHERE timestamp<" << timestamp << " AND id1=" << id1 << " AND id2=" << id2
       << " AND type=" << type;
  return stmt.str();
}

inline PreparedStatement BuildReadObject(sql::Connection &conn) {
  std::string object_string = "SELECT timestamp, value FROM objects WHERE id=?";
  return conn.makeDynamicPreparedStatement(object_string);
}

inline PreparedStatement BuildReadEdge(sql::Connection &conn) {
  std::string edge_string =
    "SELECT timestamp, value FROM edges WHERE id1=? AND id2=? AND type=?";
  return conn.makeDynamicPreparedStatement(edge_string);
}

inline PreparedStatement BuildInsertObject(sql::Connection &conn) {
  std::string object_string =
    "INSERT INTO objects (id, timestamp, value) VALUES "
    "(?, ?, ?)";
  return conn.makeDynamicPreparedStatement(object_string);
}

inline PreparedStatement BuildInsertOther(sql::Connection &conn) {
  std::string edge_base =
    "INSERT INTO edges (id1, id2, type, timestamp, value) "
    "SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS ";
  std::string other_filter =
    "(SELECT 1 FROM edges WHERE id1=? AND type=0 OR "
    "id1=? AND type=2 OR id1=? AND id2=? and "
    "type=1 OR id1=? AND id2=?)";
  return conn.makeDynamicPreparedStatement(edge_base + other_filter);
}

inline PreparedStatement BuildInsertUnique(sql::Connection &conn) {
  std::string edge_base =
    "INSERT INTO edges (id1, id2, type, timestamp, value) "
    "SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS ";
  std::string unique_filter = "(SELECT 1 FROM edges WHERE id1=? OR id1=? AND id2=?)";
  return conn.makeDynamicPreparedStatement(edge_base + unique_filter);
}

inline PreparedStatement BuildInsertBidirectional(sql::Connection &conn) {
  std::string edge_base =
    "INSERT INTO edges (id1, id2, type, timestamp, value) "
    "SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS ";
  std::string bidirectional_filter =
    "(SELECT 1 FROM edges WHERE id1=? AND type=0 OR "
    "id1=? AND type=2 OR id1=? AND id2=? AND type=3 "
    "OR id1=? AND id2=? AND type=3 OR id1=? AND id2=? AND "
    "type=0)";
  return conn.makeDynamicPreparedStatement(edge_base + bidirectional_filter);
}

inline PreparedStatement BuildInsertUniqueAndBidirectional(sql::Connection &conn) {
  std::string edge_base =
    "INSERT INTO edges (id1, id2, type, timestamp, value) "
    "SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS ";
  std::string unique_bi_filter =
    "(SELECT 1 FROM edges WHERE id1=? OR id1=? AND id2=? "
    "AND type=3 OR id1=? AND id2=? AND type=0)";
  return conn.makeDynamicPreparedStatement(edge_base + unique_bi_filter);
}

inline PreparedStatement BuildDeleteObject(sql::Connection &conn) {
  std::string object_string =
    "DELETE FROM objects where timestamp<? "
    "AND id=?";
  return conn.makeDynamicPreparedStatement(object_string.c_str());
}

inline PreparedStatement BuildDeleteEdge(sql::Connection &conn) {
  std::string edge_string =
    "DELETE FROM edges where timestamp<? "
    "AND id1=? AND id2=? AND type=?";
  return conn.makeDynamicPreparedStatement(edge_string.c_str());
}

inline PreparedStatement BuildUpdateObject(sql::Connection &conn) {
  std::string object_string =
    "UPDATE objects SET timestamp=?, value=? WHERE "
    "timestamp<? AND id=?";
  return conn.makeDynamicPreparedStatement(object_string.c_str());
}

inline PreparedStatement BuildUpdateEdge(sql::Connection &conn) {
  std::string edge_string =
    "UPDATE edges SET timestamp=?, value=? WHERE "
    "timestamp<? AND id1=? AND id2=? AND type=?";
  return conn.makeDynamicPreparedStatement(edge_string.c_str());
}

MySqlDB::PreparedStatements::PreparedStatements(utils::Properties const &props)
    : sql_connection_{props.GetProperty(DATABASE_NAME),
                      props.GetProperty(DATABASE_USERNAME),
                      props.GetProperty(DATABASE_PASSWORD),
                      props.GetProperty(DATABASE_URL),
                      static_cast<uint16_t>(std::stoi(props.GetProperty(DATABASE_PORT)))},
      read_object(BuildReadObject(sql_connection_)),
      read_edge(BuildReadEdge(sql_connection_)),
      insert_object(BuildInsertObject(sql_connection_)),
      insert_other(BuildInsertOther(sql_connection_)),
      insert_unique(BuildInsertUnique(sql_connection_)),
      insert_bidirectional(BuildInsertBidirectional(sql_connection_)),
      insert_unique_and_bidirectional(BuildInsertUniqueAndBidirectional(sql_connection_)),
      delete_object(BuildDeleteObject(sql_connection_)),
      delete_edge(BuildDeleteEdge(sql_connection_)),
      update_object(BuildUpdateObject(sql_connection_)),
      update_edge(BuildUpdateEdge(sql_connection_)) {}

void MySqlDB::Init() {
  const utils::Properties &props = *props_;
  statements                     = new PreparedStatements{props};
}

void MySqlDB::Cleanup() { delete statements; }

Status MySqlDB::Read(DataTable table, const std::vector<DB::Field> &key,
                     std::vector<TimestampValue> &buffer) {
  bool row_found = false;
  sql::Nullable<sql::StringDataBase<4100>> s;
  sql::Nullable<int64_t> timestamp;
  if (table == DataTable::Edges) {
    auto &statement = statements->read_edge;
    assert(key.size() == 3);
    assert(key[0].name == "id1");
    assert(key[1].name == "id2");
    assert(key[2].name == "type");
    int64_t id1  = key[0].value;
    int64_t id2  = key[1].value;
    int64_t type = key[2].value;
    statement.bindParam(0, id1);
    statement.bindParam(1, id2);
    statement.bindParam(2, type);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
    statement.bindResult(0, timestamp);
    statement.bindResult(1, s);
    statement.updateResultBindings();
    statement.fetch();
  } else {
    auto &statement = statements->read_object;
    assert(key.size() == 1);
    assert(key[0].name == "id");
    int64_t id = key[0].value;
    statement.bindParam(0, id);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
    statement.bindResult(0, timestamp);
    statement.bindResult(1, s);
    statement.updateResultBindings();
    statement.fetch();
  }
  if (!timestamp.isValid() || !s.isValid()) {
    std::cerr << "Key not found" << std::endl;
    return Status::kNotFound;
  }
  buffer.emplace_back(TimestampValue(timestamp.value(), s->getString()));
  return Status::kOK;
}

Status MySqlDB::Scan(DataTable table, const std::vector<Field> &key, int n,
                     std::vector<TimestampValue> &buffer) {
  return Status::kNotImplemented;
}

Status MySqlDB::Update(DataTable table, const std::vector<Field> &key,
                       TimestampValue const &value) {
  int64_t timestamp = value.timestamp;
  const char *val   = value.value.c_str();

  if (table == DataTable::Edges) {
    assert(key.size() == 3);
    assert(key[0].name == "id1");
    assert(key[1].name == "id2");
    assert(key[2].name == "type");
    auto &statement = statements->update_edge;
    int64_t id1     = key[0].value;
    int64_t id2     = key[1].value;
    int64_t type    = key[2].value;
    statement.bindParam(0, timestamp);
    statement.bindParam(1, val);
    statement.bindParam(2, timestamp);
    statement.bindParam(3, id1);
    statement.bindParam(4, id2);
    statement.bindParam(5, type);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
  } else {
    assert(key.size() == 1);
    assert(key[0].name == "id");
    auto &statement = statements->update_object;
    int64_t id      = key[0].value;
    statement.bindParam(0, timestamp);
    statement.bindParam(1, val);
    statement.bindParam(2, timestamp);
    statement.bindParam(3, id);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
  }
  return Status::kOK;
}

Status MySqlDB::Insert(DataTable table, const std::vector<Field> &key,
                       TimestampValue const &value) {
  int64_t timestamp = value.timestamp;
  const char *val   = value.value.c_str();

  if (table == DataTable::Objects) {
    assert(key.size() == 1);
    assert(key[0].name == "id");
    int64_t id      = key[0].value;
    auto &statement = statements->insert_object;
    statement.bindParam(0, id);
    statement.bindParam(1, timestamp);
    statement.bindParam(2, val);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
  } else {
    assert(key.size() == 3);
    assert(key[0].name == "id1");
    assert(key[1].name == "id2");
    assert(key[2].name == "type");
    int64_t id1  = key[0].value;
    int64_t id2  = key[1].value;
    int64_t type = key[2].value;
    auto t       = static_cast<EdgeType>(type);
    if (t == EdgeType::Other) {
      auto &statement = statements->insert_other;
      statement.bindParam(0, id1);
      statement.bindParam(1, id2);
      statement.bindParam(2, type);
      statement.bindParam(3, timestamp);
      statement.bindParam(4, val);
      statement.bindParam(5, id1);
      statement.bindParam(6, id1);
      statement.bindParam(7, id1);
      statement.bindParam(8, id2);
      statement.bindParam(9, id2);
      statement.bindParam(10, id1);
      statement.updateParamBindings();
      try {
        statement.execute();
      } catch (sql::MysqlInternalError e) {
        std::cerr << e.getMysqlError() << std::endl;
        return Status::kError;
      }
    } else if (t == EdgeType::Unique) {
      auto &statement = statements->insert_unique;
      statement.bindParam(0, id1);
      statement.bindParam(1, id2);
      statement.bindParam(2, type);
      statement.bindParam(3, timestamp);
      statement.bindParam(4, val);
      statement.bindParam(5, id1);
      statement.bindParam(6, id2);
      statement.bindParam(7, id1);
      statement.updateParamBindings();
      try {
        statement.execute();
      } catch (sql::MysqlInternalError e) {
        std::cerr << e.getMysqlError() << std::endl;
        return Status::kError;
      }
    } else if (t == EdgeType::Bidirectional) {
      auto &statement = statements->insert_bidirectional;
      statement.bindParam(0, id1);
      statement.bindParam(1, id2);
      statement.bindParam(2, type);
      statement.bindParam(3, timestamp);
      statement.bindParam(4, val);
      statement.bindParam(5, id1);
      statement.bindParam(6, id1);
      statement.bindParam(7, id1);
      statement.bindParam(8, id2);
      statement.bindParam(9, id2);
      statement.bindParam(10, id1);
      statement.bindParam(11, id1);
      statement.bindParam(12, id2);
      statement.updateParamBindings();
      try {
        statement.execute();
      } catch (sql::MysqlInternalError e) {
        std::cerr << e.getMysqlError() << std::endl;
        return Status::kError;
      }
    } else if (t == EdgeType::UniqueAndBidirectional) {
      auto &statement = statements->insert_unique_and_bidirectional;
      statement.bindParam(0, id1);
      statement.bindParam(1, id2);
      statement.bindParam(2, type);
      statement.bindParam(3, timestamp);
      statement.bindParam(4, val);
      statement.bindParam(5, id1);
      statement.bindParam(6, id2);
      statement.bindParam(7, id1);
      statement.bindParam(8, id2);
      statement.bindParam(9, id1);
      statement.updateParamBindings();
      try {
        statement.execute();
      } catch (sql::MysqlInternalError e) {
        std::cerr << e.getMysqlError() << std::endl;
        return Status::kError;
      }
    } else {
      throw std::invalid_argument("Invalid edge type!");
    }
  }
  return Status::kOK;
}

Status MySqlDB::BatchInsert(DataTable table, const std::vector<std::vector<Field>> &keys,
                            std::vector<TimestampValue> const &values) {
  std::lock_guard<std::mutex> lock(mutex_);
  return table == DataTable::Edges ? BatchInsertEdges(keys, values)
                                   : BatchInsertObjects(keys, values);
}

Status MySqlDB::BatchRead(DataTable table, const std::vector<Field> &floor_key,
                          const std::vector<Field> &ceiling_key, int n,
                          std::vector<std::vector<Field>> &key_buffer) {
  std::lock_guard<std::mutex> lock(mutex_);
  assert(table == DataTable::Edges);
  assert(floor_key.size() == 3);
  assert(floor_key[0].name == "id1");
  assert(floor_key[1].name == "id2");
  assert(floor_key[2].name == "type");
  assert(ceiling_key.size() == 3);
  assert(ceiling_key[0].name == "id1");
  assert(ceiling_key[1].name == "id2");
  assert(ceiling_key[2].name == "type");
  std::ostringstream query_string;
  query_string << "SELECT id1, id2, type FROM edges WHERE "
               << "(id1, id2, type) > ('" << floor_key[0].value << "','"
               << floor_key[1].value << "','" << floor_key[2].value << "') AND "
               << "(id1, id2, type) < ('" << ceiling_key[0].value << "','"
               << ceiling_key[1].value << "','" << ceiling_key[2].value
               << "') ORDER BY id1, id2, type "
               << "LIMIT " << n;
  auto query = statements->sql_connection_.makeQuery(query_string.str().c_str());
  try {
    query.execute();
  } catch (sql::MysqlInternalError e) {
    std::cerr << e.getMysqlError() << std::endl;
    return Status::kError;
  }
  auto result = query.store();
  while (auto row = result.fetchRow()) {
    std::vector<DB::Field> row_result;
    row_result.emplace_back("id1", std::stoll(row[0].getString()));
    row_result.emplace_back("id2", std::stoll(row[1].getString()));
    row_result.emplace_back("type", std::stoll(row[2].getString()));
    key_buffer.push_back(std::move(row_result));
  }
  return Status::kOK;
}

Status MySqlDB::BatchInsertObjects(const std::vector<std::vector<Field>> &keys,
                                   const std::vector<TimestampValue> &timeval) {
  assert(!keys.empty());
  std::ostringstream query_string;
  query_string << "INSERT INTO objects (id, timestamp, value) VALUES ";
  bool is_first = true;
  for (size_t i = 0; i < keys.size(); ++i) {
    assert(keys[i].size() == 1);
    assert(keys[i][0].name == "id");
    if (!is_first) {
      query_string << ", ";
    } else {
      is_first = false;
    }
    query_string << "('" << keys[i][0].value << "', " << timeval[i].timestamp << ", '"
                 << timeval[i].value << "')";
  }
  try {
    statements->sql_connection_.makeQuery(query_string.str().c_str()).execute();
  } catch (sql::MysqlInternalError e) {
    std::cerr << "Batch insert failed: " << e.getMysqlError() << std::endl;
    return Status::kError;
  }
  return Status::kOK;
}

Status MySqlDB::BatchInsertEdges(const std::vector<std::vector<Field>> &keys,
                                 const std::vector<TimestampValue> &timeval) {
  assert(!keys.empty());
  std::ostringstream query_string;
  query_string << "INSERT INTO edges (id1, id2, type, timestamp, value) VALUES ";
  bool is_first = true;
  for (size_t i = 0; i < keys.size(); ++i) {
    assert(keys[i].size() == 3);
    assert(keys[i][0].name == "id1");
    assert(keys[i][1].name == "id2");
    assert(keys[i][2].name == "type");
    if (!is_first) {
      query_string << ", ";
    } else {
      is_first = false;
    }
    query_string << "(" << keys[i][0].value << ", " << keys[i][1].value << ", "
                 << keys[i][2].value << ", " << timeval[i].timestamp << ", '"
                 << timeval[i].value << "')";
  }
  try {
    statements->sql_connection_.makeQuery(query_string.str().c_str()).execute();
  } catch (sql::MysqlInternalError) {
    std::cerr << "Batch insert failed" << std::endl;
    return Status::kError;
  }
  return Status::kOK;
}

Status MySqlDB::Delete(DataTable table, const std::vector<Field> &key,
                       TimestampValue const &value) {
  int64_t timestamp = value.timestamp;
  if (table == DataTable::Edges) {
    assert(key.size() == 3);
    assert(key[0].name == "id1");
    assert(key[1].name == "id2");
    assert(key[2].name == "type");
    auto &statement  = statements->delete_edge;
    int64_t id1      = key[0].value;
    int64_t id2      = key[1].value;
    std::string type = EdgeTypeToString(static_cast<EdgeType>(key[2].value));
    statement.bindParam(0, timestamp);
    statement.bindParam(1, id1);
    statement.bindParam(2, id2);
    statement.bindParam(3, type);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << e.getMysqlError() << std::endl;
      return Status::kError;
    }
  } else {
    assert(key.size() == 1);
    assert(key[0].name == "id");
    auto &statement = statements->delete_object;
    int64_t id      = key[0].value;

    statement.bindParam(0, timestamp);
    statement.bindParam(1, id);
    statement.updateParamBindings();
    try {
      statement.execute();
    } catch (sql::MysqlInternalError) { return Status::kError; }
  }
  return Status::kOK;
}

Status MySqlDB::Execute(const DB_Operation &operation,
                        std::vector<TimestampValue> &read_buffer, bool txn_op) {
  if (!txn_op) { std::lock_guard<std::mutex> lock(mutex_); }
  switch (operation.operation) {
    case Operation::READ: {
      if (Read(operation.table, operation.key, read_buffer) != Status::kOK) {
        std::cerr << "read failed" << std::endl;
        return Status::kError;
      }
      break;
    }
    case Operation::DELETE:
      if (Delete(operation.table, operation.key, operation.time_and_value) !=
          Status::kOK) {
        std::cerr << "delete failed" << std::endl;
        return Status::kError;
      }
      break;
    case Operation::UPDATE:
      if (Update(operation.table, operation.key, operation.time_and_value) !=
          Status::kOK) {
        std::cerr << "update failed" << std::endl;
        return Status::kError;
      }
      break;
    case Operation::INSERT:
      if (Insert(operation.table, operation.key, operation.time_and_value) !=
          Status::kOK) {
        std::cerr << "insert failed" << std::endl;
        return Status::kError;
      }
      break;
    default:
      std::cerr << "invalid operation" << std::endl;
      return Status::kNotImplemented;
  }
  return Status::kOK;
}

Status MySqlDB::ExecuteTransaction(const std::vector<DB_Operation> &operations,
                                   std::vector<TimestampValue> &read_buffer,
                                   bool read_only) {
  auto query = statements->sql_connection_.makeQuery("START TRANSACTION; ");
  for (auto const &op : operations) {
    switch (op.operation) {
      case Operation::READ:
        if (op.table == DataTable::Edges) {
          query << ReadEdgeSQL(op);
        } else {
          query << ReadObjectSQL(op);
        }
        break;
      case Operation::DELETE:
        if (op.table == DataTable::Edges) {
          query << DeleteEdgeSQL(op);
        } else {
          query << DeleteObjectSQL(op);
        }
        break;
      case Operation::UPDATE:
        if (op.table == DataTable::Edges) {
          query << UpdateEdgeSQL(op);
        } else {
          query << UpdateObjectSQL(op);
        }
        break;
      case Operation::INSERT:
        if (op.table == DataTable::Objects) {
          query << InsertObjectSQL(op);
        } else {
          auto type = static_cast<EdgeType>(op.key[2].value);
          if (type == EdgeType::Other) {
            query << InsertOtherSQL(op);
          } else if (type == EdgeType::Unique) {
            query << InsertUniqueSQL(op);
          } else if (type == EdgeType::Bidirectional) {
            query << InsertBidrectionalSQL(op);
          } else if (type == EdgeType::UniqueAndBidirectional) {
            query << InsertUniqueAndBidirectionalSQL(op);
          }
        }
        break;
      default:
        std::cerr << "invalid operation" << std::endl;
        return Status::kNotImplemented;
    }
    query << "; ";
  }
  query << "COMMIT";

  try {
    query.execute();
    while (query.nextResult()) {
      try {
        auto result = query.store();
        while (auto row = result.fetchRow()) {
          assert(row.size() == 2);
          int64_t timestamp = row[0];
          std::string s     = row[1].getString();
          read_buffer.emplace_back(TimestampValue(timestamp, s));
        }
      } catch (sql::LogicError) {
        // ignore error from reading non-SELECT query
      }
    }
  } catch (sql::MysqlInternalError e) {
    std::cerr << "transaction failed: " << e.getMysqlError() << std::endl;
    try {
      statements->sql_connection_.makeQuery("ROLLBACK").execute();
    } catch (sql::MysqlInternalError e) {
      std::cerr << "failed to rollback: " << e.getMysqlError() << std::endl;
    }
    if (e.getErrorCode() == 1213) { return Status::kContentionError; }
    return Status::kError;
  }
  return Status::kOK;
}

DB *NewMySqlDB() { return new MySqlDB; }

const bool registered = DBFactory::RegisterDB("mysql", NewMySqlDB);

}  // namespace benchmark
