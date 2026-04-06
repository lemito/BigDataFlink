#include <csv_producer.hpp>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/formats/json/inline.hpp>
#include <userver/kafka/exceptions.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/async.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

namespace kafka_sample {

namespace {

namespace fs = std::filesystem;

// Parse a single CSV line respecting quoted fields.
std::vector<std::string> ParseCsvLine(const std::string& line) {
  std::vector<std::string> fields;
  std::string field;
  bool in_quotes = false;

  for (std::size_t i = 0; i < line.size(); ++i) {
    char c = line[i];
    if (c == '"') {
      if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
        // Escaped double-quote inside quoted field
        field += '"';
        ++i;
      } else {
        in_quotes = !in_quotes;
      }
    } else if (c == ',' && !in_quotes) {
      fields.push_back(std::move(field));
      field.clear();
    } else {
      field += c;
    }
  }
  fields.push_back(std::move(field));
  return fields;
}

// Build a JSON string from header names and row values.
// {"col1":"val1","col2":"val2",...}
std::string MakeJsonPayload(const std::vector<std::string>& headers,
                            const std::vector<std::string>& values) {
  auto builder = formats::json::MakeObject();
  // formats::json::MakeObject returns a Value — build via ValueBuilder instead
  formats::json::ValueBuilder obj(formats::json::Type::kObject);
  for (std::size_t i = 0; i < headers.size(); ++i) {
    obj[headers[i]] = (i < values.size() ? values[i] : std::string{});
  }
  return formats::json::ToString(obj.ExtractValue());
}

std::vector<std::string> GlobCsvFiles(const std::string& dir) {
  std::vector<std::string> result;
  if (!fs::exists(dir) || !fs::is_directory(dir)) {
    LOG_WARNING() << "CSV directory does not exist or is not a directory: "
                  << dir;
    return result;
  }
  for (const auto& entry : fs::directory_iterator(dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".csv") {
      result.push_back(entry.path().string());
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace

CsvProducerComponent::CsvProducerComponent(
    const components::ComponentConfig& config,
    const components::ComponentContext& context)
    : components::ComponentBase{config, context},
      producer_{
          context.FindComponent<kafka::ProducerComponent>().GetProducer()},
      csv_dir_{config["csv-dir"].As<std::string>("/data")},
      topic_{config["topic"].As<std::string>("input-topic")},
      batch_size_{config["batch-size"].As<std::size_t>(100)} {
  // Run CSV processing in a detached async task so it does not block
  // the component tree startup.
  utils::Async("csv-producer", [this] { ProcessAllFiles(); }).Detach();
}

void CsvProducerComponent::ProcessAllFiles() {
  const auto files = GlobCsvFiles(csv_dir_);
  if (files.empty()) {
    LOG_WARNING() << "No CSV files found in " << csv_dir_;
    return;
  }

  LOG_INFO() << "Found " << files.size() << " CSV file(s) in " << csv_dir_;

  for (const auto& path : files) {
    LOG_INFO() << "Processing file: " << path;
    try {
      ProcessFile(path);
    } catch (const std::exception& ex) {
      LOG_ERROR() << "Failed to process file " << path << ": " << ex.what();
    }
  }

  LOG_INFO() << "All CSV files processed.";
}

void CsvProducerComponent::ProcessFile(const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open file: " + file_path);
  }

  // Read header row
  std::string header_line;
  if (!std::getline(file, header_line)) {
    LOG_WARNING() << "File is empty: " << file_path;
    return;
  }
  const auto headers = ParseCsvLine(header_line);

  std::vector<std::string> batch;
  batch.reserve(batch_size_);
  std::size_t total_sent = 0;

  auto flush = [&] {
    if (batch.empty()) return;

    for (const auto& payload : batch) {
      // Key is intentionally empty — use payload hash or row index
      // if ordering guarantees are needed.
      try {
        producer_.Send(topic_, /*key=*/"", payload);
      } catch (const kafka::SendException& ex) {
        if (ex.IsRetryable()) {
          LOG_WARNING() << "Retryable send error, skipping message: "
                        << ex.what();
        } else {
          LOG_ERROR() << "Non-retryable send error: " << ex.what();
        }
      }
    }
    total_sent += batch.size();
    LOG_DEBUG() << "Flushed batch of " << batch.size()
                << " messages (total sent: " << total_sent << ")";
    batch.clear();
  };

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) continue;

    const auto values = ParseCsvLine(line);
    try {
      batch.push_back(MakeJsonPayload(headers, values));
    } catch (const std::exception& ex) {
      LOG_ERROR() << "Failed to build JSON payload: " << ex.what();
      continue;
    }

    if (batch.size() >= batch_size_) {
      flush();
    }
  }

  // Flush remaining messages
  flush();

  LOG_INFO() << "File " << file_path
             << " done. Total messages sent: " << total_sent;
}

yaml_config::Schema CsvProducerComponent::GetStaticConfigSchema() {
  return yaml_config::MergeSchemas<components::ComponentBase>(R"(
type: object
description: Reads CSV files and produces rows as JSON messages to Kafka.
additionalProperties: false
properties:
    csv-dir:
        type: string
        description: Path to the directory containing *.csv files.
    topic:
        type: string
        description: Kafka topic to produce messages to.
    batch-size:
        type: integer
        description: Number of messages accumulated before flushing to Kafka.
    batch-timeout-ms:
        type: integer
        description: Unused; kept for config compatibility with Go producer.
)");
}

}  // namespace kafka_sample
