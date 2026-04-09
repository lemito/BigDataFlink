#include "csv_producer.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

namespace kafka_sample {

CsvProducerComponent::CsvProducerComponent(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : userver::components::ComponentBase(config, context),
      producer_(context
                    .FindComponent<userver::kafka::ProducerComponent>(
                        "kafka-producer")
                    .GetProducer()),
      csv_dir_(config["csv-dir"].As<std::string>()),
      topic_(config["topic"].As<std::string>()),
      batch_size_(config["batch-size"].As<std::size_t>(100)) {
  task_ = userver::utils::Async("csv_processing_task",
                                [this] { ProcessAllFiles(); });
}

std::string LoadAndSanitizeCsv(const std::string& file_path) {
  std::ifstream file(file_path);
  std::ostringstream buf;
  bool inside_quotes = false;
  char c;
  while (file.get(c)) {
    if (c == '"') inside_quotes = !inside_quotes;
    if (inside_quotes && (c == '\n' || c == '\r')) {
      buf << ' ';
    } else {
      buf << c;
    }
  }
  return buf.str();
}

void CsvProducerComponent::ProcessAllFiles() {
  if (!std::filesystem::exists(csv_dir_)) {
    LOG_ERROR() << "Directory not found: " << csv_dir_;
    return;
  }

  for (const auto& entry : std::filesystem::directory_iterator(csv_dir_)) {
    if (entry.path().extension() == ".csv") {
      ProcessFile(entry.path().string());
    }
  }
}

void CsvProducerComponent::ProcessFile(const std::string& file_path) {
  LOG_INFO() << "Processing file: " << file_path;

  try {
    auto csv_content = LoadAndSanitizeCsv(file_path);
    std::istringstream ss(csv_content);
    rapidcsv::Document doc(ss, rapidcsv::LabelParams(0, -1));

    const auto column_names = doc.GetColumnNames();
    const std::size_t row_count = doc.GetRowCount();

    for (std::size_t i = 0; i < row_count; ++i) {
      // Проверка на остановку сервиса
      if (userver::engine::current_task::ShouldCancel()) {
        LOG_WARNING() << "Task cancelled, stopping file: " << file_path;
        return;
      }

      userver::formats::json::ValueBuilder builder{
          userver::formats::json::Type::kObject};

      for (const auto& col : column_names) {
        builder[col] = doc.GetCell<std::string>(col, i);
      }

      const std::string key = fmt::format("{}-{}", file_path, i);
      const auto msg = userver::formats::json::ToString(builder.ExtractValue());
      producer_.Send(topic_, key, std::move(msg));

      LOG_INFO() << msg << " sent to topic " << topic_;

      if (i > 0 && i % batch_size_ == 0) {
        userver::engine::Yield();
      }
    }

    LOG_INFO() << "Successfully finished " << file_path << " (" << row_count
               << " rows)";

  } catch (const std::exception& ex) {
    LOG_ERROR() << "Error parsing CSV file " << file_path << ": " << ex.what();
  }
}

userver::yaml_config::Schema CsvProducerComponent::GetStaticConfigSchema() {
  return userver::yaml_config::MergeSchemas<userver::components::ComponentBase>(
      R"(
type: object
description: CSV to Kafka producer component
additionalProperties: false
properties:
    csv-dir:
        type: string
        description: directory with csv files
    topic:
        type: string
        description: kafka topic to send data
    batch-size:
        type: integer
        description: how often to yield CPU
        default: 100
)");
}

}  // namespace kafka_sample
