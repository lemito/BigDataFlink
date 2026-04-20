#include "csv_producer.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <unordered_set>

#include <userver/components/component_config.hpp>
#include <userver/components/component_context.hpp>
#include <userver/engine/async.hpp>
#include <userver/engine/semaphore.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/fs/blocking/read.hpp>
#include <userver/kafka/headers.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

const std::unordered_set<std::string> kOffsetFields = {
    "id", "sale_customer_id", "sale_seller_id", "sale_product_id"};

namespace kafka_sample {

void CsvProducerComponent::OnAllComponentsLoaded() {
  task_ = userver::utils::Async("csv_processing_task",
                                [this] { ProcessAllFiles(); });
}

CsvProducerComponent::CsvProducerComponent(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : userver::components::ComponentBase(config, context),
      producer_(context
                    .FindComponent<userver::kafka::ProducerComponent>(
                        "kafka-producer")
                    .GetProducer()),
      fs_task_processor_(context.GetTaskProcessor("fs-task-processor")),
      csv_dir_(config["csv-dir"].As<std::string>()),
      topic_(config["topic"].As<std::string>()),
      batch_size_(config["batch-size"].As<std::size_t>(100)) {
}

void CsvProducerComponent::ProcessAllFiles() {
  try {
    auto files =
        userver::engine::AsyncNoSpan(fs_task_processor_, [this] {
          std::vector<std::string> result;
          if (!std::filesystem::exists(csv_dir_)) {
            return result;
          }
          for (const auto& entry :
               std::filesystem::directory_iterator(csv_dir_)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
              result.push_back(entry.path().string());
            }
          }
          return result;
        }).Get();

    if (files.empty()) {
      LOG_INFO() << "No CSV files found in directory: " << csv_dir_;
      return;
    }

    auto semaphore = std::make_shared<userver::engine::Semaphore>(5);

    const std::size_t total_files = files.size();
    std::vector<userver::engine::TaskWithResult<isComplete>> tasks;
    tasks.reserve(total_files);

    for (std::size_t i = 0; i < total_files; ++i) {
      tasks.push_back(userver::utils::Async(
          "process_file", [this, file_path = files[i], i, semaphore] {
            userver::engine::SemaphoreLock lock(*semaphore);
            return ProcessFile(file_path, i);
          }));
    }

    for (auto& t : tasks) {
      auto res = t.Get();
      if (res == isComplete::kCancelled) {
        LOG_WARNING() << "Processing was cancelled";
        return;
      } else if (res == isComplete::kError) {
        LOG_ERROR() << "Processing failed for one of the files";
      }
    }

    LOG_INFO() << "Finished processing all CSV files in directory: "
               << csv_dir_;
  } catch (const std::exception& e) {
    LOG_ERROR("Error processing CSV files: {}", e.what());
  }
}

CsvProducerComponent::isComplete CsvProducerComponent::ProcessFile(
    const std::string& file_path, std::size_t file_idx) {
  try {
    LOG_INFO("Processing file: {}, index: {}", file_path, file_idx);

    auto doc =
        userver::engine::AsyncNoSpan(fs_task_processor_, [file_path] {
          std::ifstream stream(file_path, std::ios::binary);
          if (!stream.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
          }
          return rapidcsv::Document(
              stream, rapidcsv::LabelParams(0, -1),
              rapidcsv::SeparatorParams(',', false, false, true));
        }).Get();

    const auto column_names = doc.GetColumnNames();
    const auto row_count = doc.GetRowCount();
    const auto expected_cols = column_names.size();
    const std::string filename =
        std::filesystem::path(file_path).filename().string();
    const std::int64_t offset = static_cast<std::int64_t>(file_idx) * 1000;

    for (std::size_t i = 0; i < row_count; ++i) {
      if (userver::engine::current_task::ShouldCancel())
        return isComplete::kCancelled;

      std::vector<std::string> row;
      try {
        row = doc.GetRow<std::string>(i);
      } catch (const std::exception& e) {
        throw std::runtime_error(fmt::format(
            "Failed to read row {} in file {}: {}", i, file_path, e.what()));
      }

      userver::formats::json::ValueBuilder builder{
          userver::formats::json::Type::kObject};
      bool has_real_content = false;
      std::string original_id = "-1";

      for (std::size_t col_idx = 0; col_idx < expected_cols; ++col_idx) {
        const std::string& col_name = column_names[col_idx];
        std::string value = (col_idx < row.size()) ? row[col_idx] : "";

        if (kOffsetFields.count(col_name)) {
          try {
            if (col_name == "id") original_id = value;
            std::int64_t val_as_int = std::stoll(value);
            builder[col_name] = val_as_int + offset;
            has_real_content = true;
          } catch (...) {
            builder[col_name] = value;
          }
        } else {
          if (!value.empty() && value != "\"\"" && value != "null") {
            has_real_content = true;
          }
          builder[col_name] = std::move(value);
        }
      }

      if (!has_real_content) {
        LOG_WARNING() << "Skipping empty row " << i << " in file " << file_path;
        continue;
      }

      const std::string msg =
          userver::formats::json::ToString(builder.ExtractValue());
      if (msg.empty()) {
        throw std::runtime_error("Failed to serialize JSON message for row " +
                                 std::to_string(i) + " in file " + file_path);
      }

      const std::string key = fmt::format("{}-{}", filename, i);
      producer_.Send(topic_, key, msg);
      if (i % batch_size_ == 0) userver::engine::Yield();
    }

    std::error_code ec;
    std::filesystem::remove(file_path, ec);
    if (ec) {
      LOG_ERROR() << "Successfully processed but FAILED to remove file: "
                  << file_path << ". Error: " << ec.message();
    } else {
      LOG_INFO() << "Successfully finished and removed: " << file_path;
    }
  } catch (const std::exception& ex) {
    LOG_ERROR() << "Critical error in ProcessFile [" << file_path
                << "]: " << ex.what();
    return isComplete::kError;
  }
  return isComplete::kSuccess;
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
