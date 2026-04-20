#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <userver/components/component_base.hpp>
#include <userver/engine/task/task_processor_fwd.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/kafka/producer.hpp>
#include <userver/utils/async.hpp>

#include <rapidcsv.h>

namespace kafka_sample {

class CsvProducerComponent final : public userver::components::ComponentBase {
 public:
  enum class isComplete : int8_t { kSuccess, kCancelled, kError = -1 };
  static constexpr std::string_view kName = "csv-producer";
  void OnAllComponentsLoaded() override;

  CsvProducerComponent(const userver::components::ComponentConfig& config,
                       const userver::components::ComponentContext& context);

  static userver::yaml_config::Schema GetStaticConfigSchema();

 private:
  void ProcessAllFiles();
  isComplete ProcessFile(const std::string& file_path, std::size_t file_idx);

  const userver::kafka::Producer& producer_;
  userver::engine::TaskProcessor& fs_task_processor_;

  const std::string csv_dir_;
  const std::string topic_;
  const std::size_t batch_size_;

  userver::engine::TaskWithResult<void> task_;
};

}  // namespace kafka_sample
