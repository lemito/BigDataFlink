#pragma once

#include <string>
#include <vector>

#include <userver/components/component_base.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/kafka/producer.hpp>
#include <userver/utils/async.hpp>
#include <rapidcsv.h>
namespace kafka_sample {

class CsvProducerComponent final : public userver::components::ComponentBase {
 public:
  static constexpr std::string_view kName = "csv-producer";

  CsvProducerComponent(const userver::components::ComponentConfig& config,
                       const userver::components::ComponentContext& context);

  static userver::yaml_config::Schema GetStaticConfigSchema();

 private:
  void ProcessAllFiles();

  void ProcessFile(const std::string& file_path);

  const userver::kafka::Producer& producer_;
  const std::string csv_dir_;
  const std::string topic_;
  const std::size_t batch_size_;

  userver::engine::TaskWithResult<void> task_;
};

}  // namespace kafka_sample
