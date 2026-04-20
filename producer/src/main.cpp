#include <userver/components/common_component_list.hpp>
#include <userver/components/common_server_component_list.hpp>
#include <userver/components/component.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/server/handlers/ping.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/daemon_run.hpp>


#include "csv_producer.hpp"

int main(int argc, char* argv[]) {
  auto component_list =
      userver::components::ComponentList()
          .AppendComponentList(userver::components::CommonComponentList())
          .AppendComponentList(userver::components::CommonServerComponentList())
          .Append<userver::server::handlers::Ping>()
          .Append<userver::kafka::ProducerComponent>("kafka-producer")
          .Append<userver::components::Secdist>()
          .Append<userver::components::DefaultSecdistProvider>()
          .Append<kafka_sample::CsvProducerComponent>();

  return userver::utils::DaemonMain(argc, argv, component_list);
}
