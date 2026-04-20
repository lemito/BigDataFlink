# BigDataFlink

Поднять контейнер: ```docker compose up```  
Дальше остаётся только ждать... Ждать придется долго :)
За успехом можно наблюдать в:
- консоли docker compose (она отвечает за job-у и общие логи)
- grafana - localhost:3000 - туда летят логи userver-а
- файлик build/logs/producer.log - туда летят логи userver-а
- dbeaver - собственно, работа самого Flink 

### Что используется? (библиотеки, фреймворки и пр)
1. userver (c++ фреймворк, который является проодьюсером/примером оч нагруженого сервиса)
2. apache/kafka - брокер сообщений
3. apache/flink
4. postgresql
5. kafka-ui (ui админка для kafka)



![партиции в админке](image.png)
