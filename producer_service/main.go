package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var sugar *zap.SugaredLogger

const BATCH_SIZE int = 100

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	sugar = logger.Sugar()
}

func newKafkaWriter(broker string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    BATCH_SIZE,
		BatchTimeout: 50 * time.Millisecond,
	}
}

func main() {
	defer sugar.Sync()

	sugar.Info("Start")

	broker := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if broker == "" {
		sugar.Fatal("KAFKA_BOOTSTRAP_SERVERS не задан")
	}

	topic := "input-topic"

	writer := newKafkaWriter(broker, topic)
	defer writer.Close()

	filesPath := "/data/*.csv"
	files, err := filepath.Glob(filesPath)
	if err != nil || len(files) == 0 {
		sugar.Fatalf("CSV файлы не найдены в %s", filesPath)
	}

	sugar.Infof("Найдено файлов: %d", len(files))

	for _, file := range files {
		sugar.Infof("Обработка файла: %s", file)
		processFile(file, writer)
	}

	sugar.Info("UwU")
}

func processFile(filePath string, writer *kafka.Writer) {
	f, err := os.Open(filePath)
	if err != nil {
		sugar.Errorf("Ошибка открытия файла %s: %v", filePath, err)
		return
	}
	defer f.Close()

	reader := csv.NewReader(f)

	headers, err := reader.Read()
	if err != nil {
		sugar.Errorf("Ошибка чтения заголовков в %s: %v", filePath, err)
		return
	}

	batch := make([]kafka.Message, 0, BATCH_SIZE)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := writer.WriteMessages(ctx, batch...); err != nil {
			sugar.Errorf("Ошибка отправки батча в Kafka: %v", err)
		}
		batch = batch[:0]
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			sugar.Errorf("Ошибка чтения строки в %s: %v", filePath, err)
			continue
		}

		messageMap := make(map[string]string, len(headers))
		for i, value := range record {
			if i < len(headers) {
				messageMap[headers[i]] = value
			}
		}

		jsonData, err := json.Marshal(messageMap)
		if err != nil {
			sugar.Errorf("Ошибка JSON: %v", err)
			continue
		}

		batch = append(batch, kafka.Message{Value: jsonData})

		if len(batch) >= BATCH_SIZE {
			flush()
		}

		if len(batch) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	flush()
	sugar.Infof("Файл %s обработан", filePath)
}
