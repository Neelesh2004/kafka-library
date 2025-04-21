package config

type KafkaProviderConfig struct {
	KafkaConfig KafkaConfig
}

type KafkaConfig struct{
	ConnectionString  string `json:"connectionString"`
	Username          string `json:"username"`
	Password          string `json:"password"`
	ConsumerGroup     string `json:"consumerGroup"`
}

