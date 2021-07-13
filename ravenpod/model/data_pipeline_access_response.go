package model

type DataPipelineAccessResponse struct {
	Region              string    `json:"region"`
	AccessKey           string    `json:"access_key"`
	SecretAccessKey     string    `json:"secret_access_key"`
}