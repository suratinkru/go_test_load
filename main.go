package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
)

var client *elastic.Client

func main() {
	var err error
	elasticsearchURL := os.Getenv("ELASTICSEARCH_URL") // Get Elasticsearch URL from environment variable
	if elasticsearchURL == "" {
		elasticsearchURL = "http://13.215.48.128:9200" // Default to localhost if not specified
	}

	// Implement retry mechanism for Elasticsearch client creation
	for i := 0; i < 3; i++ {
		client, err = elastic.NewClient(
			elastic.SetURL(elasticsearchURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false), // Disable health check to prevent immediate failure
			elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(10*time.Millisecond, 1*time.Second))),
		)
		if err == nil {
			break
		}
		log.Printf("Attempt %d: Error creating Elasticsearch client: %v", i+1, err)
		time.Sleep(2 * time.Second) // Wait before retrying
	}
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	router := gin.Default()

	router.POST("/insertData", insertData)
	router.GET("/checkData", checkData) // Add this line to create a new route
	// Other routes...

	router.Run(":3000")
}

func insertData(c *gin.Context) {
	var data map[string]interface{}
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	indexName := "your-index-name" // Ensure this is set to your actual index name

	_, err := client.Index().
		Index(indexName).
		BodyJson(data).
		Do(context.Background())

	if err != nil {
		log.Printf("Error inserting data into Elasticsearch: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error inserting data into Elasticsearch"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Data inserted successfully"})
}

func checkData(c *gin.Context) {
	ctx := context.Background()
	countResult, err := client.Count().
		Index("_all"). // Count across all indices
		Query(elastic.NewMatchAllQuery()).
		Do(ctx)

	if err != nil {
		log.Printf("Error searching Elasticsearch: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"count": countResult})
}
