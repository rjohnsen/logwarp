package parsers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/opensearch-project/opensearch-go"
)

type Index struct {
	Id    string `json:"_id"`
	Index string `json:"_index"`
}

type IndexData struct {
	Index Index `json:"index"`
}

type Source struct {
	Source map[string]interface{} `json:"_source"`
}

type Document struct {
	Meta   IndexData
	Source Source
}

func LineParser(client *opensearch.Client, filepath string) {
	bulk_size := 10000
	file, err := os.Open(filepath)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	// Read file line by line into buffer
	scanner := bufio.NewScanner(file)

	var bulk_data []Document

	for scanner.Scan() {
		line := scanner.Text()

		var doc Document
		err := json.Unmarshal([]byte(line), &doc.Meta.Index)

		if err != nil {
			fmt.Println("Error parsing JSON Index data")
		}

		err_1 := json.Unmarshal([]byte(line), &doc.Source)

		if err_1 != nil {
			fmt.Println("Error parsing JSON Source")
		}

		bulk_data = append(bulk_data, doc)

		if len(bulk_data) == bulk_size {
			BulkInsert(client, bulk_data)
			bulk_data = nil
		}
	}

	if len(bulk_data) > 0 {
		BulkInsert(client, bulk_data)
	}
}

func BulkInsert(client *opensearch.Client, documents []Document) {
	var insertion_list []string

	for _, document := range documents {
		index, _ := json.Marshal(document.Meta)
		record, _ := json.Marshal(document.Source.Source)

		insertion_list = append(insertion_list, string(index))
		insertion_list = append(insertion_list, string(record))
	}

	shipment := strings.Join(insertion_list, "\n") + "\n"
	bulkResp, err := client.Bulk(strings.NewReader(shipment))

	if err != nil {
		fmt.Println("Bulkresp")
		fmt.Println(err)
		fmt.Println(bulkResp)
	}

	respAsJson, err := json.MarshalIndent(bulkResp, "", "  ")
	if err != nil {
		fmt.Println("respAsJson")
		fmt.Println(err)
	}

	fmt.Println(string(respAsJson))
}
