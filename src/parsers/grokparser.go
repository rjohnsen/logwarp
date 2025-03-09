package parsers

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/elastic/go-grok"
	"github.com/google/uuid"
	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/models"
	"github.com/rjohnsen/logwarp/src/syskernel"
)

func GrokParser(appContxt *syskernel.AppContext, jobid string, index string, filepath string, grokpattern string) {
	file, err := os.Open(filepath)
	if err != nil {
		error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Failed to open file: %v", err)}
		syskernel.SendError(appContxt.Nats, error)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var documentsBatch []models.Document

	groker := grok.New()
	err_grok := groker.Compile(grokpattern, true)

	if err_grok != nil {
		error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Unable to compile Grok statement: %s, %v", grokpattern, err)}
		syskernel.SendError(appContxt.Nats, error)
	} else {
		for scanner.Scan() {
			line := scanner.Text()

			data, err := groker.ParseTypedString(line)

			if err != nil {
				error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Unable to parse line: %s", line)}
				syskernel.SendError(appContxt.Nats, error)
			} else {

				documentId := uuid.New()

				document := models.Document{
					Metadata: models.IndexMetadataWrapper{
						Index: models.IndexMeta{
							IndexName:  index,
							DocumentID: documentId.String(),
						},
					},
					Source: models.DocumentSource{
						Content: data,
					},
				}

				if timestamp, ok := document.Source.Content["timestamp"].(string); ok && timestamp != "" {
					converted_time, _ := ConvertTimeFormat(timestamp)
					document.Source.Content["timestamp"] = converted_time
				} else {
					error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Missing or invalid timestamp in line %s", line)}
					syskernel.SendError(appContxt.Nats, error)
				}

				documentsBatch = append(documentsBatch, document)

				// Perform bulk insert when the batch reaches bulkSize.
				if len(documentsBatch) >= appContxt.Settings.OpenSearch.BulkSize {
					if err := controllers.PerformBulkInsert(appContxt.OpenSearch, documentsBatch); err != nil {
						error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Bulk insert failed: %v", err)}
						syskernel.SendError(appContxt.Nats, error)
					}
					documentsBatch = documentsBatch[:0] // Clear slice without reallocating.
				}
			}
		}

		// Insert remaining documents if any.
		if len(documentsBatch) > 0 {
			if err := controllers.PerformBulkInsert(appContxt.OpenSearch, documentsBatch); err != nil {
				error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Bulk insert failed: %v", err)}
				syskernel.SendError(appContxt.Nats, error)
			}
		}
	}
}

func ConvertTimeFormat(timestring string) (string, bool) {
	inputLayout := "02/Jan/2006:15:04:05 -0700"

	parsedTime, err := time.Parse(inputLayout, timestring)
	if err != nil {
		fmt.Printf("Failed to parse timestamp: %v\n", err)
		return "", true
	}

	return parsedTime.UTC().Format(time.RFC3339), false
}
