package controllers

import (
	"fmt"
	"os"
)

func LogFileExists(basePath string, logName string) (bool, error) {
	files, err := os.ReadDir(basePath)
	if err != nil {
		return false, err
	}

	fmt.Println(files)

	for _, file := range files {
		if !file.IsDir() && file.Name() == logName {
			return true, nil
		}
	}

	return false, nil
}
