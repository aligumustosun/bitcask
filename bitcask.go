package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var dirName = "files"

const fileSizeThreshold = 1024 * 1024 * 1024 * 2 // 2GB
const batchSizeThreshold = 1024 * 1024 * 4       // 4MB
const valueSizeThreshold = 1024 * 1024 * 3       // 3MB
const loopCount = 1000

var keyMap = make(map[string]struct {
	filename  string
	offset    int
	valueSize int
})

func main() {
	file, err := getFileToOperateOn()
	if err != nil {
		fmt.Println("Error0:", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var batchWriteRow string
	var batchSize int
	var totalOffset int64
	startTime := time.Now()

	for i := 0; i < loopCount; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%s", strings.Repeat("a", valueSizeThreshold-7))
		pair := struct {
			key   string
			value string
		}{
			key,
			value,
		}
		writeRowWithoutNewLine := buildWriteRow(pair.key, pair.value)
		writeRow := writeRowWithoutNewLine + "\n"
		batchWriteRow += writeRow
		batchSize += len(writeRow)

		// Update offsets and metadata for each key
		keyMap[pair.key] = struct {
			filename  string
			offset    int
			valueSize int
		}{
			filename:  file.Name(),
			offset:    int(totalOffset) + len(writeRowWithoutNewLine) - len(pair.value),
			valueSize: len(pair.value),
		}

		totalOffset += int64(len(writeRow))

		if batchSize >= batchSizeThreshold || i == loopCount-1 {
			_, err := writer.WriteString(batchWriteRow)
			if err != nil {
				fmt.Println("Error writing batch to file:", err)
				return
			}
			writer.Flush()

			batchWriteRow = "" // Reset the batch
			batchSize = 0

			// Check if a new file is needed and rotate if necessary
			fileInfo, err := file.Stat()
			if err != nil {
				fmt.Println("Error getting file info:", err)
				return
			}
			if fileInfo.Size() > fileSizeThreshold {
				err = rotateFile(&file, &writer)
				totalOffset = 0
				if err != nil {
					fmt.Println("Error rotating file:", err)
					return
				}
			}
		}
	}

	if len(batchWriteRow) > 0 {
		_, err := writer.WriteString(batchWriteRow)
		if err != nil {
			fmt.Println("Error writing final batch to file:", err)
			return
		}
		err = writer.Flush()
		if err != nil {
			fmt.Println("Error flushing final batch to file:", err)
			return
		}
	}

	endTime := time.Now() // Record the end time
	fmt.Printf("Execution Time: %s\n", endTime.Sub(startTime))
	/* fmt.Println(keyMap) */

	// log all values from the keymap using getKeyValue function
	for key := range keyMap {
		_, err := getKeyValue(key)
		if err != nil {
			fmt.Println("Error1:", err)
			return
		}
		/* fmt.Println("key:", key, "value:", value) */
	}

}

// rotateFile closes the current file, opens a new file, and updates the file and writer references
func rotateFile(file **os.File, writer **bufio.Writer) error {
	// Ensure the current buffer is flushed to avoid data loss
	if err := (*writer).Flush(); err != nil {
		return err
	}

	// Close the current file after flushing
	if err := (*file).Close(); err != nil {
		return err
	}

	// Open a new file for writing
	newFile, err := getFileToOperateOn() // Assuming this creates or finds a new file to write to
	if err != nil {
		return err
	}

	// Update the file and writer pointers to point to the new objects
	*file = newFile
	*writer = bufio.NewWriter(newFile)

	return nil
}

func getKeyValue(key string) (string, error) {
	value := ""
	if val, ok := keyMap[key]; ok {
		value, err := getValueFromFileWithOffsetAndSize(val.filename, val.offset, val.valueSize)
		if err != nil {
			fmt.Println("Error2:", err)
			return "", err
		}
		return value, nil
	}
	return value, nil
}

func getValueFromFileWithOffsetAndSize(filename string, offset int, size int) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error3:", err)
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		fmt.Println("Error4:", err)
		return "", err

	}
	value := make([]byte, size)
	_, err = file.Read(value)
	if err != nil {
		fmt.Println("Error5:", offset, size, filename)
		fmt.Println("Error5:", err)
		return "", err
	}
	return string(value), nil
}

func getFileToOperateOn() (*os.File, error) {
	fileName, err := findMostRecentFileBasedOnName(dirName)
	if err != nil {
		fmt.Println("Error6:", err)
		return nil, err
	}
	fullPath := fmt.Sprintf("%s/%s", dirName, fileName)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		fmt.Println("Error7:", err)
		return nil, err
	}

	if fileInfo.Size() > fileSizeThreshold {
		fileName, err = createFileWithTimestamp(dirName)
		if err != nil {
			fmt.Println("Error8:", err)
			return nil, err
		}
	}
	fullPath = fmt.Sprintf("%s/%s", dirName, fileName)

	file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return file, nil

}

func createFileWithTimestamp(path string) (string, error) {
	v := time.Now().UTC().Format("2006-01-02T15-04-05.000Z")
	filename := path + "/" + v
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	file.Close()
	return v, nil
}

func writeToFile(file *os.File, data string) error {
	// write data to file
	_, err := file.WriteString(data)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func buildWriteRow(key string, value string) string {
	v := time.Now().UTC().Format("2006-01-02T15-04-05.000Z")
	keySize := len(key)
	valueSize := len(value)
	key = fmt.Sprintf("%d,%s", keySize, key)
	value = fmt.Sprintf("%d,%s", valueSize, value)

	checksum := createChecksum(v + "," + key + "," + value)
	writeRow := fmt.Sprintf("%d,%s,%s,%s", checksum, v, key, value)

	return writeRow
}

func createChecksum(data string) int {
	checksum := 0
	for _, c := range data {
		checksum += int(c)
	}
	return checksum
}

func verifyRow(row string) bool {
	// split the row by ,
	values := strings.Split(row, ",")
	checksum, timestamp, keySize, key, valueSize, value := values[0], values[1], values[2], values[3], values[4], values[5]
	checksumInt, err := strconv.Atoi(checksum)
	if err != nil {
		fmt.Println(err)
		return false
	}

	createdChecksum := createChecksum(timestamp + "," + keySize + "," + key + "," + valueSize + "," + value)

	return checksumInt == createdChecksum
}

func findMostRecentFileBasedOnName(dirPath string) (string, error) {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return "", err
	}

	// if no files found in the directory
	// create one and return its dirpath
	if len(files) == 0 {
		fileName, err := createFileWithTimestamp(dirPath)
		if err != nil {
			return "", err
		}
		return fileName, nil
	}

	var newestFile string
	for _, file := range files {
		if !file.IsDir() && file.Name() > newestFile {
			newestFile = file.Name()
		}
	}

	if newestFile == "" {
		return "", fmt.Errorf("no files found in the directory")
	}
	return newestFile, nil
}
