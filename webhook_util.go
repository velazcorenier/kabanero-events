/*
Copyright 2019 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

/* constants */
const (
	TRIGGERS = "triggers"
	CHKSUM = "sha256"
)

func readFile(fileName string) ([]byte, error) {
	ret := make([]byte, 0)
	file, err := os.Open(fileName)
	if err != nil {
		return ret, err
	}
	defer file.Close()
	input := bufio.NewScanner(file)
	for input.Scan() {
		for _, b := range input.Bytes() {
			ret = append(ret, b)
		}
	}
	return ret, nil
}

func readJSON(fileName string) (*unstructured.Unstructured, error) {
	bytes, err := readFile(fileName)
	if err != nil {
		return nil, err
	}
	var unstructuredObj = &unstructured.Unstructured{}
	err = unstructuredObj.UnmarshalJSON(bytes)
	if err != nil {
		return nil, err
	}
	return unstructuredObj, nil
}

func downloadFileTo(url, path string) error {
	client := http.Client{}
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the file
	_, err = io.Copy(out, response.Body)
	return err
}

func getHTTPURLReaderCloser(url string) (io.ReadCloser, error) {

	client := http.Client{}
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusOK {
		return response.Body, nil
	}
	return nil, fmt.Errorf("unable to read from url %s, http status: %s", url, response.Status)
}

/* Read remote file from URL and return bytes */
func readHTTPURL(url string) ([]byte, error) {
	readCloser, err := getHTTPURLReaderCloser(url)
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	bytes, err := ioutil.ReadAll(readCloser)
	return bytes, err
}

func yamlToMap(bytes []byte) (map[string]interface{}, error) {
	var myMap map[string]interface{}
	err := yaml.Unmarshal(bytes, &myMap)
	if err != nil {
		return nil, err
	}
	return myMap, nil
}

/* Get the URL and sha256 checksum of where the trigger is stored */
func getTriggerURL(collection map[string]interface{}) (string, string, error) {
	triggersObj, ok := collection[TRIGGERS]
	if !ok {
		return "", "", fmt.Errorf("collection does not contain triggers: section")
	}

	triggersArray, ok := triggersObj.([]interface{})
	if !ok {
		return "", "", fmt.Errorf("collection does not contain triggers section is not an Arry")
	}

	var retURL = ""
	var retChkSum = ""

	for index, arrayElement := range triggersArray {
		mapObj, ok := arrayElement.(map[interface{}]interface{})
		if !ok {
			return "", "", fmt.Errorf("triggers section at index %d is not properly formed", index)
		}
		urlObj, ok := mapObj[URL]
		if !ok {
			return "", "", fmt.Errorf("triggers section at index %d does not contain url", index)
		}
		url, ok := urlObj.(string)
		if !ok {
			return "", "", fmt.Errorf("triggers section at index %d url is not a string: %v", index, urlObj)
		}
		retURL = url

		if !skipChkSumVerify {
			chksumObj, ok := mapObj[CHKSUM]
			if !ok {
				return "", "", fmt.Errorf("triggers section at index %d does not contain sha256 checksum", index)
			}
			chksum, ok := chksumObj.(string)
			if !ok {
				return "", "", fmt.Errorf("triggers section at index %d is not a string: %v", index, chksumObj)
			}
			retChkSum = chksum
		}
	}

	if retURL == "" {
		return "", "", fmt.Errorf("unable to find url from triggers section")
	}

	if !skipChkSumVerify && retChkSum == "" {
		return "", "", fmt.Errorf("unable to find sha256 checksum from triggers section")
	}

	return retURL, retChkSum, nil
}

/* Merge a directory path with a relative path. Return error if the rectory not a prefix of the merged path after the merge  */
func mergePathWithErrorCheck(dir string, toMerge string) (string, error) {
	dest := filepath.Join(dir, toMerge)
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	dest, err = filepath.Abs(dest)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(dest, dir) {
		return dest, nil
	}
	return dest, fmt.Errorf("unable to merge directory %s with %s, The merged directory %s is not in a subdirectory", dir, toMerge, dest)
}

/* Calculate the SHA256 sum of a file */
func sha256sum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

/* gunzip and then untar into a directory */
func gUnzipUnTar(readCloser io.ReadCloser, dir string) error {
	defer readCloser.Close()

	gzReader, err := gzip.NewReader(readCloser)
	if err != nil {
		return err
	}
	tarReader := tar.NewReader(gzReader)
	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if header == nil {
			continue
		}
		dest, err := mergePathWithErrorCheck(dir, header.Name)
		if err != nil {
			return err
		}
		fileInfo := header.FileInfo()
		mode := fileInfo.Mode()
		if mode.IsRegular() {
			fileToCreate, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("unable to create file %s, error: %s", dest, err)
			}

			_, err = io.Copy(fileToCreate, tarReader)
			closeErr := fileToCreate.Close()
			if err != nil {
				return fmt.Errorf("unable to read file %s, error: %s", dest, err)
			}
			if closeErr != nil {
				return fmt.Errorf("unable to close file %s, error: %s", dest, closeErr)
			}
		} else if mode.IsDir() {
			err = os.MkdirAll(dest, 0755)
			if err != nil {
				return fmt.Errorf("unable to make directory %s, error:  %s", dest, err)
			}
			klog.Infof("Created subdirectory %s\n", dest)
		} else {
			return fmt.Errorf("unsupported file type within tar archive: file within tar: %s, field type: %v", header.Name, mode)
		}

	}
	return nil
}

/* Download the trigger.tar.gz and unpack into the directory
kabaneroIndexUrl: URL that serves kabanero-index.yaml
dir: directory to unpack the trigger.tar.gz
*/
func downloadTrigger(kabaneroIndexURL string, dir string) error {
	if klog.V(5) {
		klog.Infof("Entering downloadTrigger kabaneroIndexURL: %s, directory to store trigger: %s", kabaneroIndexURL, dir)
		defer klog.Infof("Leaving downloadTrigger kabaneroIndexURL: %s, directory to store trigger: %s", kabaneroIndexURL, dir)
	}
	kabaneroIndexBytes, err := readHTTPURL(kabaneroIndexURL)
	if err != nil {
		return err
	}
	if klog.V(5) {
		klog.Infof("Retrieved kabanero index file: %s", string(kabaneroIndexBytes))
	}
	kabaneroIndexMap, err := yamlToMap(kabaneroIndexBytes)
	if err != nil {
		return err
	}
	triggerURL, triggerChkSum, err := getTriggerURL(kabaneroIndexMap)
	if err != nil {
		return err
	}

	if klog.V(5) {
		klog.Infof("Found trigger with URL %s and sha256 checksum of %s", triggerURL, triggerChkSum)
	}

	triggerArchiveName := filepath.Join(dir, "incubator.trigger.tar.gz")
	err = downloadFileTo(triggerURL, triggerArchiveName)
	if err != nil {
		return err
	}

	// Verify that the checksum matches the value found in kabanero-index.yaml
	if !skipChkSumVerify {
		chkSum, err := sha256sum(triggerArchiveName)
		if err != nil {
			return fmt.Errorf("unable to calculate checksum of file %s: %s", triggerArchiveName, err)
		}

		if klog.V(5) {
			klog.Infof("Calculated sha256 checksum of file %s: %s", triggerArchiveName, chkSum)
		}

		if chkSum != triggerChkSum {
			klog.Fatalf("trigger collection checksum does not match the checksum from the Kabanero index: found: %s, expected: %s",
				chkSum, triggerChkSum)
		}
	}

	// Untar the triggers collection
	triggerReadCloser, err := os.Open(triggerArchiveName)
	if err != nil {
		return err
	}

	err = gUnzipUnTar(triggerReadCloser, dir)
	return err
}
