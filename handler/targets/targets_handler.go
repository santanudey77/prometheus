package targets

import (
	"encoding/json"
	"log"
	"io/ioutil"
	"os"
)

type Label struct{
	Job string	`json:"job"`
	Id	string	`json:"id"`
}

type Target struct {
	Targets	[]string	`json:"targets"`
	Labels	Label		`json:"labels"`
}

func AddTargetToConfig (id, url, dir_of_json string) bool{
	filename := dir_of_json + "/targets.json"
	
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
		return false
	}

	defer file.Close()

	file_data, err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatal(err)
		return false
	}

	json_text := file_data

	var targets []Target

	if err := json.Unmarshal([]byte(json_text), &targets); err != nil {
		log.Println(err)
		return false
	}

	target_url_str := []string{url}
	targets = append(targets, Target{Targets: target_url_str, Labels: Label{Job: "dist", Id: id}})

	result, err := json.Marshal(targets)
	if err != nil {
		log.Println(err)
		return false
	}

	if !writeToFile(filename, string(result)){
		log.Println("unable to write")
		return false
	}

	return true
}

func writeToFile(filename, data string) bool{
	file, err := os.Create(filename)
	if err != nil{
		log.Println(err)
		return false
	}

	defer file.Close()

	file.WriteString(data)

	return true
}
