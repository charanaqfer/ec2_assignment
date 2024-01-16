package main

import (
	// "bytes"
	"context"
	"reflect"
	"strconv"

	// "io/ioutil"
	"fmt"
	"log"

	// "os"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	// "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	// "github.com/aws/aws-sdk-go-v2/service/s3/types"
)
type employee_info struct{
	Id int `json:"id,omitempty"`
	Name string `json:"name"`
	Age int `json:"age"`
	Address string `json:"address"`
	Is_active bool `json:"is_active,omitempty"`
}
// var employees []employee_info
// var bucket_name = "sivasankar-test-bucket"
// var file_key = "empRecords/rec.csv"
var bucket_name = "charan-test-bucket"
var file_key = "data.csv"
func read_csv(csv_content string) ([]employee_info, error) {
	var employees []employee_info

	reader := csv.NewReader(strings.NewReader(csv_content))

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			return nil, err
		}
		if (record[0] == "id"){
			continue
		}
		var id,age int
		if reflect.TypeOf(record[0])!=reflect.TypeOf(id){
			id,err=strconv.Atoi(record[0])
			if err!=nil{
				return employees,err
			}
		}
		if reflect.TypeOf(record[2])!=reflect.TypeOf(id){
			age,err=strconv.Atoi(record[2])
			if err!=nil{
				return employees,err
			}
		}
		employee := employee_info{
			Id:       id,
			Name:     record[1],
			Age:      age, 
			Address:  record[3],
			Is_active: strings.ToLower(record[4]) == "true" || record[4]=="",
		}

		employees = append(employees, employee)
	}

	return employees, nil
}
func get_employees() (*[]employee_info, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("Unable to load SDK config: %v", err)
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	input := &s3.GetObjectInput{
		Bucket: &bucket_name,
		Key:    &file_key,
	}

	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		log.Printf("Failed to get object from S3: %v", err)
		return  nil, err
	}
	defer result.Body.Close()

	file_content, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Failed to read file content: %v", err)
		return nil, err
	}

	content_string := string(file_content)
	employee_data,err:=read_csv(content_string)
	if err!=nil{
		return nil,err
	}
	
	log.Printf("Successfully read file from S3: %s", content_string)
	return &employee_data, nil
}

func write_to_s3(w http.ResponseWriter,r *http.Request,employee employee_info){
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		http.Error(w,"Unable to load SDK config: %v",http.StatusInternalServerError)
		return  
	}
	client := s3.NewFromConfig(cfg)
	// var existing_content *[]employee_info
	existing_content, err := get_employees()
	if err != nil {
		http.Error(w,"error reading existing content from S3:", http.StatusInternalServerError)
		return
	}
	for i :=range *existing_content{
		if (*existing_content)[i].Name == employee.Name{
			(*existing_content)[i].Address=employee.Address
			(*existing_content)[i].Age=employee.Age
			(*existing_content)[i].Is_active=employee.Is_active
		}
	}
	// if len(existing_content)==0{
	// 	employee.Id=1
	// }else{
	// 	employee.Id=existing_content[len(existing_content)-1].Id + 1
	// }
	// // Append the new data to the existing content
	// new_content := append(existing_content, employee)
	// Convert the updated content to CSV format
	csv_content, err := convert_to_csv(*existing_content)
	if err != nil {
		http.Error(w,"error converting content to CSV: ",http.StatusInternalServerError)
		return
	}

	// Upload the updated content to S3
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket_name,
		Key:    &file_key,
		Body:   strings.NewReader(csv_content),
	})
	if err != nil {
		http.Error(w,"error uploading content to S3: %v",http.StatusInternalServerError)
		return
	}
}


func convert_to_csv(employee []employee_info) (string, error) {
	// Convert data to CSV format
	var csv_data [][]string
	for _, entry := range employee {
		csv_data = append(csv_data, []string{strconv.Itoa(entry.Id),entry.Name,strconv.Itoa(entry.Age), entry.Address,strconv.FormatBool(entry.Is_active)})
	}

	// Write CSV data to a string
	var csv_content strings.Builder
	writer := csv.NewWriter(&csv_content)
	err := writer.WriteAll(csv_data)
	if err != nil {
		return "", fmt.Errorf("error writing CSV data: %v", err)
	}

	return csv_content.String(), nil
}

func put_method(w http.ResponseWriter,r *http.Request,req_name string)(){
	var employee employee_info
	employee.Is_active=true
	// err:=json.Unmarshal([]byte(r.Body),&employee)
	err:=json.NewDecoder(r.Body).Decode(&employee)
	if err != nil {
		http.Error(w,"Error unmarshaling JSON to employee ", http.StatusBadRequest)
		return 
	}
	response_body := fmt.Sprintf("Employee ,Name %s,Age: %d, Address: %s, is_active : %t", employee.Name,employee.Age, employee.Address,employee.Is_active)
	fmt.Printf("%s",response_body)
	write_to_s3(w,r,employee)
	// if err != nil {
	// 	http.Error(w,"Error writing to S3:",http.StatusInternalServerError)
	// 	return
	// }
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Employee details updated successfully"))
}
func get_method_with_name(w http.ResponseWriter,r *http.Request,req_name string){
	employee_data, err := get_employees()
	if err != nil {
		http.Error(w,"Error reading S3 file", http.StatusInternalServerError)
	}
	for _,row := range *employee_data{
		if row.Name==req_name{
			employees_json, err := json.Marshal(row)
			if err != nil {
				http.Error(w,"Error marshaling employees to JSON", http.StatusInternalServerError)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(employees_json)	
			return
		}
	}
	
	http.Error(w,"username not found", http.StatusNotFound)
	// events.APIGatewayProxyResponse{StatusCode: http.StatusNotFound, Body: "username not found"}, err
}
func get_method(w http.ResponseWriter,r *http.Request){
	employee_data, err := get_employees()
	if err != nil {
		http.Error(w,"Error reading S3 file:", http.StatusInternalServerError)
		return
	}
	employees_json, err := json.Marshal(&employee_data)
	if err != nil {
		http.Error(w,"Error marshaling employees to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(employees_json)
}
func handler(w http.ResponseWriter,r *http.Request) {
	path:=r.URL.RawPath
	path_params:=strings.Split(path, "/")
	// for _,row:=range path_params{
	// 	fmt.Printf("path_parameters %s ,",row)
	// }
    if r.Method==http.MethodGet{
		if len(path_params)==2 && path_params[1]=="employees"{
			get_method(w,r)
			return 
		}
		if len(path_params)==3 && path_params[2]!=""{
			get_method_with_name(w,r,path_params[2])
			return
		}
		// return events.APIGatewayProxyResponse{StatusCode: http.StatusNotFound, Body: "Invalid url"}, nil
		
    }
	if r.Method==http.MethodPut && len(path_params)==3 && path_params[2]!=""{
		put_method(w,r,path_params[2])
		return
	}
	http.Error(w,"invalid url", http.StatusNotFound)
}
func main(){
	// lambda.Start(handler_get)
	http.HandleFunc("/employees",handler)
	http.HandleFunc("/employees/",handler)
	port:=8080
	fmt.Printf("Server is listening on :%d...\n", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}