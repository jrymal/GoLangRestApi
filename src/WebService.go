package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"io"
)

const (
	IP_PORT               = "localhost:9999"
	
	DB_USER     = "root"
	DB_PASSWORD = "root"
	DB_NAME     = "employees"

	DB_AUTH = DB_USER + ":" + DB_PASSWORD + "@/" + DB_NAME
	HTTP_HDR_CONTENT_TYPE = "Content-Type"
	HTTP_HDR_STATUS_CODE = "Status-Code"
	VALUE_CONTENT_TYPE_JSON = "application/json; charset=utf-8"
)

type err_response struct {
	Message string `json: "message"`
}

type db_request struct {
	RequestType string       `json: "requestType"`
	Query []queryParam       `json: "query"`
	Create map[string]string `json: "create"`
	Update map[string]string `json: "update"`
}

type queryParam struct {
	ObjectName string
	Value string
	Comparison string
}

func main() {
	fmt.Printf("Starting service\n")
	http.HandleFunc("/", handleAllRoutes)
	http.ListenAndServe(IP_PORT, nil)
}

func handleAllRoutes(writer http.ResponseWriter, request *http.Request) {
	handler := WebHandler{writer: writer}
	handler.Execute(request)
}

// This is outside the scope of a WebService object so not in class
func executeQuery(handler *WebHandler, con *sql.DB, compostedQuery string, table string) {

	queryResp, err := con.Query(compostedQuery)
	if err != nil {
        handler.fireErrResponse(500, "Failed executing query: "+compostedQuery+":" +err.Error())
	}
	
	queryValues, err := queryResp.Columns()
	if err != nil {
        handler.fireErrResponse(500, "Failed extracting columns: "+err.Error())
	}

	body := make(map[string]string)
	
	colNames := getTableColumnNames(handler, con, table)
	if len(colNames) != len(queryValues) {
		// not supposed to happen
        handler.fireErrResponse(500, "DB response mismatch to table: expected "+ string(len(queryValues))+", recieved "+string(len(colNames)))
	}
	
	for index, colName := range colNames  {
		body[colName] = queryValues[index]
	}

	handler.setResponse(200, body)
}

func decodeJson(handler *WebHandler, body io.Reader, t interface{}) {
	decoder := json.NewDecoder(body)
	
	if err := decoder.Decode(&t); err != nil {
        handler.fireErrResponse(400, "Unable to parse body:"+err.Error())
	}
}

// pulls the column names from the table
func getTableColumnNames(handler *WebHandler, con *sql.DB, table string) []string{
	compostedQuery := "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='"+DB_NAME+"' AND `TABLE_NAME`='"+table+"';"

	resp, err := con.Query(compostedQuery)
	if err != nil {
        handler.fireErrResponse(500, "Failed executing query: "+compostedQuery+":" +err.Error())
	}
	
	cols, err := resp.Columns()
	if err != nil {
        handler.fireErrResponse(500, "Failed extracting columns: "+err.Error())
	}
	
	return cols;
}

// look up to get the table id column name for table
func lookUpTableIdColumn(handler *WebHandler, tableName string) string {

    // TODO: really need to identify a multi-index index and generate a composite id structure
    
	switch tableName {
		case "departments" :
			return "dept_no"
		case "dept_emp":
			return "emp_no"
		case "dept_manager":
			return "dept_no"
		case "employees":
			return "emp_no"
		case "salaries":
			return "emp_no"
		case "titles":
			return "emp_no"
		default:
        	handler.fireErrResponse(400, "Unknown table: "+tableName)
	}

	// note that this should never be used	
	return ""
}

func generateQueryItems(createMap map[string]string) (string, string) {
	var columns string
	var values string
	
	for k, v := range createMap {
		columns += k+","
		values += v+","
	}
	
	return columns, values
}

func generateComparisons(queryParams []queryParam) string {
	var comps string
	
	for _, value := range queryParams {
		comps += "("+value.ObjectName+value.Comparison+"'"+value.Value+"'),"
	}
	
	return comps 
}

func generateValuePairs(updateMap map[string]string) string {
	var comps string
	
	for k, v := range updateMap {
		comps += "("+k+"='"+v+"'),"
	}
	
	return comps 
}

type WebHandler struct {
	body interface{} // data in the body of the response
	code int // the respnse code to return with
	writer http.ResponseWriter // the writer to write to when ready
}

// Performs the handling of the request and returns the response
func (handler *WebHandler) Execute(request *http.Request) {	
	
	defer func(){
		if r := recover(); r != nil {
			handler.sendResponse()
		} 		
	}()
	
	urlPath := request.URL.Path
	path := strings.Split(urlPath, "/")
	
    handler.mandatoryPathLength(path, 1)
    handler.parseRequest(path, request)
	handler.sendResponse()
}

// ensures a length to the path requested, panics if the path is too short
func (handler *WebHandler) mandatoryPathLength(path []string, minLength int) {
	// seems that the first object is always empty in the split...not sure why
	if len(path) < (minLength+1) {
        handler.fireErrResponse(400, "Requested path is too short")
	}
}

// parse the request and handle it accordingly
func (handler *WebHandler) parseRequest(path []string, request *http.Request) {
    
	// Determine request location
	// Format: /db_table/args
    dbTable := path[1]

    // All commands use sql so build it now	
    con, err := sql.Open("mysql", DB_AUTH)
    defer con.Close()

    if err != nil {
        handler.fireErrResponse(500, "Failed to open DB connection")
    }

	// treat according to http method type    
    switch request.Method {
        case "GET":
            // Validation checks
            handler.mandatoryPathLength(path, 2)
            handler.handleGet(con, dbTable, path[2])
        case "POST":
            // use path[2] to id the type of post
            // create
            // query
            handler.handlePost(con, request.Body, dbTable)
        case "PUT":
            // Update an existing record.
            handler.handlePut(con, request.Body, dbTable, path[2])
        case "DELETE":
            // Remove the record.
            handler.handleDelete(con, dbTable, path[2])
        default:
            // Give an error message.
            handler.fireErrResponse(400, "Unsupported HTTP method: "+request.Method)
    }
}

// Fires a panic and sets the error message and error code
func (handler *WebHandler) fireErrResponse(code int, msg string) {
	handler.code = code
	handler.body = err_response{Message: msg}
	panic(msg)
}

// COnfigures the header to have the body and the status code as set i nthe parameters
func (handler *WebHandler) setResponse(code int, body interface{}) {
	handler.code = code
	handler.body = body
}

// marshals the response up and send it as per the configured object
func (handler *WebHandler) sendResponse() {
	marshaledResponse, err := json.Marshal(handler.body)
	
	if err != nil {
		handler.code = 500
		fmt.Fprint(handler.writer, "{ \"Message\": \"Failed marshialling: "+err.Error()+"\"")
	} else {
		handler.writer.Write([]byte(marshaledResponse))
	}
	
	// Set up the headers
	handler.writer.Header().Set(HTTP_HDR_CONTENT_TYPE, VALUE_CONTENT_TYPE_JSON)
	handler.writer.Header().Set(HTTP_HDR_STATUS_CODE, string(handler.code))
}

func (handler *WebHandler) handleGet (con *sql.DB, table string, id string) {
	idColumn := lookUpTableIdColumn(handler, table)
	compostedQuery := "select * from "+table+" where "+idColumn+"="+id+";"
	executeQuery(handler, con, compostedQuery, table)
}

func (handler *WebHandler) handlePost (con *sql.DB, body io.Reader, table string) {
	var t db_request
	decodeJson(handler, body, &t)

	var compostedQuery string	
	switch t.RequestType {
		case "create":
			columns, values := generateQueryItems(t.Create)
			compostedQuery = "insert into "+table+"("+columns+") values ("+values+");"
		case "query":
			comparisons := generateComparisons(t.Query)
			compostedQuery = "select * from "+table+" where "+comparisons+";"
		default:
            handler.fireErrResponse(400, "Unsupported request type: "+t.RequestType)
	}
	
	executeQuery(handler, con, compostedQuery, table)
}

func (handler *WebHandler) handlePut (con *sql.DB, body io.Reader, table string, id string) {
	idColumn := lookUpTableIdColumn(handler, table)

	var t db_request
	decodeJson(handler, body, &t)

	// only updates are handled in a put
	if t.RequestType != "update"{
        handler.fireErrResponse(400, "Unsupported request type: "+t.RequestType)
	}
	
	valuePairs := generateValuePairs(t.Update)
	compostedQuery := "update "+table+" set "+ valuePairs +" where "+idColumn+"="+id+";"
	executeQuery(handler, con, compostedQuery, table)
}

func (handler *WebHandler) handleDelete (con *sql.DB, table string, id string) {
	idColumn := lookUpTableIdColumn(handler, table)
	compostedQuery := "delete from "+table+" where "+idColumn+"='"+id+"';"
	executeQuery(handler, con, compostedQuery, table)
}