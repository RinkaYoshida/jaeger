package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"errors"
	"os"
	"fmt"
	"sort"
	"math"
	"reflect"
	"strconv"
	"time"
	"strings"

	"github.com/jaegertracing/jaeger/pkg/sqlite/config"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/cache"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	_ "github.com/mattn/go-sqlite3"
)

var (
	benchmark = false

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("Malformed request object")
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("Service Name must be set")
	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("Start and End Time must be set")
	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("Start Time Minimum is above Maximum")
	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("Duration Minimum is above Maximum")
)

const (
	defaultNumTraces = 100
)

type Store struct {
	// number of SQLite database files there are
	num_db 			int 

	// the path to the directory that contains database files
	db_path			string

	// a list of connection objects to SQLite databases. conn to db-0 is at 
	// idx 0, conn to 1-DB is at idx 1
	conns 			[]*sql.DB

	cache_size		int
	cache			cache.Cache
}

func trackExecutionTime(funcName string) func() {
	startTime := time.Now()
	return func() {
		elapsed := time.Since(startTime)
		fmt.Printf("%s took %d\n", funcName, elapsed)
	}
}



func WithConfiguration(configuration config.Configuration) *Store {
	db_path := configuration.DBPath
	cache_size := configuration.CacheSize

	// default value of the cache size is 300 traces 
	if cache_size == 0 {
		cache_size = 300
	}

	// get all files that ends with -DB in the db_path directory
	var db_files []string
	err := filepath.Walk(db_path, func(path string, file os.FileInfo, err error) error {
		if !file.IsDir() {
			if strings.HasSuffix(file.Name(), "-DB") {
				db_files = append(db_files, file.Name())
			}
		}
		return nil
	})
	checkErr(err)

	// sort all the DBs (because e.x. 11-DB HAS to be at index 11 of conns array)
	sort.Slice(db_files, func(i, j int) bool {
		strings.Trim(db_files[i], "-DB")
		numA, _ := strconv.Atoi(strings.Trim(db_files[i], "-DB"))
		numB, _ := strconv.Atoi(strings.Trim(db_files[j], "-DB"))
		return numA < numB
	})

	// create connection objects to each DB
	conns := make([]*sql.DB, len(db_files))
	for idx := 0; idx < len(conns); idx ++ {
		conn, err := sql.Open("sqlite3", filepath.Join(db_path, db_files[idx]))
		checkErr(err)
		conns[idx] = conn
	}

	return &Store{
		num_db:		len(conns),
		db_path:	db_path,
		conns:		conns,
		cache_size:		cache_size,
		cache:	cache.NewLRUWithOptions(
			cache_size,
			&cache.Options{
				InitialCapacity: cache_size,
			}),
	}
}



func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (m *Store) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return []model.DependencyLink{}, nil
}

func (m *Store) WriteSpan(span *model.Span) error {
	return nil
}

// Helper function to convert timestamp from float64 to time.Time().
func convertFloat64ToTime(t float64) time.Time {
	// Get decimal part of the float, and convert it to nanoseconds
	temp := t - float64(int(t))
	temp = math.Round(temp * math.Pow(10, 6)) // round it to 6th decimal place, same as Unix time
	nsec := int64(temp * math.Pow(10, 3))
	return time.Unix(int64(t), nsec)
}

func (m *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	trace_id_high := traceID.High
	trace_id_low := traceID.Low

	// If trace exists in cache, return created one
	key := strconv.Itoa(int(trace_id_high)) + " " + strconv.Itoa(int(trace_id_low))
	exists := m.cache.Get(key)
	if exists != nil {	// Created trace before
		return exists.(*model.Trace), nil
	}

	// Determine which SQLite database to query
	db_idx := int(trace_id_high) % m.num_db

	// Get all the logs related to this traceID from SQL database and create Spans
	query := fmt.Sprintf("SELECT * FROM logs WHERE trace_id_high = %d AND trace_id_low = %d", trace_id_high, trace_id_low)
	rows, err := m.conns[db_idx].Query(query)
	checkErr(err)
	var traceid_high int
	var traceid_low int
	var parent_spanid int
	var spanid int
	var operation_name string
	var timestamp float64
	var log string

	// Use prev_span to keep track of if new Span should be created
	// Use prev_span_endtime to finish an old Span
	prev_span := &model.Span{}
	var prev_span_endtime time.Time
	spans := make([]*model.Span, 0)

	for rows.Next() {
		err := rows.Scan(&traceid_high, &traceid_low, &parent_spanid, &spanid, &operation_name, &timestamp, &log)
		checkErr(err)
		// Convert float64 time to time.Time()
		t := convertFloat64ToTime(timestamp)

		// Creating new Span
		if reflect.DeepEqual(*prev_span, (model.Span{})) || uint64(prev_span.SpanID) != uint64(spanid) {
			// Closing previous Span
			if !reflect.DeepEqual(*prev_span, (model.Span{})) {
				prev_span.Duration = prev_span_endtime.Sub(prev_span.StartTime)
			}
			// Configuring Flags and Tags
			flags := model.Flags(0)
			flags.SetSampled()
			tags := []model.KeyValue{
				// model.String("trace_id", strconv.Itoa(traceid_high) + " " + strconv.Itoa(traceid_low)),
				model.String("trace_id", key),
				model.String("span_id", strconv.Itoa(spanid)),
				model.String("log", t.String() + ":  " + log)}
			// Creating parent Span reference
			var references []model.SpanRef
			if parent_spanid != 0 {	// Normal span, set parent
				reference := model.SpanRef{
					TraceID: traceID,
					SpanID:  model.NewSpanID(uint64(parent_spanid)),
					RefType: model.ChildOf,
				}
				references = []model.SpanRef{reference}
			} else {	// Root span, no parent
				references = []model.SpanRef{}
			}
			// Creating Span struct
			span := &model.Span{
				TraceID:       traceID,
				SpanID:        model.NewSpanID(uint64(spanid)),
				Flags:         flags,
				OperationName: operation_name,
				References:    references,
				StartTime:     t,
				Tags:          tags,
				Process:       &model.Process{ServiceName: "logger"},
			}
			prev_span = span
			prev_span_endtime = t
			spans = append(spans, span)
		} else { // Adding log message as tag to the existing Span
			temp := model.String("log", t.String() + ":  " + log)
			prev_span.Tags = append(prev_span.Tags, temp)
			prev_span_endtime = t
		}
	}
	rows.Close()

	// Closing the last Span
	if !reflect.DeepEqual(*prev_span, (model.Span{})) {
		prev_span.Duration = prev_span_endtime.Sub(prev_span.StartTime)
	}

	trace := &model.Trace{
		Spans: spans,
	}
	m.cache.Put(key, trace)
	return trace, nil
}

func (m *Store) GetServices(ctx context.Context) ([]string, error) {
	return []string{"logger"}, nil
}

type void struct{}

func (m *Store) GetOperations(ctx context.Context, service string) ([]string, error) {
	if benchmark {
		defer trackExecutionTime("GetOperations")()
	}
	var toReturn []string
	operations := make(map[string]void)
	var val void
	for _, conn := range m.conns {
		rows, err := conn.Query("SELECT operation_name FROM ops")
		checkErr(err)
		var operation string
		for rows.Next() {
			err = rows.Scan(&operation)
			checkErr(err)
			operations[operation] = val
		}
		rows.Close()	
	}
	for k := range operations {
		toReturn = append(toReturn, k)
	}
	return toReturn, nil
}


func (m *Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if benchmark {
		defer trackExecutionTime("FindTraces")()
	}

	keys, err := m.FindTraceIDs(ctx, query)
	checkErr(err)
	var traces []*model.Trace
	// For each TraceID get the actual Trace
	for _, key := range keys {
		trace, err := m.GetTrace(ctx, key)
		checkErr(err)
		traces = append(traces, trace)
	}
	return traces, nil
}

// Helper function to create query statement
func addAndOrWhere(addAnd bool, buffer *bytes.Buffer) {
	if addAnd == true {
		buffer.WriteString(" AND ")
	} else {
		buffer.WriteString(" WHERE ")
	}
}

// Helper function to onvert timestamp from time.Time() to float64
func convertTimeToFloat64(t time.Time) float64 {
	return float64(t.Unix()) + (float64(t.Nanosecond()) / math.Pow(10, 9))
}


func (m *Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	if benchmark {
		defer trackExecutionTime("FindTraceIDs")()
	}

	if err := validateQuery(query); err != nil {
		return nil, err
	}
	// To return variable
	var trace_ids []model.TraceID

	var buffer bytes.Buffer
	addAnd := false
	useTime := false
	buffer.WriteString("SELECT DISTINCT trace_id_high, trace_id_low FROM logs")

	// Extrat tags. Valid tags: trace_id. TODO: add more tags
	if len(query.Tags) > 0 {
		for k, v := range query.Tags {
			if k == "trace_id" {
				// Parse high and low trace_id from v
				temp := strings.Fields(v)
				
				// Determines which db file to query
				high, _ := strconv.Atoi(temp[0])
				db_idx := high % m.num_db

				// Check if the trace_id actually exists in database
				query := "SELECT 1 FROM logs WHERE trace_id_high='" + temp[0] + "' AND trace_id_low='" + temp[1] + "'"
				rows, err := m.conns[db_idx].Query(query)
				checkErr(err)
				exists := rows.Next()
				if exists {
					low, _ := strconv.Atoi(temp[1])
					trace_ids = append(trace_ids, model.NewTraceID(uint64(high), uint64(low)))
				}
			}
			if k == "span_id" {
				addAndOrWhere(addAnd, &buffer)
				buffer.WriteString("my_id=")
				buffer.WriteString(v)
				addAnd = true
			}
			if k == "use_time" {
				// Use startime_min and starttime_max
				if v != "false" || v != "F" || v != "FALSE" || v != "False" {
					useTime = true
				}
			}
		}
	}

	// User specified valid trace_id(s), ignore other parameters
	if len(trace_ids) > 0 {
		return trace_ids, nil
	}

	// Extract operation name.
	if query.OperationName != "" {
		addAndOrWhere(addAnd, &buffer)
		buffer.WriteString("operation_name='")
		buffer.WriteString(query.OperationName)
		buffer.WriteString("'")
		addAnd = true
	}

	// Extract time.
	if useTime && !query.StartTimeMin.IsZero() {
		starttime_min := convertTimeToFloat64(query.StartTimeMin)
		addAndOrWhere(addAnd, &buffer)
		buffer.WriteString("time_stamp>")
		buffer.WriteString(strconv.FormatFloat(starttime_min, 'f', -1, 64))
		addAnd = true
	}

	if useTime && !query.StartTimeMax.IsZero() {
		starttime_max := convertTimeToFloat64(query.StartTimeMax)
		addAndOrWhere(addAnd, &buffer)
		buffer.WriteString("time_stamp<")
		buffer.WriteString(strconv.FormatFloat(starttime_max, 'f', -1, 64))
	}

	// Set num of traces to look up.
	var num_traces int
	if query.NumTraces == 0 {
		num_traces = defaultNumTraces
	} else {
		num_traces = query.NumTraces
	}
	
	// There are m.num_db number of databases to query from; therefore, num_traces
	// have to be distributed across the databases
	num := int(num_traces / m.num_db)
	last_num := int(num_traces / m.num_db) + num_traces % m.num_db

	// number of bytes accumulated so far, used for truncation later
	buffer.WriteString(" LIMIT ")
	length := buffer.Len()
	buffer.WriteString(strconv.Itoa(num))

	// fmt.Println("***************************")
	// fmt.Println(buffer.String())
	// fmt.Println("***************************")

	// Query and return TraceIDs
	for idx, conn := range m.conns {
		// for last query, use different query LIMIT, 
		// truncate everything starting from LIMIT
		if idx == (len(m.conns) - 1) {
			buffer.Truncate(length)
			buffer.WriteString(strconv.Itoa(last_num))
		}
		rows, err := conn.Query(buffer.String())
		checkErr(err)
		var trace_id_high int
		var trace_id_low int
		for rows.Next() {
			err = rows.Scan(&trace_id_high, &trace_id_low)
			checkErr(err)
			trace_ids = append(trace_ids, model.NewTraceID(uint64(trace_id_high), uint64(trace_id_low)))
		}
		rows.Close()
	}
	return trace_ids, nil
}

// validateQuery returns an error if certain restrictions are not met
func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}

	if p.ServiceName == "" && p.OperationName != "" {
		return ErrServiceNameNotSet
	}

	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}

	if !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	return nil
}
