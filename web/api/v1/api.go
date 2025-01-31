// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/dist"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
	"io/ioutil"
	

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/prometheus/prometheus/handler/targets"
)

const (
	namespace = "prometheus"
	subsystem = "api"
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

var remoteReadQueries = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: namespace,
	Subsystem: subsystem,
	Name:      "remote_read_queries",
	Help:      "The current number of remote read queries being executed or waiting.",
})

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type targetRetriever interface {
	TargetsActive() map[string][]*scrape.Target
	TargetsDropped() map[string][]*scrape.Target
}

type alertmanagerRetriever interface {
	Alertmanagers() []*url.URL
	DroppedAlertmanagers() []*url.URL
}

type rulesRetriever interface {
	RuleGroups() []*rules.Group
	AlertingRules() []*rules.AlertingRule
}

// PrometheusVersion contains build information about Prometheus.
type PrometheusVersion struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// RuntimeInfo contains runtime information about Prometheus.
type RuntimeInfo struct {
	StartTime           time.Time `json:"startTime"`
	CWD                 string    `json:"CWD"`
	ReloadConfigSuccess bool      `json:"reloadConfigSuccess"`
	LastConfigTime      time.Time `json:"lastConfigTime"`
	ChunkCount          int64     `json:"chunkCount"`
	TimeSeriesCount     int64     `json:"timeSeriesCount"`
	CorruptionCount     int64     `json:"corruptionCount"`
	GoroutineCount      int       `json:"goroutineCount"`
	GOMAXPROCS          int       `json:"GOMAXPROCS"`
	GOGC                string    `json:"GOGC"`
	GODEBUG             string    `json:"GODEBUG"`
	StorageRetention    string    `json:"storageRetention"`
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  storage.Warnings
	finalizer func()
}

type apiFunc func(r *http.Request) apiFuncResult

// TSDBAdmin defines the tsdb interfaces used by the v1 API for admin operations.
type TSDBAdmin interface {
	CleanTombstones() error
	Delete(mint, maxt int64, ms ...*labels.Matcher) error
	Dir() string
	Snapshot(dir string, withHead bool) error
	Head() *tsdb.Head
}

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	de *dist.DistEngine
	Queryable   storage.Queryable
	QueryEngine *promql.Engine

	targetRetriever       targetRetriever
	alertmanagerRetriever alertmanagerRetriever
	rulesRetriever        rulesRetriever
	now                   func() time.Time
	config                func() config.Config
	flagsMap              map[string]string
	ready                 func(http.HandlerFunc) http.HandlerFunc

	db                        func() TSDBAdmin
	enableAdmin               bool
	logger                    log.Logger
	remoteReadSampleLimit     int
	remoteReadMaxBytesInFrame int
	remoteReadGate            *gate.Gate
	CORSOrigin                *regexp.Regexp
	buildInfo                 *PrometheusVersion
	runtimeInfo               func() (RuntimeInfo, error)
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("promql.Point", marshalPointJSON, marshalPointJSONIsEmpty)
	prometheus.MustRegister(remoteReadQueries)
}

// NewAPI returns an initialized API type.
func NewAPI(
	de *dist.DistEngine,
	qe *promql.Engine,
	q storage.Queryable,
	tr targetRetriever,
	ar alertmanagerRetriever,
	configFunc func() config.Config,
	flagsMap map[string]string,
	readyFunc func(http.HandlerFunc) http.HandlerFunc,
	db func() TSDBAdmin,
	enableAdmin bool,
	logger log.Logger,
	rr rulesRetriever,
	remoteReadSampleLimit int,
	remoteReadConcurrencyLimit int,
	remoteReadMaxBytesInFrame int,
	CORSOrigin *regexp.Regexp,
	runtimeInfo func() (RuntimeInfo, error),
	buildInfo *PrometheusVersion,
) *API {
	return &API{
		de: de,
		QueryEngine:           qe,
		Queryable:             q,
		targetRetriever:       tr,
		alertmanagerRetriever: ar,

		now:                       time.Now,
		config:                    configFunc,
		flagsMap:                  flagsMap,
		ready:                     readyFunc,
		db:                        db,
		enableAdmin:               enableAdmin,
		rulesRetriever:            rr,
		remoteReadSampleLimit:     remoteReadSampleLimit,
		remoteReadGate:            gate.New(remoteReadConcurrencyLimit),
		remoteReadMaxBytesInFrame: remoteReadMaxBytesInFrame,
		logger:                    logger,
		CORSOrigin:                CORSOrigin,
		runtimeInfo:               runtimeInfo,
		buildInfo:                 buildInfo,
	}
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router) {
	wrap := func(f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			httputil.SetCORS(w, api.CORSOrigin, r)
			result := f(r)
			if result.err != nil {
				api.respondError(w, result.err, result.data)
			} else if result.data != nil {
				api.respond(w, result.data, result.warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			if result.finalizer != nil {
				result.finalizer()
			}
		})
		return api.ready(httputil.CompressionHandler{
			Handler: hf,
		}.ServeHTTP)
	}

	r.Options("/*path", wrap(api.options))

	r.Get("/query", wrap(api.query))
	r.Post("/query", wrap(api.query))
	r.Get("/query_range", wrap(api.queryRange))
	r.Post("/query_range", wrap(api.queryRange))

	r.Get("/labels", wrap(api.labelNames))
	r.Post("/labels", wrap(api.labelNames))
	r.Get("/label/:name/values", wrap(api.labelValues))

	r.Get("/series", wrap(api.series))
	r.Post("/series", wrap(api.series))
	r.Del("/series", wrap(api.dropSeries))

	r.Get("/targets", wrap(api.targets))
	r.Get("/targets/metadata", wrap(api.targetMetadata))
	r.Get("/alertmanagers", wrap(api.alertmanagers))

	r.Get("/status/config", wrap(api.serveConfig))
	r.Get("/status/runtimeinfo", wrap(api.serveRuntimeInfo))
	r.Get("/status/buildinfo", wrap(api.serveBuildInfo))
	r.Get("/status/flags", wrap(api.serveFlags))
	r.Get("/status/tsdb", wrap(api.serveTSDBStatus))
	r.Post("/read", api.ready(http.HandlerFunc(api.remoteRead)))

	r.Get("/alerts", wrap(api.alerts))
	r.Get("/rules", wrap(api.rules))

	// Admin APIs
	r.Post("/admin/tsdb/delete_series", wrap(api.deleteSeries))
	r.Post("/admin/tsdb/clean_tombstones", wrap(api.cleanTombstones))
	r.Post("/admin/tsdb/snapshot", wrap(api.snapshot))

	r.Post("/admin/targets/add_target", wrap(api.addTarget))

	r.Put("/admin/tsdb/delete_series", wrap(api.deleteSeries))
	r.Put("/admin/tsdb/clean_tombstones", wrap(api.cleanTombstones))
	r.Put("/admin/tsdb/snapshot", wrap(api.snapshot))

}

type queryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

func (api *API) options(r *http.Request) apiFuncResult {
	return apiFuncResult{nil, nil, nil, nil}
}

func (api *API) query(r *http.Request) apiFuncResult {
	level.Info(api.logger).Log("msg", fmt.Sprintf("Handling client query %v",r.URL.String()))
	if strings.Contains(r.URL.String(), "id%3D") {
		// find id
		id_start_index := strings.Index(r.URL.String(), "id%3D%22")
		id_start_index = id_start_index + 8
		id_end_index := id_start_index + 8
		source_id := r.URL.String()[id_start_index:id_end_index]
		level.Info(api.logger).Log("msg", fmt.Sprintf("Looking up source ID %v", source_id))

		lookedup_node := api.de.LookupGuid(source_id)
		level.Info(api.logger).Log("msg", fmt.Sprintf("Looked up node %+v", lookedup_node))
		local_id := api.de.GetLocalId()

		if lookedup_node.Guuid != local_id{
			level.Info(api.logger).Log("msg", "Looked up remote node")
			// lookup if its not remote do nothing else get the ip and port
			socket_ip_of_prom := lookedup_node.Address
			hostname := strings.Split(socket_ip_of_prom, ":")[0]
			hostname = "http://"+hostname+":9090/api/v1"
			new_url := hostname+r.URL.String()
			level.Info(api.logger).Log("msg", fmt.Sprintf("Redirecting query to URL %v", new_url))
			query_response, err := http.Get(new_url)
			if err != nil {
				level.Warn(api.logger).Log("msg", "get request not successfull", "err", err)
			}

			defer query_response.Body.Close()

			query_body, err := ioutil.ReadAll(query_response.Body)
			if err != nil {
				level.Warn(api.logger).Log("msg", "get response not successfull", "err", err)
			}

			var objs response

			json := jsoniter.ConfigCompatibleWithStandardLibrary
			err = json.Unmarshal(query_body, &objs)
			if err != nil {
				level.Warn(api.logger).Log("msg", "could not unmarshal", "err", err)
			}
			//level.Warn(api.logger).Log("msg", "successfull", "stat", objs)
			fmt.Printf("%+v\n", objs)

			level.Info(api.logger).Log("msg", "Returning remote response")
			var s_warnings storage.Warnings
			for _,rw := range objs.Warnings {
				s_warnings = append(s_warnings, errors.New(rw))
			} 
			//return apiFuncResult{&objs.Data,
			//	&apiError{objs.ErrorType, errors.New(objs.Error)},
			//	s_warnings,
			//	nil}

			if ((objs.Error != "") && len(objs.Warnings) < 1){
				return apiFuncResult{nil,
					&apiError{objs.ErrorType, errors.New(objs.Error)},
					nil,
					nil}
			}

			if ((objs.Error == "") && len(objs.Warnings) > 0) {
				return apiFuncResult{&objs.Data,
					nil,
					s_warnings,
					nil}
			}

			if ((objs.Error != "") && len(objs.Warnings) > 0){
				return apiFuncResult{nil,
					&apiError{objs.ErrorType, errors.New(objs.Error)},
					s_warnings,
					nil}
			}

			//TODO : check error and warnings based on unmarshalled objs.ErrorType, objs.Error and objs.Warnings
			return apiFuncResult{&objs.Data,
				nil,
				nil,
				nil}
			
		}
	}
	
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			err = errors.Wrapf(err, "invalid parameter 'time'")
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	} else {
		ts = api.now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			err = errors.Wrapf(err, "invalid parameter 'timeout'")
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := api.QueryEngine.NewInstantQuery(api.Queryable, r.FormValue("query"), ts)
	if err != nil {
		err = errors.Wrapf(err, "invalid parameter 'query'")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func (api *API) queryRange(r *http.Request) apiFuncResult {
	level.Info(api.logger).Log("msg", "Handling query range from client")
	if strings.Contains(r.URL.String(), "id=") {
		// find id
		
		id_start_index := strings.Index(r.URL.String(), "id=")
		id_start_index = id_start_index + 6
		id_end_index := id_start_index + 8
		source_id := r.URL.String()[id_start_index:id_end_index]
		//level.Warn(api.logger).Log("msg", "checking of match", "err", source_id)

		lookedup_node := api.de.LookupGuid(source_id)
		local_id := api.de.GetLocalId()

		if lookedup_node.Guuid != local_id{
	
			// lookup if its not remote do nothing else get the ip and port
			socket_ip_of_prom := lookedup_node.Address
			hostname := strings.Split(socket_ip_of_prom, ":")[0]
			hostname = "http://"+hostname+":9090/api/v1"
			new_url := hostname+r.URL.String()
			
			query_response, err := http.Get(new_url)
			if err != nil {
				level.Warn(api.logger).Log("msg", "get request not successfull", "err", err)
			}

			defer query_response.Body.Close()

			query_body, err := ioutil.ReadAll(query_response.Body)
			if err != nil {
				level.Warn(api.logger).Log("msg", "get response not successfull", "err", err)
			}

			var objs response

			json := jsoniter.ConfigCompatibleWithStandardLibrary
			err = json.Unmarshal(query_body, &objs)
			if err != nil {
				level.Warn(api.logger).Log("msg", "could not unmarshal", "err", err)
			}
			//level.Warn(api.logger).Log("msg", "successfull", "stat", objs)
			fmt.Printf("%+v", objs)
			var s_warnings storage.Warnings
			for _,rw := range objs.Warnings {
				s_warnings = append(s_warnings, errors.New(rw))
			} 
			return apiFuncResult{objs.Data, &apiError{objs.ErrorType, errors.New(objs.Error)}, s_warnings, nil}
			
		}
	}
	
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		err = errors.Wrapf(err, "invalid parameter 'start'")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		err = errors.Wrapf(err, "invalid parameter 'end'")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		err = errors.Wrapf(err, "invalid parameter 'step'")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	// For safety limit the number of returned points per timeseries.
	// This is sufficient for 60sec resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			err = errors.Wrap(err, "invalid parameter 'timeout'")
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := api.QueryEngine.NewRangeQuery(api.Queryable, r.FormValue("query"), start, end, step)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	switch err.(type) {
	case promql.ErrQueryCanceled:
		return &apiError{errorCanceled, err}
	case promql.ErrQueryTimeout:
		return &apiError{errorTimeout, err}
	case promql.ErrStorage:
		return &apiError{errorInternal, err}
	}

	return &apiError{errorExec, err}
}

func (api *API) labelNames(r *http.Request) apiFuncResult {
	q, err := api.Queryable.Querier(r.Context(), math.MinInt64, math.MaxInt64)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	defer q.Close()

	names, warnings, err := q.LabelNames()
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
	}
	return apiFuncResult{names, nil, warnings, nil}
}

func (api *API) labelValues(r *http.Request) apiFuncResult {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Errorf("invalid label name: %q", name)}, nil, nil}
	}
	q, err := api.Queryable.Querier(ctx, math.MinInt64, math.MaxInt64)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	defer q.Close()

	vals, warnings, err := q.LabelValues(name)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
	}

	return apiFuncResult{vals, nil, warnings, nil}
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func (api *API) series(r *http.Request) apiFuncResult {
	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Wrapf(err, "error parsing form values")}, nil, nil}
	}
	if len(r.Form["match[]"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no match[] parameter provided")}, nil, nil}
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	} else {
		start = minTime
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	} else {
		end = maxTime
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
		matcherSets = append(matcherSets, matchers)
	}

	q, err := api.Queryable.Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	defer q.Close()

	var sets []storage.SeriesSet
	var warnings storage.Warnings
	for _, mset := range matcherSets {
		s, wrn, err := q.Select(nil, mset...) //TODO
		warnings = append(warnings, wrn...)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
		}
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets, nil)
	metrics := []labels.Labels{}
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return apiFuncResult{nil, &apiError{errorExec, set.Err()}, warnings, nil}
	}
	return apiFuncResult{metrics, nil, warnings, nil}
}

func (api *API) dropSeries(r *http.Request) apiFuncResult {
	return apiFuncResult{nil, &apiError{errorInternal, errors.New("not implemented")}, nil, nil}
}

// Target has the information for one target.
type Target struct {
	// Labels before any processing.
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
	// Any labels that are added to this target and its metrics.
	Labels map[string]string `json:"labels"`

	ScrapePool string `json:"scrapePool"`
	ScrapeURL  string `json:"scrapeUrl"`

	LastError          string              `json:"lastError"`
	LastScrape         time.Time           `json:"lastScrape"`
	LastScrapeDuration float64             `json:"lastScrapeDuration"`
	Health             scrape.TargetHealth `json:"health"`
}

// DroppedTarget has the information for one target that was dropped during relabelling.
type DroppedTarget struct {
	// Labels before any processing.
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
}

// TargetDiscovery has all the active targets.
type TargetDiscovery struct {
	ActiveTargets  []*Target        `json:"activeTargets"`
	DroppedTargets []*DroppedTarget `json:"droppedTargets"`
}

func (api *API) targets(r *http.Request) apiFuncResult {
	sortKeys := func(targets map[string][]*scrape.Target) ([]string, int) {
		var n int
		keys := make([]string, 0, len(targets))
		for k := range targets {
			keys = append(keys, k)
			n += len(targets[k])
		}
		sort.Strings(keys)
		return keys, n
	}

	flatten := func(targets map[string][]*scrape.Target) []*scrape.Target {
		keys, n := sortKeys(targets)
		res := make([]*scrape.Target, 0, n)
		for _, k := range keys {
			res = append(res, targets[k]...)
		}
		return res
	}

	state := strings.ToLower(r.URL.Query().Get("state"))
	showActive := state == "" || state == "any" || state == "active"
	showDropped := state == "" || state == "any" || state == "dropped"
	res := &TargetDiscovery{}

	if showActive {
		targetsActive := api.targetRetriever.TargetsActive()
		activeKeys, numTargets := sortKeys(targetsActive)
		res.ActiveTargets = make([]*Target, 0, numTargets)

		for _, key := range activeKeys {
			for _, target := range targetsActive[key] {
				lastErrStr := ""
				lastErr := target.LastError()
				if lastErr != nil {
					lastErrStr = lastErr.Error()
				}

				res.ActiveTargets = append(res.ActiveTargets, &Target{
					DiscoveredLabels:   target.DiscoveredLabels().Map(),
					Labels:             target.Labels().Map(),
					ScrapePool:         key,
					ScrapeURL:          target.URL().String(),
					LastError:          lastErrStr,
					LastScrape:         target.LastScrape(),
					LastScrapeDuration: target.LastScrapeDuration().Seconds(),
					Health:             target.Health(),
				})
			}
		}
	} else {
		res.ActiveTargets = []*Target{}
	}
	if showDropped {
		tDropped := flatten(api.targetRetriever.TargetsDropped())
		res.DroppedTargets = make([]*DroppedTarget, 0, len(tDropped))
		for _, t := range tDropped {
			res.DroppedTargets = append(res.DroppedTargets, &DroppedTarget{
				DiscoveredLabels: t.DiscoveredLabels().Map(),
			})
		}
	} else {
		res.DroppedTargets = []*DroppedTarget{}
	}
	return apiFuncResult{res, nil, nil, nil}
}

func matchLabels(lset labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (api *API) targetMetadata(r *http.Request) apiFuncResult {
	limit := -1
	if s := r.FormValue("limit"); s != "" {
		var err error
		if limit, err = strconv.Atoi(s); err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, errors.New("limit must be a number")}, nil, nil}
		}
	}

	matchTarget := r.FormValue("match_target")

	var matchers []*labels.Matcher

	var err error

	if matchTarget != "" {
		matchers, err = promql.ParseMetricSelector(matchTarget)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	}

	metric := r.FormValue("metric")

	var res []metricMetadata
	for _, tt := range api.targetRetriever.TargetsActive() {
		for _, t := range tt {
			if limit >= 0 && len(res) >= limit {
				break
			}
			// Filter targets that don't satisfy the label matchers.
			if matchTarget != "" && !matchLabels(t.Labels(), matchers) {
				continue
			}
			// If no metric is specified, get the full list for the target.
			if metric == "" {
				for _, md := range t.MetadataList() {
					res = append(res, metricMetadata{
						Target: t.Labels(),
						Metric: md.Metric,
						Type:   md.Type,
						Help:   md.Help,
						Unit:   md.Unit,
					})
				}
				continue
			}
			// Get metadata for the specified metric.
			if md, ok := t.Metadata(metric); ok {
				res = append(res, metricMetadata{
					Target: t.Labels(),
					Type:   md.Type,
					Help:   md.Help,
					Unit:   md.Unit,
				})
			}
		}
	}
	if len(res) == 0 {
		return apiFuncResult{nil, &apiError{errorNotFound, errors.New("specified metadata not found")}, nil, nil}
	}
	return apiFuncResult{res, nil, nil, nil}
}

type metricMetadata struct {
	Target labels.Labels        `json:"target"`
	Metric string               `json:"metric,omitempty"`
	Type   textparse.MetricType `json:"type"`
	Help   string               `json:"help"`
	Unit   string               `json:"unit"`
}

// AlertmanagerDiscovery has all the active Alertmanagers.
type AlertmanagerDiscovery struct {
	ActiveAlertmanagers  []*AlertmanagerTarget `json:"activeAlertmanagers"`
	DroppedAlertmanagers []*AlertmanagerTarget `json:"droppedAlertmanagers"`
}

// AlertmanagerTarget has info on one AM.
type AlertmanagerTarget struct {
	URL string `json:"url"`
}

func (api *API) alertmanagers(r *http.Request) apiFuncResult {
	urls := api.alertmanagerRetriever.Alertmanagers()
	droppedURLS := api.alertmanagerRetriever.DroppedAlertmanagers()
	ams := &AlertmanagerDiscovery{ActiveAlertmanagers: make([]*AlertmanagerTarget, len(urls)), DroppedAlertmanagers: make([]*AlertmanagerTarget, len(droppedURLS))}
	for i, url := range urls {
		ams.ActiveAlertmanagers[i] = &AlertmanagerTarget{URL: url.String()}
	}
	for i, url := range droppedURLS {
		ams.DroppedAlertmanagers[i] = &AlertmanagerTarget{URL: url.String()}
	}
	return apiFuncResult{ams, nil, nil, nil}
}

// AlertDiscovery has info for all active alerts.
type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

// Alert has info for an alert.
type Alert struct {
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	State       string        `json:"state"`
	ActiveAt    *time.Time    `json:"activeAt,omitempty"`
	Value       string        `json:"value"`
}

func (api *API) alerts(r *http.Request) apiFuncResult {
	alertingRules := api.rulesRetriever.AlertingRules()
	alerts := []*Alert{}

	for _, alertingRule := range alertingRules {
		alerts = append(
			alerts,
			rulesAlertsToAPIAlerts(alertingRule.ActiveAlerts())...,
		)
	}

	res := &AlertDiscovery{Alerts: alerts}

	return apiFuncResult{res, nil, nil, nil}
}

func rulesAlertsToAPIAlerts(rulesAlerts []*rules.Alert) []*Alert {
	apiAlerts := make([]*Alert, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &Alert{
			Labels:      ruleAlert.Labels,
			Annotations: ruleAlert.Annotations,
			State:       ruleAlert.State.String(),
			ActiveAt:    &ruleAlert.ActiveAt,
			Value:       strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}

	return apiAlerts
}

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

// RuleGroup has info for rules which are part of a group
type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules    []rule  `json:"rules"`
	Interval float64 `json:"interval"`
}

type rule interface{}

type alertingRule struct {
	Name        string           `json:"name"`
	Query       string           `json:"query"`
	Duration    float64          `json:"duration"`
	Labels      labels.Labels    `json:"labels"`
	Annotations labels.Labels    `json:"annotations"`
	Alerts      []*Alert         `json:"alerts"`
	Health      rules.RuleHealth `json:"health"`
	LastError   string           `json:"lastError,omitempty"`
	// Type of an alertingRule is always "alerting".
	Type string `json:"type"`
}

type recordingRule struct {
	Name      string           `json:"name"`
	Query     string           `json:"query"`
	Labels    labels.Labels    `json:"labels,omitempty"`
	Health    rules.RuleHealth `json:"health"`
	LastError string           `json:"lastError,omitempty"`
	// Type of a recordingRule is always "recording".
	Type string `json:"type"`
}

func (api *API) rules(r *http.Request) apiFuncResult {
	ruleGroups := api.rulesRetriever.RuleGroups()
	res := &RuleDiscovery{RuleGroups: make([]*RuleGroup, len(ruleGroups))}
	for i, grp := range ruleGroups {
		apiRuleGroup := &RuleGroup{
			Name:     grp.Name(),
			File:     grp.File(),
			Interval: grp.Interval().Seconds(),
			Rules:    []rule{},
		}

		for _, r := range grp.Rules() {
			var enrichedRule rule

			lastError := ""
			if r.LastError() != nil {
				lastError = r.LastError().Error()
			}

			switch rule := r.(type) {
			case *rules.AlertingRule:
				enrichedRule = alertingRule{
					Name:        rule.Name(),
					Query:       rule.Query().String(),
					Duration:    rule.Duration().Seconds(),
					Labels:      rule.Labels(),
					Annotations: rule.Annotations(),
					Alerts:      rulesAlertsToAPIAlerts(rule.ActiveAlerts()),
					Health:      rule.Health(),
					LastError:   lastError,
					Type:        "alerting",
				}
			case *rules.RecordingRule:
				enrichedRule = recordingRule{
					Name:      rule.Name(),
					Query:     rule.Query().String(),
					Labels:    rule.Labels(),
					Health:    rule.Health(),
					LastError: lastError,
					Type:      "recording",
				}
			default:
				err := errors.Errorf("failed to assert type of rule '%v'", rule.Name())
				return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
			}

			apiRuleGroup.Rules = append(apiRuleGroup.Rules, enrichedRule)
		}
		res.RuleGroups[i] = apiRuleGroup
	}
	return apiFuncResult{res, nil, nil, nil}
}

type prometheusConfig struct {
	YAML string `json:"yaml"`
}

func (api *API) serveRuntimeInfo(r *http.Request) apiFuncResult {
	status, err := api.runtimeInfo()
	if err != nil {
		return apiFuncResult{status, &apiError{errorInternal, err}, nil, nil}
	}
	return apiFuncResult{status, nil, nil, nil}
}

func (api *API) serveBuildInfo(r *http.Request) apiFuncResult {
	return apiFuncResult{api.buildInfo, nil, nil, nil}
}

func (api *API) serveConfig(r *http.Request) apiFuncResult {
	cfg := &prometheusConfig{
		YAML: api.config().String(),
	}
	return apiFuncResult{cfg, nil, nil, nil}
}

func (api *API) serveFlags(r *http.Request) apiFuncResult {
	return apiFuncResult{api.flagsMap, nil, nil, nil}
}

// stat holds the information about individual cardinality.
type stat struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

// tsdbStatus has information of cardinality statistics from postings.
type tsdbStatus struct {
	SeriesCountByMetricName     []stat `json:"seriesCountByMetricName"`
	LabelValueCountByLabelName  []stat `json:"labelValueCountByLabelName"`
	MemoryInBytesByLabelName    []stat `json:"memoryInBytesByLabelName"`
	SeriesCountByLabelValuePair []stat `json:"seriesCountByLabelValuePair"`
}

func (api *API) serveTSDBStatus(r *http.Request) apiFuncResult {
	db := api.db()
	if db == nil {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("TSDB not ready")}, nil, nil}
	}
	convert := func(stats []index.Stat) []stat {
		result := make([]stat, 0, len(stats))
		for _, item := range stats {
			item := stat{Name: item.Name, Value: item.Count}
			result = append(result, item)
		}
		return result
	}

	posting := db.Head().PostingsCardinalityStats(model.MetricNameLabel)
	response := tsdbStatus{
		SeriesCountByMetricName:     convert(posting.CardinalityMetricsStats),
		LabelValueCountByLabelName:  convert(posting.CardinalityLabelStats),
		MemoryInBytesByLabelName:    convert(posting.LabelValueStats),
		SeriesCountByLabelValuePair: convert(posting.LabelValuePairsStats),
	}

	return apiFuncResult{response, nil, nil, nil}
}

func (api *API) remoteRead(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := api.remoteReadGate.Start(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	remoteReadQueries.Inc()

	defer api.remoteReadGate.Done()
	defer remoteReadQueries.Dec()

	req, err := remote.DecodeReadRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	externalLabels := api.config().GlobalConfig.ExternalLabels.Map()

	sortedExternalLabels := make([]prompb.Label, 0, len(externalLabels))
	for name, value := range externalLabels {
		sortedExternalLabels = append(sortedExternalLabels, prompb.Label{
			Name:  string(name),
			Value: string(value),
		})
	}
	sort.Slice(sortedExternalLabels, func(i, j int) bool {
		return sortedExternalLabels[i].Name < sortedExternalLabels[j].Name
	})

	responseType, err := remote.NegotiateResponseType(req.AcceptedResponseTypes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch responseType {
	case prompb.ReadRequest_STREAMED_XOR_CHUNKS:
		w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "internal http.ResponseWriter does not implement http.Flusher interface", http.StatusInternalServerError)
			return
		}
		for i, query := range req.Queries {
			err := api.remoteReadQuery(ctx, query, externalLabels, func(set storage.SeriesSet) error {

				return remote.StreamChunkedReadResponses(
					remote.NewChunkedWriter(w, f),
					int64(i),
					set,
					sortedExternalLabels,
					api.remoteReadMaxBytesInFrame,
				)
			})
			if err != nil {
				if httpErr, ok := err.(remote.HTTPError); ok {
					http.Error(w, httpErr.Error(), httpErr.Status())
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	default:
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		// On empty or unknown types in req.AcceptedResponseTypes we default to non streamed, raw samples response.
		resp := prompb.ReadResponse{
			Results: make([]*prompb.QueryResult, len(req.Queries)),
		}
		for i, query := range req.Queries {
			err := api.remoteReadQuery(ctx, query, externalLabels, func(set storage.SeriesSet) error {

				resp.Results[i], err = remote.ToQueryResult(set, api.remoteReadSampleLimit)
				if err != nil {
					return err
				}

				for _, ts := range resp.Results[i].Timeseries {
					ts.Labels = remote.MergeLabels(ts.Labels, sortedExternalLabels)
				}
				return nil
			})
			if err != nil {
				if httpErr, ok := err.(remote.HTTPError); ok {
					http.Error(w, httpErr.Error(), httpErr.Status())
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		if err := remote.EncodeReadResponse(&resp, w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// filterExtLabelsFromMatchers change equality matchers which match external labels
// to a matcher that looks for an empty label,
// as that label should not be present in the storage.
func filterExtLabelsFromMatchers(pbMatchers []*prompb.LabelMatcher, externalLabels map[string]string) ([]*labels.Matcher, error) {
	matchers, err := remote.FromLabelMatchers(pbMatchers)
	if err != nil {
		return nil, err
	}

	filteredMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		value := externalLabels[m.Name]
		if m.Type == labels.MatchEqual && value == m.Value {
			matcher, err := labels.NewMatcher(labels.MatchEqual, m.Name, "")
			if err != nil {
				return nil, err
			}
			filteredMatchers = append(filteredMatchers, matcher)
		} else {
			filteredMatchers = append(filteredMatchers, m)
		}
	}

	return filteredMatchers, nil
}

func (api *API) remoteReadQuery(ctx context.Context, query *prompb.Query, externalLabels map[string]string, seriesHandleFn func(set storage.SeriesSet) error) error {
	filteredMatchers, err := filterExtLabelsFromMatchers(query.Matchers, externalLabels)
	if err != nil {
		return err
	}

	querier, err := api.Queryable.Querier(ctx, query.StartTimestampMs, query.EndTimestampMs)
	if err != nil {
		return err
	}

	var selectParams *storage.SelectParams
	if query.Hints != nil {
		selectParams = &storage.SelectParams{
			Start: query.Hints.StartMs,
			End:   query.Hints.EndMs,
			Step:  query.Hints.StepMs,
			Func:  query.Hints.Func,
		}
	}

	defer func() {
		if err := querier.Close(); err != nil {
			level.Warn(api.logger).Log("msg", "error on querier close", "err", err.Error())
		}
	}()

	set, _, err := querier.Select(selectParams, filteredMatchers...)
	if err != nil {
		return err
	}
	return seriesHandleFn(set)
}

func (api *API) addTarget(r *http.Request) apiFuncResult {
	if !api.enableAdmin {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("admin APIs disabled")}, nil, nil}
	}

	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Wrap(err, "error parsing form values")}, nil, nil}
	}

	if len(r.Form["id"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no id parameter provided")}, nil, nil}
	}

	if len(r.Form["url"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no url parameter provided")}, nil, nil}
	}

	target_id := r.FormValue("id")
	target_url := r.FormValue("url")
	
	path_of_yml_file := api.flagsMap["config.file"]
	dir_for_target := filepath.Dir(path_of_yml_file)
	dir_for_target = dir_for_target

	// lookup goes here
	lookedup_node := api.de.LookupGuid(target_id)
	local_id := api.de.GetLocalId()

	if local_id == lookedup_node.Guuid {
		// add config here
		// addTarget function: curl -X POST -g 'http://localhost:9090/api/v1/admin/targets/add_target?id=ABCDEF1&url=localhost:8082'
		if !targets.AddTargetToConfig(target_id, target_url, dir_for_target) {
			return apiFuncResult{nil, &apiError{errorBadData, errors.New("could not add target")}, nil, nil}
		}	
	}else {
		// add config on the remote peer lookedup_node
		api.de.SendSourceAddToPeer(target_id, target_url, lookedup_node.Address)
	}

	return apiFuncResult{struct {
			TargetId string `json:"id"`
			TargetUrl string `json:"url"`
	}{target_id, target_url}, nil, nil, nil}
}

func (api *API) deleteSeries(r *http.Request) apiFuncResult {
	if !api.enableAdmin {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("admin APIs disabled")}, nil, nil}
	}
	db := api.db()
	if db == nil {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("TSDB not ready")}, nil, nil}
	}

	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Wrap(err, "error parsing form values")}, nil, nil}
	}
	if len(r.Form["match[]"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no match[] parameter provided")}, nil, nil}
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	} else {
		start = minTime
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}
	} else {
		end = maxTime
	}

	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		if err := db.Delete(timestamp.FromTime(start), timestamp.FromTime(end), matchers...); err != nil {
			return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
		}
	}

	return apiFuncResult{nil, nil, nil, nil}
}

func (api *API) snapshot(r *http.Request) apiFuncResult {
	if !api.enableAdmin {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("admin APIs disabled")}, nil, nil}
	}
	var (
		skipHead bool
		err      error
	)
	if r.FormValue("skip_head") != "" {
		skipHead, err = strconv.ParseBool(r.FormValue("skip_head"))
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, errors.Wrapf(err, "unable to parse boolean 'skip_head' argument")}, nil, nil}
		}
	}

	db := api.db()
	if db == nil {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("TSDB not ready")}, nil, nil}
	}

	var (
		snapdir = filepath.Join(db.Dir(), "snapshots")
		name    = fmt.Sprintf("%s-%x",
			time.Now().UTC().Format("20060102T150405Z0700"),
			rand.Int())
		dir = filepath.Join(snapdir, name)
	)
	if err := os.MkdirAll(dir, 0777); err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errors.Wrap(err, "create snapshot directory")}, nil, nil}
	}
	if err := db.Snapshot(dir, !skipHead); err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errors.Wrap(err, "create snapshot")}, nil, nil}
	}

	return apiFuncResult{struct {
		Name string `json:"name"`
	}{name}, nil, nil, nil}
}

func (api *API) cleanTombstones(r *http.Request) apiFuncResult {
	if !api.enableAdmin {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("admin APIs disabled")}, nil, nil}
	}
	db := api.db()
	if db == nil {
		return apiFuncResult{nil, &apiError{errorUnavailable, errors.New("TSDB not ready")}, nil, nil}
	}

	if err := db.CleanTombstones(); err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}

	return apiFuncResult{nil, nil, nil, nil}
}

func (api *API) respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func (api *API) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})

	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}

func marshalPointJSON(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	p := *((*promql.Point)(ptr))
	stream.WriteArrayStart()
	// Write out the timestamp as a float divided by 1000.
	// This is ~3x faster than converting to a float.
	t := p.T
	if t < 0 {
		stream.WriteRaw(`-`)
		t = -t
	}
	stream.WriteInt64(t / 1000)
	fraction := t % 1000
	if fraction != 0 {
		stream.WriteRaw(`.`)
		if fraction < 100 {
			stream.WriteRaw(`0`)
		}
		if fraction < 10 {
			stream.WriteRaw(`0`)
		}
		stream.WriteInt64(fraction)
	}
	stream.WriteMore()
	stream.WriteRaw(`"`)

	// Taken from https://github.com/json-iterator/go/blob/master/stream_float.go#L71 as a workaround
	// to https://github.com/json-iterator/go/issues/365 (jsoniter, to follow json standard, doesn't allow inf/nan).
	buf := stream.Buffer()
	abs := math.Abs(p.V)
	fmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	buf = strconv.AppendFloat(buf, p.V, fmt, -1, 64)
	stream.SetBuffer(buf)

	stream.WriteRaw(`"`)
	stream.WriteArrayEnd()
}

func marshalPointJSONIsEmpty(ptr unsafe.Pointer) bool {
	return false
}
