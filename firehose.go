package analytics

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	uuid "github.com/hashicorp/go-uuid"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
)

// Event used for storage on disk.
type Event struct {
	Timestamp string                 `json:"ts"`    // Timestamp of the event
	Event     string                 `json:"event"` // Event name
	Body      map[string]interface{} `json:"body"`  // Body of the event
}

// Config struct
type Config struct {
	Session *session.Session // Session credentials for AWS
	Stream  string           // Stream we'll publish to on FH
	Prefix  string           // Prefix the events with a string
	Dir     string           // Dir we'll use. Defaults to stream name
	Log     log.Interface    // Log (optional)
}

func (c *Config) defaults() {
	if c.Log == nil {
		c.Log = log.Log
	}
}

// New Analytics instance
func New(config *Config) *Analytics {
	config.defaults()

	a := &Analytics{
		Config:  config,
		globals: Body{},
	}

	a.init()
	return a
}

// Analytics struct
type Analytics struct {
	*Config
	root       string
	userID     string
	eventsFile *os.File
	events     *json.Encoder
	globals    Body
}

// Initialize:
//
// - ~/<dir>
// - ~/<dir>/id
// - ~/<dir>/events
// - ~/<dir>/last_flush
//
func (a *Analytics) init() {
	if err := a.initRoot(); err != nil {
		a.Log.WithError(err).Error("couldn't create root")
		return
	}

	enabled, err := a.Enabled()
	if err != nil || !enabled {
		a.Log.Debug("disabled")
		return
	}

	a.initDir()
	a.initID()
	a.initEvents()
}

// init root directory.
func (a *Analytics) initRoot() error {
	dir := a.Dir
	if dir == "" {
		dir = a.Stream
	}

	root, err := getPath(dir)
	if err != nil {
		return err
	}
	a.root = root

	return nil
}

// init ~/<dir>.
func (a *Analytics) initDir() {
	os.Mkdir(a.root, 0755)
}

// init ~/<dir>/id.
func (a *Analytics) initID() {
	path := filepath.Join(a.root, "id")

	b, err := ioutil.ReadFile(path)
	if err == nil {
		a.userID = string(b)
		a.Log.Debug("id already created")
		return
	}

	a.Log.Debug("creating id")
	id, err := uuid.GenerateUUID()
	if err != nil {
		return
	}
	a.userID = string(id)

	err = ioutil.WriteFile(path, []byte(id), 0666)
	if err != nil {
		a.Log.WithError(err).Debug("error saving id")
		return
	}

	a.Touch()
}

// init ~/<dir>/events.
func (a *Analytics) initEvents() {
	path := filepath.Join(a.root, "events")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.WithError(err).Debug("error opening events")
		return
	}
	a.eventsFile = f

	a.events = json.NewEncoder(f)
}

// Enabled returns true if the user hasn't opted out.
func (a *Analytics) Enabled() (bool, error) {
	_, err := os.Stat(filepath.Join(a.root, "disable"))

	if os.IsNotExist(err) {
		return true, nil
	}

	return false, err
}

// Disable tracking. This method creates ~/<dir>/disable.
func (a *Analytics) Disable() error {
	a.Log.Debug("disable")
	_, err := os.Create(filepath.Join(a.root, "disable"))
	return err
}

// Enable tracking. This method removes ~/<dir>/disable.
func (a *Analytics) Enable() error {
	a.Log.Debug("enable")
	return os.Remove(filepath.Join(a.root, "disable"))
}

// Events reads the events from disk.
func (a *Analytics) Events() (v []*Event, err error) {
	f, err := os.Open(filepath.Join(a.root, "events"))
	if err != nil {
		return nil, errors.Wrap(err, "opening")
	}

	dec := json.NewDecoder(f)

	for {
		var e Event
		err := dec.Decode(&e)

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "decoding")
		}

		v = append(v, &e)
	}

	return v, nil
}

// Size returns the number of events.
func (a *Analytics) Size() (int, error) {
	events, err := a.Events()
	if err != nil {
		return 0, errors.Wrap(err, "reading events")
	}

	return len(events), nil
}

// Touch ~/<dir>/last_flush.
func (a *Analytics) Touch() error {
	path := filepath.Join(a.root, "last_flush")
	return ioutil.WriteFile(path, []byte(":)"), 0755)
}

// LastFlush returns the last flush time.
func (a *Analytics) LastFlush() (time.Time, error) {
	info, err := os.Stat(filepath.Join(a.root, "last_flush"))
	if err != nil {
		return time.Unix(0, 0), err
	}

	return info.ModTime(), nil
}

// LastFlushDuration returns the last flush time delta.
func (a *Analytics) LastFlushDuration() (time.Duration, error) {
	lastFlush, err := a.LastFlush()
	if err != nil {
		return 0, nil
	}

	return time.Now().Sub(lastFlush), nil
}

// Body are the meta data surrounding an event
type Body map[string]interface{}

// Set another field
func (f Body) Set(key string, value interface{}) Body {
	f[key] = value
	return f
}

// Body sets a field
func (a *Analytics) Body(key string, value interface{}) Body {
	body := Body{}
	body.Set(key, value)
	return body
}

// Set global fields included in every event
// This is not concurrency safe
func (a *Analytics) Set(body Body) {
	for k, v := range body {
		a.globals.Set(k, v)
	}
}

// Track event `name` with optional `data`.
func (a *Analytics) Track(name string, body Body) error {
	if a.events == nil {
		return nil
	}

	if body == nil {
		body = Body{}
	}

	// attach any globals
	for k, v := range a.globals {
		if body[k] == nil {
			body.Set(k, v)
		}
	}

	return a.events.Encode(&Event{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Event:     a.Config.Prefix + name,
		Body:      body,
	})
}

// MaybeFlush flushes if event count is above `aboveSize`, or age is `aboveDuration`,
// otherwise Close() is called and the underlying file(s) are closed.
func (a *Analytics) MaybeFlush(aboveSize int, aboveDuration time.Duration) error {
	age, err := a.LastFlushDuration()
	if err != nil {
		return err
	}

	size, err := a.Size()
	if err != nil {
		return err
	}

	ctx := a.Log.WithFields(log.Fields{
		"age":            age,
		"size":           size,
		"above_size":     aboveSize,
		"above_duration": aboveDuration,
	})

	switch {
	case size >= aboveSize:
		ctx.Debug("flush size")
		return a.Flush()
	case age >= aboveDuration:
		ctx.Debug("flush age")
		return a.Flush()
	default:
		return a.Close()
	}
}

// Flush the events to Segment, removing them from disk.
func (a *Analytics) Flush() error {
	// Ignore if we don't have a session
	if a.Session == nil {
		return nil
	} else if a.Stream == "" {
		return fmt.Errorf("missing stream name")
	}

	if err := a.Close(); err != nil {
		return errors.Wrap(err, "close error")
	}

	events, err := a.Events()
	if err != nil {
		return errors.Wrap(err, "reading events")
	} else if len(events) == 0 {
		return nil
	}

	var records []*firehose.Record
	for _, event := range events {
		record, err := json.Marshal(event)
		if err != nil {
			return errors.Wrapf(err, "marshal error")
		}
		records = append(records, &firehose.Record{Data: record})
	}

	// setup the firehose client
	fh := firehose.New(a.Session)
	retries := 3

retry:
	output, err := fh.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(a.Stream),
		Records:            records,
	})
	if err != nil {
		return errors.Wrap(err, "error sending records to firehose")
	} else if output.FailedPutCount != nil && *output.FailedPutCount > 0 {
		newRecords := []*firehose.Record{}
		for i, res := range output.RequestResponses {
			if res.ErrorCode != nil {
				newRecords = append(newRecords, records[i])
			}
		}
		records = newRecords
		retries--
		if retries > 0 {
			goto retry
		} else {
			return errors.Wrapf(err, "couldn't send all the records")
		}
	}

	if err := a.Touch(); err != nil {
		return errors.Wrap(err, "touching")
	}

	return os.Remove(filepath.Join(a.root, "events"))
}

// Close the underlying file descriptor(s).
func (a *Analytics) Close() error {
	return a.eventsFile.Close()
}

// get the path to the storage
func getPath(paths ...string) (p string, err error) {
	home, err := homedir.Dir()
	if err != nil {
		return p, err
	}

	switch runtime.GOOS {
	case "darwin":
		ps := append([]string{home, "Library", "Preferences"}, paths...)
		return path.Join(ps...), err
	case "linux":
		base := os.Getenv("XDG_CONFIG_HOME")
		if base == "" {
			base = path.Join(home, ".config")
		}
		ps := append([]string{base}, paths...)
		return path.Join(ps...), err
	case "windows":
		appdata := os.Getenv("LOCALAPPDATA")
		if appdata == "" {
			appdata = path.Join(home, "AppData", "Local")
		}
		ps := append([]string{appdata}, paths...)
		ps = append(ps, "Config")
		return path.Join(ps...), err
	default:
		return p, errors.New("store does not yet support " + runtime.GOOS + ". Please open a pull request!")
	}
}
