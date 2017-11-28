package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"golang.org/x/sys/unix"

	pagerduty "github.com/PagerDuty/go-pagerduty"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("pagerduty-dump-incidents",
		"Gather a list of incidents from the PagerDuty API and dump them to stdout as a stream of JSON objects.\n\n"+
			"Incidents are output in ascending chronological order.")
	apiKey := app.Flag("api-key",
		"PagerDuty API key.  May be read-only.  May alternatively be supplied using the PAGERDUTY_API_KEY environment variable.").
		Envar("PAGERDUTY_API_KEY").Required().String()
	sinceStamp := app.Flag("since",
		"Set the start of the query window.  Specified as a Go time duration or an RFC 3339 time stamp.  If specified as a Go time duration, the duration will be deducted from the current time.  Supply --until to set the end of the query window.").
		Default("1h").String()
	untilStamp := app.Flag("until",
		"Set the end of the query window.  Specified as a Go time duration or an RFC 3339 time stamp.  If specified as a Go time duration, the duration will be deducted from the current time.  Defaults to the current time.").
		PlaceHolder("RFC3339-STAMP").String()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	window, err := parseQueryWindow(*sinceStamp, *untilStamp)
	if err != nil {
		log.Fatalf("failed to parse query window: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, unix.SIGINT, unix.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case s := <-signals:
			log.Printf("%s received - terminating...", s)
			cancel()
		}
	}()

	var (
		incidents    = make(chan pagerduty.Incident, 10)
		producerDone = make(chan error)
		consumerDone = make(chan struct{})
		w            = bufio.NewWriter(os.Stdout)
	)
	go func() {
		defer close(incidents)
		defer close(producerDone)
		producerDone <- listIncidents(ctx, *apiKey, window, incidents)
	}()
	go func() {
		defer w.Flush()
		defer close(consumerDone)
		for {
			select {
			case i, ok := <-incidents:
				if !ok {
					return
				}
				b, err := json.MarshalIndent(i, "", "  ")
				if err != nil {
					log.Fatalf("failed to serialise incident: %v", err)
				}
				if _, err = w.Write(b); err != nil {
					log.Fatalf("write: %v", err)
				}
				if _, err = w.WriteString("\n"); err != nil {
					log.Fatalf("write: %v", err)
				}
			}
		}
	}()
	select {
	case err := <-producerDone:
		switch err {
		case nil:
		case context.Canceled:
		case context.DeadlineExceeded:
		default:
			log.Fatalf("failed to list incidents: %v", err)
		}
	}
	<-consumerDone
}

func listIncidents(ctx context.Context, apiKey string, window queryWindow, output chan<- pagerduty.Incident) error {
	c := pagerduty.NewClient(apiKey)
	opts := pagerduty.ListIncidentsOptions{
		APIListObject: pagerduty.APIListObject{
			Limit: 50,
		},
		Since:  window.Since.Format(time.RFC3339),
		Until:  window.Until.Format(time.RFC3339),
		SortBy: "created_at:asc",
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := c.ListIncidents(opts)
		if err != nil {
			return err
		}
		for _, i := range res.Incidents {
			output <- i
		}
		if !res.More {
			break
		}
		opts.Offset += uint(len(res.Incidents))
	}
	return nil
}

type queryWindow struct {
	Since time.Time
	Until time.Time
}

func parseQueryWindow(sinceStamp, untilStamp string) (queryWindow, error) {
	since, err := parseDurationTime(sinceStamp)
	if err != nil {
		return queryWindow{}, fmt.Errorf("since: %v", err)
	}
	until, err := parseDurationTime(untilStamp)
	if err != nil {
		return queryWindow{}, fmt.Errorf("until: %v", err)
	}
	if since.Equal(until) || since.After(until) {
		return queryWindow{}, errors.New("since must precede until")
	}
	return queryWindow{
		Since: since,
		Until: until,
	}, nil
}

func parseDurationTime(stamp string) (time.Time, error) {
	if stamp == "" {
		return time.Now().UTC(), nil
	}
	ago, derr := time.ParseDuration(stamp)
	if derr != nil {
		t, terr := time.Parse(time.RFC3339, stamp)
		if terr != nil {
			return time.Time{}, terr
		}
		return t, nil
	}
	t := time.Now().UTC().Add(-1 * ago)
	return t, nil
}
