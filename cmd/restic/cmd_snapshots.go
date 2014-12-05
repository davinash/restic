package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/restic/restic"
	"github.com/restic/restic/backend"
)

const (
	minute = 60
	hour   = 60 * minute
	day    = 24 * hour
	week   = 7 * day
)

type Table struct {
	Header string
	Rows   [][]interface{}

	RowFormat string
}

func NewTable() Table {
	return Table{
		Rows: [][]interface{}{},
	}
}

func (t Table) Print(w io.Writer) error {
	_, err := fmt.Fprintln(w, t.Header)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, strings.Repeat("-", 70))
	if err != nil {
		return err
	}

	for _, row := range t.Rows {
		_, err = fmt.Fprintf(w, t.RowFormat+"\n", row...)
		if err != nil {
			return err
		}
	}

	return nil
}

const TimeFormat = "2006-01-02 15:04:05"

func reltime(t time.Time) string {
	sec := uint64(time.Since(t).Seconds())

	switch {
	case sec > week:
		return t.Format(TimeFormat)
	case sec > day:
		return fmt.Sprintf("%d days ago", sec/day)
	case sec > hour:
		return fmt.Sprintf("%d hours ago", sec/hour)
	case sec > minute:
		return fmt.Sprintf("%d minutes ago", sec/minute)
	default:
		return fmt.Sprintf("%d seconds ago", sec)
	}
}

func init() {
	commands["snapshots"] = commandSnapshots
}

func commandSnapshots(be backend.Server, key *restic.Key, args []string) error {
	if len(args) != 0 {
		return errors.New("usage: snapshots")
	}

	ch, err := restic.NewContentHandler(be, key)
	if err != nil {
		return err
	}

	tab := NewTable()
	tab.Header = fmt.Sprintf("%-8s  %-19s  %-10s  %s", "ID", "Date", "Source", "Directory")
	tab.RowFormat = "%-8s  %-19s  %-10s  %s"

	list := []*restic.Snapshot{}
	backend.EachID(be, backend.Snapshot, func(id backend.ID) {
		sn, err := ch.LoadSnapshot(id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading snapshot %s: %v\n", id, err)
			return
		}

		pos := sort.Search(len(list), func(i int) bool {
			return list[i].Time.After(sn.Time)
		})

		if pos < len(list) {
			list = append(list, nil)
			copy(list[pos+1:], list[pos:])
			list[pos] = sn
		} else {
			list = append(list, sn)
		}
	})

	plen, err := backend.PrefixLength(be, backend.Snapshot)
	if err != nil {
		return err
	}

	for _, sn := range list {
		tab.Rows = append(tab.Rows, []interface{}{sn.ID()[:plen], sn.Time.Format(TimeFormat), sn.Hostname, sn.Dir})
	}

	tab.Print(os.Stdout)

	return nil
}