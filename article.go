package newsrover

import (
	"fmt"
	"time"
)

var dateFormats []string

func init() {
	dateFormats = []string{
		"02 Jan 2006 15:04:05 MST",
		time.RFC1123Z,
		time.RFC1123,
		time.RFC3339,
		time.RFC3339Nano,
		time.RFC850,
		time.UnixDate,
		time.ANSIC,
		time.RubyDate,
	}
}

type Group struct {
	Name   string
	Number int
	Low    int
	High   int
}

type Article struct {
	Group     string `json:"group"`
	ArticleId int    `json:"article_id"`
	Subject   string `json:"subject"`
	From      string `json:"from"`
	Date      string `json:"date"`
	MessageId string `json:"message_id"`
	Bytes     int64  `json:"bytes"`
}

func (a Article) Time() time.Time {
	for _, df := range dateFormats {
		if t, err := time.Parse(df, a.Date); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (a Article) Pretty() string {
	return fmt.Sprintf("Subject:\t%s\nFrom:\t%s\nDate:\t%s (%d)\n", a.Subject, a.From, a.Date, a.Time().Unix())
}
