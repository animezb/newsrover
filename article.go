package newsrover

import (
	"fmt"
	"time"
)

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
	t, _ := time.Parse("02 Jan 2006 15:04:05 MST", a.Date)
	return t
}

func (a Article) Pretty() string {
	return fmt.Sprintf("Subject:\t%s\nFrom:\t%s\nDate:\t%s (%d)\n", a.Subject, a.From, a.Date, a.Time().Unix())
}
