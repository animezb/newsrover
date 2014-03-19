package newsrover

import (
	"fmt"
	"log"
)

type Sink interface {
	Name() string
	Accept([]Article)
	Serve()
	Stop()
	SetLogger(*log.Logger)
}

type StdSink struct{}

func (s *StdSink) Name() string {
	return "standard"
}

func (s *StdSink) Accept(a []Article) {
	for _, i := range a {
		fmt.Println(i.Pretty())
	}
}

func (s *StdSink) Serve() {

}

func (s *StdSink) Stop() {

}

func (s *StdSink) SetLogger(*log.Logger) {

}
