package newsrover

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RoverConfig struct {
	Host  string `json:"host"`
	SSL   bool   `json:"use_ssl"`
	Group string `json:"newsgroup"`

	AuthUser string `json:"auth_user"`
	AuthPass string `json:"auth_pass"`

	CheckEvery  int `json:"check_every"`
	FlushEvery  int `json:"flush_articles_every"`
	MaxArticles int `json:"max_buffered_articles"`

	StartAt  int    `json:"start_at_article"`
	StopAt   int    `json:"no_article_to_process"`
	Progress string `json:"progress"`
}

type Rover struct {
	conn   *textproto.Conn
	config RoverConfig

	reconnect func() (io.ReadWriteCloser, error)
	stop      chan bool

	processed     int
	processedLife int
	lastArticle   int
	finalArticle  int
	high          int

	serverSubject   int
	serverFrom      int
	serverDate      int
	serverMessageId int
	serverBytes     int
	serverMinXover  int

	progressFile *os.File
	articleNo    int

	sinks []Sink

	logger *log.Logger

	runningWg sync.WaitGroup
}

type FetchIter struct {
	zreader io.ReadCloser
	reader  *bufio.Reader
	err     error
	rover   *Rover
}

var ErrBadYenc error = fmt.Errorf("Only recieved bad Yencs.")

func NewRover(addr string, conf RoverConfig) (*Rover, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	reconnect := func() (io.ReadWriteCloser, error) {
		return net.Dial("tcp", addr)
	}
	return newRover(conn, reconnect, conf)
}

func NewRoverSsl(addr string, conf RoverConfig) (*Rover, error) {
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return nil, err
	}
	reconnect := func() (io.ReadWriteCloser, error) {
		return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	}
	return newRover(conn, reconnect, conf)
}

func newRover(conn io.ReadWriteCloser, reconnect func() (io.ReadWriteCloser, error), conf RoverConfig) (*Rover, error) {
	r := &Rover{
		config:    conf,
		reconnect: reconnect,
		sinks:     make([]Sink, 0, 2),
		logger:    log.New(ioutil.Discard, "", log.LstdFlags),
	}
	if err := r.strapConnection(conn); err != nil {
		return nil, err
	}
	if r.config.Progress != "" {
		var err error
		if r.progressFile, err = os.OpenFile(r.config.Progress, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			fmt.Printf("WARNING: Progress File for group %s failed to open. (%s). Progress will not be saved.\n", conf.Group, err.Error())
		}
	}
	if r.config.CheckEvery == 0 {
		r.config.CheckEvery = 60
	}
	if r.config.FlushEvery == 0 {
		r.config.FlushEvery = 60
	}
	if r.config.MaxArticles == 0 {
		r.config.MaxArticles = 64
	}
	return r, nil
}

func (r *Rover) Group() string {
	return r.config.Group
}

func (r *Rover) strapConnection(conn io.ReadWriteCloser) error {
	r.conn = textproto.NewConn(conn)
	if _, _, err := r.conn.ReadCodeLine(200); err != nil {
		return err
	}
	if r.config.AuthUser != "" {
		if _, err := r.Do(281, "AUTHINFO USER %s", r.config.AuthUser); err != nil {
			if response, err := r.Do(381, "AUTHINFO USER %s", r.config.AuthUser); err == nil {
				if response, err := r.Do(281, "AUTHINFO PASS %s", r.config.AuthPass); err != nil {
					return fmt.Errorf("Authenication Error: %s (%s)", err.Error(), response)
				}
			} else {
				return fmt.Errorf("Authenication Error: %s (%s)", err.Error(), response)
			}
		}
	}
	if _, err := r.Do(215, "LIST OVERVIEW.FMT"); err != nil {
		return err
	}
	if lines, err := r.conn.ReadDotLines(); err == nil {
		for idx, line := range lines {
			line = strings.ToLower(line)
			line = strings.TrimSuffix(line, ":")
			switch line {
			case "subject":
				r.serverSubject = idx
				r.serverMinXover = idx + 1
			case "from":
				r.serverFrom = idx
				r.serverMinXover = idx + 1
			case "date":
				r.serverDate = idx
				r.serverMinXover = idx + 1
			case "message-id":
				r.serverMessageId = idx
				r.serverMinXover = idx + 1
			case "bytes", ":bytes":
				r.serverBytes = idx
				r.serverMinXover = idx + 1
			}
		}
	}
	return nil
}

func (r *Rover) SetLogger(logger *log.Logger) {
	if logger == nil {
		r.logger = log.New(ioutil.Discard, "", log.LstdFlags)
	} else {
		r.logger = logger
		if r.logger.Prefix() == "" {
			r.logger.SetPrefix("[NewsRover - " + r.config.Group + "]")
		}
	}
}

func parseN(s string) int {
	if b, err := strconv.Atoi(s); err == nil {
		return b
	}
	return 0
}

func (r *Rover) SwitchGroup(group string) (Group, error) {
	if response, err := r.Do(211, "GROUP %s", group); err == nil {
		values := strings.Fields(response)
		if len(values) != 4 {
			return Group{}, fmt.Errorf("Malformed response from GROUP function.")
		}
		if group != r.config.Group {
			if r.logger.Prefix() == "" || r.logger.Prefix() == "[NewsRover - "+r.config.Group+"]" {
				r.logger.SetPrefix("[NewsRover - " + group + "]")
			}
			r.config.Group = group
		}
		return Group{
			Name:   values[3],
			Number: parseN(values[0]),
			Low:    parseN(values[1]),
			High:   parseN(values[2]),
		}, nil
	} else {
		return Group{}, err
	}
}

func (r *Rover) Close() {
	r.conn.Close()
}

func (r *Rover) Do(expectCode int, format string, args ...interface{}) (string, error) {
	if err := r.conn.PrintfLine(format, args...); err != nil {
		return "", err
	}
	if _, response, err := r.conn.ReadCodeLine(expectCode); err == nil {
		return response, nil
	} else {
		return response, err
	}
}

func (r *Rover) fetch(articleId ...int) (int, error) {
	var command string
	var noa int
	switch len(articleId) {
	case 1:
		command = fmt.Sprintf("XZVER %d", articleId[0])
		noa = 1
	case 2:
		if articleId[0] == articleId[1] {
			command = fmt.Sprintf("XZVER %d", articleId[0])
			noa = 1
		} else if articleId[1] < 0 {
			command = fmt.Sprintf("XZVER %d-", articleId[0])
			noa = 64
		} else {
			command = fmt.Sprintf("XZVER %d-%d", articleId[0], articleId[1])
			noa = articleId[1] - articleId[0]
			if noa == 0 {
				noa = 1
			} else if noa < 0 {
				noa = noa * -1
			}
		}
	default:
		panic(fmt.Sprintf("Rover.Fetch called with an invalid number of arguments (Expected 1 or 2, got %d).", len(articleId)))
	}
	for i := 0; i < 3; i++ {
		if _, err := r.Do(224, command); err == nil {
			break
		} else if err == io.ErrClosedPipe {
			if newConn, err := r.reconnect(); err == nil {
				if err := r.strapConnection(newConn); err != nil {
					return 0, err
				}
				continue
			} else {
				return 0, err
			}
		} else {
			return 0, err
		}
	}
	return noa, nil
}

func (r *Rover) ParseArticle(line string) (Article, error) {
	var article Article
	fields := strings.Split(line, "\t")
	if len(fields)-1 < r.serverMinXover {
		return article, fmt.Errorf("Malformed line.")
	}
	article.Group = r.config.Group
	article.ArticleId = parseN(fields[0])
	for idx, field := range fields[1:] {
		switch idx {
		case r.serverSubject:
			article.Subject = field
		case r.serverFrom:
			article.From = field
		case r.serverDate:
			article.Date = field
		case r.serverMessageId:
			article.MessageId = field
		case r.serverBytes:
			article.Bytes = int64(parseN(field))
		}
	}
	return article, nil
}

func (r *Rover) Fetch(articleId ...int) ([]Article, error) {
	noa, e := r.fetch(articleId...)
	if e != nil {
		return nil, e
	}
	articles := make([]Article, 0, noa)

	dec := NewYencDecoder(r.conn.R)
	part, err := dec.Decode()
	if err != nil {
		return nil, err
	}
	r.conn.ReadDotBytes()
	zreader := flate.NewReader(bytes.NewReader(part.Body))
	defer zreader.Close()
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(zreader)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if article, err := r.ParseArticle(line); err == nil {
			articles = append(articles, article)
		}

	}
	return articles, nil
}

func (r *Rover) FetchIter(articleId ...int) (*FetchIter, error) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(5 * time.Duration(i) * time.Second)
		}
		if _, e := r.fetch(articleId...); e != nil {
			return nil, e
		}

		dec := NewYencDecoder(r.conn.R)
		part, err := dec.Decode()
		if err != nil {
			return nil, err
		}
		r.conn.ReadDotBytes()
		if bytes.Equal(part.Body, []byte{3, 0}) {
			r.logger.Println("Recieved bad yenc. Refetching...")
			continue
		}
		zreader := flate.NewReader(bytes.NewReader(part.Body))
		reader := bufio.NewReader(zreader)
		return &FetchIter{
			reader:  reader,
			zreader: zreader,
			err:     nil,
			rover:   r,
		}, nil
	}
	return nil, ErrBadYenc
}

func (i *FetchIter) Next(outArticle *Article) bool {
	for {
		line, err := i.reader.ReadString('\n')
		if err != nil {
			//i.rover.logger.Printf("Error reading article. (%s, %v, %d)", err.Error(), err == io.EOF, len(line))
			if err == io.EOF {
				break
			}
			i.err = err
			return false
		}
		if article, err := i.rover.ParseArticle(line); err == nil {
			*outArticle = article
			return true
		} else {
			i.rover.logger.Printf("Failed to parse line %s.", line)
		}
	}
	return false
}

func (i *FetchIter) Close() error {
	defer i.zreader.Close()
	if i.err != nil {
		return i.err
	}
	return nil
}

func (r *Rover) AddSink(sink Sink) {
	r.sinks = append(r.sinks, sink)
}

func (r *Rover) RemoveSink(sink Sink) {
	for idx, other := range r.sinks {
		if other == sink {
			r.sinks[idx] = r.sinks[len(r.sinks)-1]
			r.sinks = r.sinks[0 : len(r.sinks)-1]
			return
		}
	}
}

func (r *Rover) flush(articles []Article) {
	nr := make([]Article, len(articles))
	copy(nr, articles)
	r.logger.Printf("Flushing articles. Current Point: %d Processed: %d (%d) High: %d", r.lastArticle, r.processed, r.processedLife, r.high)
	for _, sink := range r.sinks {
		sink.Accept(nr)
	}
}

func (r *Rover) ReadProgress() int {
	if r.progressFile != nil {
		r.progressFile.Seek(0, 0)
		if d, err := ioutil.ReadAll(r.progressFile); err == nil {
			sp := strings.Split(string(d), ";")
			if n, err := strconv.Atoi(sp[0]); err == nil {
				if sp[1] == r.Group() {
					// reprocess some articles just to be safe
					if n-100 > 0 {
						return n - 100
					}
					return n
				}
			}
		}
	}
	return -1
}

func (r *Rover) SaveProgress(article int) {
	if r.progressFile != nil {
		r.progressFile.Seek(0, 0)
		r.progressFile.WriteString(fmt.Sprintf("%d;%s;", article, r.Group()))
	}
}

func (r *Rover) Serve() error {
	r.runningWg.Add(1)
	if group, err := r.SwitchGroup(r.config.Group); err == nil {
		if r.config.StartAt <= 0 {
			r.lastArticle = group.High + r.config.StartAt
		} else if r.config.StartAt > group.Low {
			r.lastArticle = r.config.StartAt
		} else {
			r.lastArticle = group.High
		}
	} else {
		r.logger.Println("An error occured switching newsgroup: ", err.Error())
		return err
	}
	progress := r.ReadProgress()
	if progress != -1 {
		r.lastArticle = progress
		r.logger.Printf("Starting rover from article %d.", r.lastArticle)
	}
	saveTime := 30 * time.Second
	flushTime := time.Duration(r.config.FlushEvery) * time.Second
	checkTime := time.Duration(r.config.CheckEvery) * time.Second
	flush := time.NewTimer(flushTime)
	check := time.NewTimer(1 * time.Millisecond)
	midCheck := 1
	var psave *time.Timer
	if r.progressFile != nil {
		psave = time.NewTimer(saveTime)
		midCheck++
	}
	getSaveChannel := func() <-chan time.Time {
		if r.progressFile != nil {
			return psave.C
		}
		return nil
	}
	bfSz := r.config.MaxArticles
	if bfSz < 0 {
		bfSz = 64
	}
	articleBuff := make([]Article, 0, bfSz+1)
	defer func() {
		if len(articleBuff) > 0 {
			r.flush(articleBuff)
		}
	}()
	defer func() {
		r.SaveProgress(r.lastArticle)
	}()
	defer func() {
		r.runningWg.Done()
	}()
	defer func() {
		r.stop = nil
	}()
	r.stop = make(chan bool)
	r.logger.Printf("Starting newsrover on group %s", r.config.Group)
	for {
		select {
		case <-r.stop:
			return nil
		case <-flush.C:
			if len(articleBuff) > 0 {
				r.flush(articleBuff)
				articleBuff = articleBuff[:0]
			}
			flush.Reset(flushTime)
		case <-getSaveChannel():
			r.SaveProgress(r.lastArticle)
			psave.Reset(saveTime)
		case <-check.C:
			if group, err := r.SwitchGroup(r.config.Group); err == nil {
				if group.High > r.lastArticle {
					r.logger.Printf("Reading new articles. Starting from %d going to %d", r.lastArticle, group.High)
					r.high = group.High
					r.processed = 0

					for start, end := r.lastArticle+1, r.lastArticle+1025; start <= group.High; {
						if end > group.High {
							end = group.High
						}
						//r.logger.Printf("Fetching range %d - %d", start, end)
						for k := 0; k < 3; k++ {
							if iter, err := r.FetchIter(start, end); err == nil {
								var article Article
								z := 0
								var maxArt int
								for iter.Next(&article) {
									r.processed++
									r.processedLife++
									articleBuff = append(articleBuff, article)
									if r.config.MaxArticles >= 0 {
										if len(articleBuff) > r.config.MaxArticles {
											r.flush(articleBuff)
											articleBuff = articleBuff[:0]
										}
									}
									if z%50 == 0 {
										for i := 0; i < midCheck; i++ {
											select {
											case <-flush.C:
												if len(articleBuff) > 0 {
													r.flush(articleBuff)
													articleBuff = articleBuff[:0]
												}
												flush.Reset(flushTime)
											case <-getSaveChannel():
												r.SaveProgress(article.ArticleId)
												psave.Reset(saveTime)
											default:
												i = 100
											}
										}
									}
									if article.ArticleId > maxArt {
										maxArt = article.ArticleId
									}
									//end = article.ArticleId
									z++
								}
								if z == 0 {
									r.logger.Printf("Was going to process articles, but no articles were actually processed.")
								}
								if err := iter.Close(); err != nil {
									r.logger.Printf("An error occured closing the iterator: ", err.Error())
									return err
								}
								if maxArt < end {
									r.logger.Printf("Wanted to read up to %d, only read up to %d.", end, maxArt)
									start = maxArt + 1
								} else {
									start = end + 1
								}
								end = start + 1024
								break
							} else if err == ErrBadYenc {
								if k == 2 {
									r.logger.Printf("[FATAL] Could not reliable get articles. Segments will be skipped.")
								} else {
									r.logger.Printf("Failed to fetch articles, badYenc.")
									r.logger.Printf("Sleeping for %d seconds...", (k+1)*30)
									time.Sleep(time.Duration(k+1) * 30 * time.Second)
									select {
									case <-flush.C:
										if len(articleBuff) > 0 {
											r.flush(articleBuff)
											articleBuff = articleBuff[:0]
										}
										flush.Reset(flushTime)
									default:
									}
								}
							} else {
								r.logger.Println("An error occured fetching the articles: ", err.Error())
								if newConn, err := r.reconnect(); err == nil {
									if err := r.strapConnection(newConn); err != nil {
										r.logger.Println("An error occured bootstrapping the connection: ", err.Error())
										return err
									}
									r.logger.Printf("Reconnected successfully.")
									r.SwitchGroup(r.config.Group)
									continue
								} else {
									r.logger.Println("An error reconnecting to the news server: ", err.Error())
									return err
								}
							}
						}
					}
					r.lastArticle = group.High
					if r.config.StopAt > 0 {
						if r.processedLife >= r.config.StopAt {
							r.logger.Printf("Successfully processed %d articles. Set to stop at %d. Stopping.", r.processedLife, r.config.StopAt)
							go func() {
								r.stop <- true
							}()
						}
					}
				}
			} else {
				r.logger.Printf("Error occured connecting to host: %s. Trying to reconnect.", err.Error())
				if newConn, err := r.reconnect(); err == nil {
					if err := r.strapConnection(newConn); err != nil {
						r.logger.Println("An error occured bootstrapping the connection: ", err.Error())
						return err
					}
					r.logger.Printf("Reconnected successfully.")
					check.Reset(1 * time.Millisecond)
					continue
				} else {
					r.logger.Println("An error reconnecting to the news server: ", err.Error())
					return err
				}
			}
			check.Reset(checkTime)
		}
	}
	return nil
}

func (r *Rover) Stop() {
	if r.stop != nil {
		r.stop <- true
		r.stop = nil
	}
	r.runningWg.Wait()
}
