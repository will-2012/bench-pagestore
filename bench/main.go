package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bench-pagestore/monitor"
	"bench-pagestore/pagestore"
	"github.com/urfave/cli/v2"
)

const (
	readWorkerNumber = 10
)

func main() {
	app := &cli.App{
		Name:  "Bench",
		Usage: "bench page store",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "write",
				Usage: "bench page store write",
			},
			&cli.BoolFlag{
				Name:  "read",
				Usage: "bench page store read",
			},
			&cli.Uint64Flag{
				Name:  "read-start",
				Usage: "read range from start",
			},
			&cli.Uint64Flag{
				Name:  "read-end",
				Usage: "read range to end",
			},
		},

		Action: benchMain,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func benchMain(c *cli.Context) error {
	monitor.Init()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, os.Interrupt, os.Kill, syscall.SIGUSR1, syscall.SIGUSR2)

	if c.Bool("write") {
		return benchWrite(ch)
	}
	if c.Bool("read") {
		return benchRead(ch, c.Uint64("read-start"), c.Uint64("read-end"))
	}
	fmt.Println("Bench nothing")
	return nil
}

func benchWrite(ch chan os.Signal) error {
	fmt.Println("Start bench write")

	var (
		pageStore   *pagestore.PageStore
		err         error
		wGenerator  *pagestore.BenchWriteGenerator
		curPageID   *pagestore.PageID
		curPageData *pagestore.PageData
	)

	if pageStore, err = pagestore.Open(); err != nil {
		fmt.Printf("Failed to open page store due to error=%v\n", err)
		return err
	}
	wGenerator = &pagestore.BenchWriteGenerator{}
	wGenerator.Init()

	for {
		select {
		case s := <-ch:
			fmt.Printf("Break loop due to signal=%v, last_page_version=%v\n", s, curPageID.Version())
			fmt.Println("End bench write")
			return nil
		default:
			curPageID, curPageData = wGenerator.Generate()
			if err = pageStore.Put(curPageID, curPageData); err != nil {
				fmt.Printf("Break loop due to put failed, error=%v\n", err)
				fmt.Println("End bench write")
				return err
			}
		}
	}

}

func benchRead(ch chan os.Signal, start uint64, end uint64) error {
	fmt.Println("Start bench read")

	var (
		pageStore   *pagestore.PageStore
		err         error
		readWorkers []*ReadWorker
	)
	if pageStore, err = pagestore.Open(); err != nil {
		fmt.Printf("Failed to open page store due to error=%v\n", err)
		return err
	}
	readWorkers = make([]*ReadWorker, readWorkerNumber)

	for index := uint64(0); index < readWorkerNumber; index++ {
		readWorkers[index] = &ReadWorker{
			workIndex:    index,
			pageStore:    pageStore,
			startVersion: start,
			endVersion:   end,
			stopCh:       make(chan struct{}),
			waitStopCh:   make(chan struct{}),
		}
		readWorkers[index].Start()
	}

	select {
	case s := <-ch:
		fmt.Printf("Break loop due to signal=%v\n", s)
		for index := uint64(0); index < readWorkerNumber; index++ {
			readWorkers[index].Stop()
		}
		pageStore.Close()
		fmt.Println("End bench read")
		return nil
	}
}

type ReadWorker struct {
	workIndex    uint64
	pageStore    *pagestore.PageStore
	startVersion uint64
	endVersion   uint64
	stopCh       chan struct{}
	waitStopCh   chan struct{}
}

func (rWorker *ReadWorker) Start() {
	if rWorker == nil {
		return
	}

	var (
		err        error
		rGenerator *pagestore.BenchReadGenerator
		curPageID  *pagestore.PageID
	)
	rGenerator = &pagestore.BenchReadGenerator{}
	rGenerator.Init(rWorker.startVersion, rWorker.endVersion)

	go func() {
		for {
			select {
			case <-rWorker.stopCh:
				fmt.Printf("Break loop due to stop\n")
				fmt.Printf("Worker-%v end bench read\n", rWorker.workIndex)
				rWorker.waitStopCh <- struct{}{}
				return
			default:
				curPageID = rGenerator.Generate()
				if _, err = rWorker.pageStore.Get(curPageID); err != nil { // continue
					fmt.Printf("Break loop due to get failed, error=%v\n", err)
					fmt.Println("End bench read")
				}
			}
		}
	}()
}

func (rWorker *ReadWorker) Stop() {
	if rWorker == nil {
		return
	}
	rWorker.stopCh <- struct{}{}
	<-rWorker.waitStopCh
}
