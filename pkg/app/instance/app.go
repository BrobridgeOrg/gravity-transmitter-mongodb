package instance

import (
	writer "github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/database/writer"
	subscriber "github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/subscriber/service"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type AppInstance struct {
	done       chan os.Signal
	writer     *writer.Writer
	subscriber *subscriber.Subscriber
}

func NewAppInstance() *AppInstance {

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)
	a := &AppInstance{
		done: sig,
	}

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing modules
	a.writer = writer.NewWriter()
	a.subscriber = subscriber.NewSubscriber(a)

	// Initializing Writer
	err := a.initWriter()
	if err != nil {
		return err
	}

	err = a.subscriber.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	err := a.subscriber.Run()
	if err != nil {
		return err
	}

	<-a.done
	a.subscriber.Stop()
	time.Sleep(5 * time.Second)
	log.Error("Bye!")

	return nil
}
