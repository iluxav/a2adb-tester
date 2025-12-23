package server

import (
	"github.com/gofiber/fiber/v2"

	"a2adb-tester/internal/database"
)

type FiberServer struct {
	*fiber.App

	db database.Service
}

func New() *FiberServer {
	server := &FiberServer{
		App: fiber.New(fiber.Config{
			ServerHeader: "a2adb-tester",
			AppName:      "a2adb-tester",
		}),

		db: database.New(),
	}

	return server
}
