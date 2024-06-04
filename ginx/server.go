package ginx

import "github.com/gin-gonic/gin"

type Server struct {
	*gin.Engine
	Addr string
}

// Start 启动gin server
func (s *Server) Start() error {
	return s.Engine.Run(s.Addr)
}
